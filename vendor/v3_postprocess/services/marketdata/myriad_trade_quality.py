"""Known WS transaction-leg groups, not exchange fill completeness.

Myriad WS reports a taker aggregate and maker legs; REST does not expose the
same leg IDs. Compare market/transaction/outcome/action presence only. Never
deduplicate the stored REST occurrences or assert amount/fee equality here.
"""
from copy import deepcopy


def group(market, row, transaction=None):
    tx = transaction or row.get('txHash')
    outcome = row.get('outcome'); side = row.get('side')
    if (not isinstance(tx, str) or not tx or type(outcome) is not int or
            outcome not in (0, 1) or side not in ('buy', 'sell', 'split', 'merge')):
        return None
    return (market, tx.lower(), outcome, side)


class MyriadKnownWSTrades:
    def __init__(self, max_groups=100000):
        self.max_groups = max_groups
        self.groups = {}; self.unknown = []; self.overflow = False

    def observe(self, event, available_at_ns):
        if event.get('kind') != 'trade':return False  # settlements are a separate protocol
        raw = event.get('native', {}); market = event.get('market_id')
        stamp = event.get('source_timestamp_ms')
        clock = stamp * 10**6 if type(stamp) is int else None
        legs = [raw.get('taker')] + (raw.get('makers') if isinstance(raw.get('makers'), list) else [None])
        unknown = not market or clock is None
        for leg in legs:
            key = group(market, leg, raw.get('txHash')) if isinstance(leg, dict) else None
            if key is None:unknown = True; continue
            if key not in self.groups and len(self.groups) >= self.max_groups:
                self.overflow = True; continue
            g = self.groups.setdefault(key, dict(available_at_ns=available_at_ns,
                source_ns=clock, observations=0, native_reference=event.get('_collector_native_reference')))
            g['observations'] += 1  # diagnostic only: reconnects may repeat a publication
        if unknown:
            if len(self.unknown) < self.max_groups:
                self.unknown.append(dict(market=market, source_ns=clock, available_at_ns=available_at_ns))
            else:self.overflow = True
        return not unknown

    def compare(self, market, observed, lower_ns, upper_ns, asof_ns):
        def relevant(g):
            t = g['source_ns']
            return g['available_at_ns'] <= asof_ns and (t is None or lower_ns <= t < upper_ns)
        expected = {k for k, g in self.groups.items() if k[0] == market and relevant(g)}
        missing = expected - set(observed)
        unknown = sum((not g['market'] or g['market'] == market) and relevant(g) for g in self.unknown)
        reasons = []
        if missing:reasons.append('known_Myriad_WS_group_absent_from_API')
        if unknown:reasons.append('Myriad_WS_identity_or_clock_incomplete')
        if self.overflow:reasons.append('Myriad_WS_witness_capacity_exceeded')
        young = sum(asof_ns-self.groups[k]['available_at_ns'] < 30*10**9 for k in missing)
        return dict(reasons=reasons, known_WS_groups=len(expected), known_WS_matched_groups=len(expected)-len(missing),
            known_WS_missing_groups=len(missing), unknown_WS_observations=unknown,
            pending_indexing_groups=young, persistent_missing_groups=len(missing)-young,
            missing_reference_samples=[dict(key=list(k), **self.groups[k]) for k in sorted(missing)[:20]],
            match_basis='Myriad market+transaction+outcome+action presence groups; not unique fills or amounts',
            WS_comparison_observed=bool(expected), full_exchange_fills_certified=False,
            amount_fee_and_occurrence_multiplicity_certified=False)

    def state(self):
        return dict(groups=[[list(k),deepcopy(v)] for k,v in self.groups.items()],
                    unknown=deepcopy(self.unknown),overflow=self.overflow)

    def restore(self, state):
        if len(state['groups']) > self.max_groups or len(state['unknown']) > self.max_groups:
            raise ValueError('Myriad_WS_checkpoint_capacity_exceeded')
        self.groups={tuple(k):deepcopy(v) for k,v in state['groups']}
        self.unknown=deepcopy(state['unknown']);self.overflow=state['overflow']
