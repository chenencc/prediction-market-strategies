"""Type 2: Polymarket/Predict recording candidates, without execution."""
from typing import Callable
from .contracts import (PAIR, CONDITION, PairPolicy, MarketFacts,
                        digest, instant, receipt, valid_id, validate_native_outcomes)

from .rules import compare_rules, same_target, CREATION_RELATIVE
from .outcomes import bind_outcomes
from .recording import fresh_catalog,recording_checks


def select_pairs(catalog, *, at, decode_market: Callable[[str, dict], MarketFacts], policy=PairPolicy()):
    """Polymarket/Predict native-mapped recording candidates, not proven arbitrage.

    Catalog envelopes: {venue, raw, observed_at, http_status}. Exact rule text
    modulo whitespace or verified terminal metadata; semantic differences need review.
    """
    now = instant(at)
    if policy.max_metadata_age_seconds <= 0:
        raise ValueError('positive_metadata_age_required')
    indexed, conflicts, decisions = {}, set(), []

    def result(decision, reason, **fields):
        return receipt(PAIR, decision, reason, observed_at=at,
                       equivalence_verified=False, executable_arbitrage_verified=False, **fields)

    for packet in catalog:
        key = None
        try:
            venue, raw = packet['venue'], packet['raw']
            if venue not in {'polymarket', 'predictfun'}:
                decisions.append(result('review', 'unsupported_pair_venue'))
                continue
            native_id = raw['id']
            if not valid_id(native_id):
                raise ValueError('market_id_invalid')
            key = venue + ':' + str(native_id)
            fresh_catalog(packet,now,policy)
            validate_native_outcomes(venue, raw)
            leg = decode_market(venue, raw)
            fingerprint = digest(raw)
            if key in indexed and indexed[key][1] != fingerprint:
                conflicts.add(key)
            else:
                indexed[key] = (leg, fingerprint)
        except (ValueError, TypeError, KeyError, AttributeError):
            if key:
                conflicts.add(key)
            decisions.append(result('review', 'invalid_or_stale_catalog_entry', market_id=key))
    for key in sorted(conflicts):
        indexed.pop(key, None)
        decisions.append(result('review', 'conflicting_catalog_entry', market_id=key))
    polys = {}
    for leg, fingerprint in indexed.values():
        if leg.venue == 'polymarket' and CONDITION.fullmatch(leg.condition_id):
            polys.setdefault(leg.condition_id, []).append((leg, fingerprint))
    for pred, pred_hash in sorted(indexed.values(), key=lambda pair: pair[0].key):
        if pred.venue != 'predictfun':
            continue

        def reject(reason, decision='review', **fields):
            decisions.append(result(decision, reason, market_id=pred.key, **fields))

        refs = pred.raw.get('polymarketConditionIds')
        if not isinstance(refs, list) or len(refs) != 1 or not isinstance(refs[0], str) or not CONDITION.fullmatch(refs[0]):
            reject('missing_or_ambiguous_native_mapping')
            continue
        matches = polys.get(refs[0].lower(), [])
        if len(matches) != 1:
            reject('missing_or_ambiguous_polymarket_leg')
            continue
        poly, poly_hash = matches[0]
        members = sorted([pred.key, poly.key])
        check=recording_checks(pred,poly,policy=policy,
            bind=lambda left,right:bind_outcomes(left,right,require_question_match=policy.require_semantic_match))
        if check['decision']!='include':
            reject(check['reason'],decision=check['decision'],members=members);continue
        bindings,direction_proof,comparison=check['bindings'],check['direction_proof'],check['comparison']
        decisions.append(result('include', 'native_mapped_recording_candidate', members=members,
            candidate_id=digest([PAIR, members]),
            evidence_hash=digest(sorted([(pred.key, pred_hash), (poly.key, poly_hash)])),
            relation_status='candidate', **comparison, **direction_proof,
            outcome_bindings=bindings,
            required_checks=['fresh_execution_preflight', 'settlement_and_void_equivalence',
                             'fees_and_payout_terms', 'time_alignment_and_capacity']))
    return sorted(decisions, key=lambda d: (d.get('market_id') or '', d.get('candidate_id') or '', d['reason']))
