"""Task5 mapping adapter over the same Task2 recording filter implementation."""
from native_archive.fees import exact_value
from pair_recorder.rules.contracts import CONDITION,MarketFacts,PairPolicy,digest,instant,receipt,validate_native_outcomes,valid_id
from pair_recorder.rules.recording import fresh_catalog,recording_checks
from pair_recorder.selection import decode_market as decode_existing
from task_control import from_legacy,evidence_ref
from myriad import protocol
from . import TASK5


def decode_market(venue,raw):
    if venue!='myriad':return decode_existing(venue,raw)
    mid=protocol.identity(raw)
    names=[o.get('title') for o in raw['outcomes']];assets=[str(o['tokenId']) for o in raw['outcomes']]
    if any(not isinstance(n,str) or not n.strip() for n in names):raise ValueError('native_outcome_labels_required')
    labels=[n.lower() for n in names]
    if len(set(labels))!=2 or len(set(assets))!=2 or any(not a.isascii() or not a.isdecimal() for a in assets):
        raise ValueError('ambiguous_native_myriad_outcomes')
    if raw.get('voided') is not None and type(raw['voided']) is not bool:raise ValueError('invalid_myriad_void_status')
    if raw.get('resolvedOutcomeId') is not None and (type(raw['resolvedOutcomeId']) is not int or raw['resolvedOutcomeId'] not in (-1,0,1)):
        raise ValueError('invalid_myriad_resolution_id')
    opened=raw.get('state')=='open' and raw.get('voided') is not True and raw.get('resolvedOutcomeId') in (None,-1)
    return MarketFacts('myriad',mid,'',raw.get('question') or raw.get('title',''),
        raw.get('description',''),opened,dict(zip(labels,assets)),raw)


def native_mapping(raw):
    refs=[r for r in raw.get('externalSources') or [] if isinstance(r,dict) and
          isinstance(r.get('providerName'),str) and r['providerName'].casefold()=='polymarket']
    if len(refs)!=1:return None
    value=refs[0].get('externalMarketId')
    if isinstance(value,bool) or not isinstance(value,(str,int)) or not str(value).isascii() or not str(value).isdecimal():return None
    return str(value)


def bind_outcomes(myriad,poly):
    if set(myriad.outcomes)!=set(poly.outcomes):return None
    bindings=[]
    for row in sorted(myriad.raw['outcomes'],key=lambda r:r['id']):
        label=row['title'].lower()
        bindings.append(dict(label=label,polymarket_asset_id=poly.outcomes[label],
            myriad_asset_id=str(row['tokenId']),myriad_outcome_id=row['id'],myriad_book_origin='native'))
    return bindings,dict(outcome_binding_proof=dict(policy='myriad-native-outcome-labels/1',
        method='explicit_native_ids_and_exact_labels',independent_rule_equivalence_certified=False))


def select(catalog,*,at,policy=PairPolicy()):
    now=instant(at)
    if policy.max_metadata_age_seconds<=0:raise ValueError('positive_metadata_age_required')
    indexed={};conflicts=set();decisions=[]
    def result(decision,reason,**fields):
        return receipt(TASK5,decision,reason,observed_at=at,equivalence_verified=False,
                       executable_arbitrage_verified=False,**fields)
    for packet in catalog:
        key=None
        try:
            venue=packet['venue'];raw=exact_value(packet['raw'])
            if venue not in ('polymarket','myriad'):
                decisions.append(result('review','unsupported_pair_venue'));continue
            if venue=='myriad':
                if type(raw.get('networkId')) is not int or type(raw.get('id')) is not int:raise ValueError('native_myriad_identity_required')
                if raw['networkId']!=56:raise ValueError('unsupported_myriad_pair_network')
                if raw.get('tradingModel')=='ob' and raw.get('executionMode')==1:
                    key=f'myriad:{raw["networkId"]}:ob:{raw["id"]}'
            else:
                if not valid_id(raw.get('id')):raise ValueError('market_id_invalid')
                key='polymarket:'+str(raw['id'])
            fresh_catalog(packet,now,policy)
            if venue=='polymarket':validate_native_outcomes(venue,raw)
            leg=decode_market(venue,raw);key=leg.key
            if key in indexed and indexed[key][1]!=digest(raw):conflicts.add(key)
            else:indexed[key]=(leg,digest(raw))
        except (ValueError,TypeError,KeyError,AttributeError):
            if key:conflicts.add(key)
            decisions.append(result('review','invalid_or_stale_catalog_entry',market_id=key))
    for key in sorted(conflicts):
        indexed.pop(key,None);decisions.append(result('review','conflicting_catalog_entry',market_id=key))
    polys={}
    for leg,h in indexed.values():
        if leg.venue=='polymarket' and CONDITION.fullmatch(leg.condition_id):polys.setdefault(leg.market_id,[]).append((leg,h))
    for myriad,h in sorted(indexed.values(),key=lambda r:r[0].key):
        if myriad.venue!='myriad':continue
        target=native_mapping(myriad.raw)
        if target is None:
            decisions.append(result('review','missing_or_ambiguous_native_mapping',market_id=myriad.key));continue
        matches=polys.get(target,[])
        if len(matches)!=1:
            decisions.append(result('review','missing_or_ambiguous_polymarket_leg',market_id=myriad.key));continue
        poly,poly_hash=matches[0];members=sorted([myriad.key,poly.key])
        check=recording_checks(myriad,poly,policy=policy,bind=bind_outcomes)
        fields=dict(market_id=myriad.key,members=members)
        if check['decision']!='include':
            decisions.append(result(check['decision'],check['reason'],**fields));continue
        decisions.append(result('include','native_mapped_recording_candidate',members=members,
            candidate_id=digest([TASK5,members]),evidence_hash=digest(sorted([(myriad.key,h),(poly.key,poly_hash)])),
            relation_status='candidate',native_mapping=dict(source='myriad.externalSources',polymarket_market_id=target),
            outcome_bindings=check['bindings'],**check['comparison'],**check['direction_proof'],
            required_checks=['fresh_execution_preflight','settlement_and_void_equivalence',
                             'fees_and_payout_terms','time_alignment_and_capacity']))
    return sorted(decisions,key=lambda r:(r.get('market_id') or '',r.get('candidate_id') or '',r['reason']))


def normalize(value,at_ns):
    return from_legacy(value,observed_at_ns=at_ns,
        evidence_refs=[evidence_ref('task5_selection_receipt',digest(value),observed_at_ns=at_ns)],
        source_policy=value.get('policy') or 'shared-Task2-recording-rules+myriad-native-map/1')


def native_terminal(venue,raw):
    if venue!='myriad':
        from pair_recorder.selection import native_terminal as existing
        return existing(venue,raw)
    return raw.get('state') in ('closed','resolved') or raw.get('voided') is True
