"""One recording-pair filter for Task2 and Task5; mapping stays venue-specific."""
from .contracts import instant
from .rules import CREATION_RELATIVE,compare_rules,same_target


def fresh_catalog(packet,now,policy):
    age=(now-instant(packet['observed_at'])).total_seconds()
    if packet.get('http_status')!=200 or not 0<=age<=policy.max_metadata_age_seconds:
        raise ValueError('catalog_observation_not_fresh')


def recording_checks(other,poly,*,policy,bind):
    if type(policy.require_semantic_match) is not bool:raise ValueError('boolean_semantic_policy_required')
    def reject(reason,decision='review'):return dict(decision=decision,reason=reason)
    if not other.open_for_orders or not poly.open_for_orders:
        return reject('leg_not_orderable','exclude')
    if policy.excluded_categories:
        categories=[leg.raw.get('category') for leg in (other,poly)]
        if any(not isinstance(c,str) or not c.strip() for c in categories):
            return reject('category_review_required')
        if any(c.casefold() in {x.casefold() for x in policy.excluded_categories} for c in categories):
            return reject('pair_category_excluded','exclude')
    if any(len(m.outcomes)!=2 or any(not x.strip() for x in m.outcomes) or
        any(not v.strip() for v in m.outcomes.values()) or len(set(m.outcomes.values()))!=2 for m in (other,poly)):
        return reject('binary_outcome_identity_required')
    bound=bind(other,poly)
    if bound is None:return reject('outcome_direction_requires_review')
    bindings,direction=bound
    if any(not isinstance(m.raw.get('description'),str) or not m.rules.strip() or
        not isinstance(m.raw.get('question') or m.raw.get('title'),str) or not m.question.strip() for m in (other,poly)):
        return reject('question_or_rules_missing')
    if not policy.require_semantic_match:
        return dict(decision='include',reason='native_mapped_recording_candidate',bindings=bindings,
            direction_proof=direction,comparison=dict(policy='native-mapped-recording/2',
                recording_filter=dict(question_target_match_required=False,rules_match_required=False,
                    creation_clock_match_required=False,outcome_direction_required=True)))
    if not same_target(other.question,poly.question):return reject('question_target_requires_review')
    # The footer exception is specifically backed by Predict metadata. It must
    # not silently become a Myriad rule-removal heuristic.
    equal,comparison=compare_rules(other.rules,poly.rules,other.raw.get('createdAt') if other.venue=='predictfun' else None)
    if not equal:return reject('rules_differ_requires_review')
    if CREATION_RELATIVE.search(other.rules):
        try:same_creation=instant(other.raw.get('createdAt'))==instant(poly.raw.get('createdAt'))
        except (ValueError,TypeError):same_creation=False
        if not same_creation:return reject('creation_relative_rules_require_review')
    return dict(decision='include',reason='native_mapped_recording_candidate',bindings=bindings,
                direction_proof=direction,comparison=comparison)
