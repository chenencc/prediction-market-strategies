"""One portable selection interface; business classifiers supply evidence only.

Legacy receipt normalization does not reclassify markets or prove their current
eligibility. No business-rule engine is copied into this Stage 1 bridge.
"""
from copy import deepcopy
from typing import Protocol
from .contracts import require, members as member_list, integer, text, evidence, digest, seal, verified, sha
from .registry import SUPPORTED_KINDS as KINDS, OPENING, CROSS_SECTIONAL, definition, validate_members, load_definitions,definition_hash


class Selector(Protocol):
    def select(self, snapshot: dict, *, observed_at_ns: int) -> list[dict]:
        """Return sealed task-candidate/1 receipts; no networking or dispatch."""
        ...


def candidate(kind, *, members, decision, reason, observed_at_ns, evidence_refs,
              source_policy, source_receipt_hash=None):
    require(kind in KINDS, 'unsupported_task_type')
    require(decision in {'include','exclude','review'}, 'invalid_selection_decision')
    values = member_list(members, allow_empty=decision != 'include')
    if decision == 'include':
        validate_members(kind, values)
    d = definition(kind)
    if source_receipt_hash is not None:
        sha(source_receipt_hash)
    return seal('task-candidate/1', task_type=kind, members=values,
                group_id=digest([kind, values]) if values else None,
                decision=decision, reason=text(reason, 'selection_reason_required'),
                observed_at_ns=integer(observed_at_ns, 'selection_clock_required'),
                evidence_refs=evidence(evidence_refs), source_policy=text(source_policy, 'source_policy_required'),
                source_receipt_hash=source_receipt_hash,
                definitions_hash=definition_hash(kind),
                requires_intelligence=d['requires_intelligence'],
                recording_authorized=False, execution_authorized=False)


def validate_candidate(value):
    result = verified(value, 'task-candidate/1')
    rebuilt = candidate(result['task_type'], **{k:result[k] for k in (
        'members','decision','reason','observed_at_ns','evidence_refs','source_policy','source_receipt_hash')})
    require(result == rebuilt, 'candidate_fields_inconsistent')
    return result


def from_legacy(receipt, *, observed_at_ns, evidence_refs, source_policy=None):
    """Preserve an actual old decision/reason/hash; never invent a passing proof."""
    receipt = deepcopy(receipt)
    kind = receipt.get('task_type')
    values = receipt.get('members', [])
    if kind == OPENING:
        values = [receipt['question_id']] if receipt.get('question_id') else []
    if kind == CROSS_SECTIONAL and not values and receipt.get('market_id'):
        values = ['polymarket:' + str(receipt['market_id'])]
    require(receipt.get('recording_authorized', False) is False and
            receipt.get('execution_authorized', False) is False, 'legacy_execution_receipt_not_selection')
    return candidate(kind, members=values, decision=receipt['decision'], reason=receipt.get('reason') or 'legacy_structured_verdict',
        observed_at_ns=observed_at_ns, evidence_refs=evidence_refs,
        source_policy=source_policy or receipt.get('policy') or receipt.get('schema'),
        source_receipt_hash=digest(receipt))


def allocate(candidates, *, assigned=None):
    """Pure priority arbitration, not a lease store or capacity allocator.

    Existing ownership is sticky. A conflicting leg excludes the whole Pair or
    combination. Physical-feed sharing belongs to the later runtime layer.
    """
    owners = deepcopy(assigned or {})
    for key, value in owners.items():
        member_list([key]);require(value['task_type'] in KINDS, 'invalid_existing_owner')
        sha(value.get('group_id'))
    rows = [validate_candidate(c) for c in candidates]
    rows.sort(key=lambda c:(definition(c['task_type'])['priority'], c['group_id'] or '', c['contract_hash']))
    decisions = [];seen = set()
    for c in rows:
        if c['decision'] != 'include':
            outcome, reason = 'not_selected', 'screening_not_included'
        elif c['group_id'] in seen:
            outcome, reason = 'excluded', 'duplicate_candidate'
        else:
            seen.add(c['group_id'])
            conflicts = [m for m in c['members'] if m in owners and owners[m]['group_id'] != c['group_id']]
            if conflicts:
                outcome, reason = 'excluded', 'market_already_assigned'
            else:
                outcome, reason = 'assigned', 'exclusive_market_assignment'
                for m in c['members']:
                    owners[m] = dict(task_type=c['task_type'], group_id=c['group_id'])
        decisions.append(dict(candidate_hash=c['contract_hash'], group_id=c['group_id'], task_type=c['task_type'],
                              members=c['members'], allocation=outcome, reason=reason))
    return seal('task-allocation/1', decisions=decisions, assignments=owners,
                recording_authorized=False, execution_authorized=False)
