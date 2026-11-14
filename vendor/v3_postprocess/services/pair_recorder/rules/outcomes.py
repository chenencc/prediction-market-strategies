"""Bind native outcomes without guessing from array order or fuzzy aliases."""
from .contracts import digest, valid_id


def _same_team(left, right, *, provider_id=False):
    """Compare declared identities within one provider namespace only."""
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    if not all(valid_id(t.get('id')) for t in (left, right)):
        return False
    names = [t.get('name') for t in (left, right)]
    if not all(isinstance(n, str) and n.strip() for n in names):
        return False
    if str(left['id']) != str(right['id']) or names[0].lower() != names[1].lower():
        return False
    if provider_id:
        return (all(valid_id(t.get('providerTeamId')) for t in (left, right)) and
                str(left['providerTeamId']) == str(right['providerTeamId']))
    return True


def _declared_team(row, market_variant):
    details = row.get('variantDetails')
    sports = details.get('sports') if isinstance(details, dict) else None
    if not isinstance(sports, dict):
        return None
    provider, team = sports.get('provider'), sports.get('team')
    if provider not in {'POLYMARKET', 'PREDICT_SPORTS'} or not isinstance(team, dict):
        return None
    if not valid_id(team.get('providerTeamId')):
        return None
    native = row.get('team')
    if provider == 'POLYMARKET':
        if not _same_team(team, native):
            return None
        sources = ['team', 'variantDetails.sports.team']
    else:
        # PREDICT_SPORTS uses its own ID namespace and may omit legacy team.
        # Compare the two declared schemas; neither is independent oracle proof.
        variant = row.get('variantData')
        if (not isinstance(variant, dict) or variant.get('provider') != provider
                or not isinstance(market_variant, str) or not market_variant.strip()
                or variant.get('type') != market_variant
                or not _same_team(team, variant.get('team'), provider_id=True)):
            return None
        if native is not None and not _same_team(team, native):
            return None
        sources = ['variantData.team', 'variantDetails.sports.team']
        if native is not None:
            sources.append('team')
    return provider, team, sources


def bind_outcomes(pred, poly, *, require_question_match=True):
    """Return bindings and optional proof, or None when direction is unproved.

    The sports fallback uses supported Predict-declared team metadata, exact
    event identity and exact target outcome names. Provider team IDs are retained
    as assertions, not claimed independently verified against a team registry.
    """
    rows = pred.raw.get('outcomes', [])
    if len(rows) != 2 or any(type(r.get('indexSet')) is not int or
                             not isinstance(r.get('name'), str) for r in rows):
        return None
    slots = {r['indexSet']: r['name'].lower() for r in rows}
    if set(slots) != {1, 2}:
        return None
    sports_identity_present = pred.raw.get('marketType') == 'SPORTS_MONEYLINE' and any(
        isinstance(r.get('variantDetails'), dict) and 'sports' in r['variantDetails'] for r in rows)
    # Explicit sports identity conflicts must not be hidden by equal display names.
    if set(slots.values()) == set(poly.outcomes) and not sports_identity_present:
        return [dict(label=label, polymarket_asset_id=poly.outcomes[label],
                     predict_asset_id=pred.outcomes[label], predict_index_set=slot,
                     predict_book_origin='native' if slot == 1 else 'derived_complement')
                for slot, label in sorted(slots.items())], {}

    # Narrow scope: whole-match moneyline and the same native event/teams.
    # Strict historical replay additionally requires the original exact question.
    # This deliberately excludes spreads, totals, props and approximate names.
    if (pred.raw.get('marketType') != 'SPORTS_MONEYLINE' or
            poly.raw.get('sportsMarketType') != 'moneyline' or
            (require_question_match and pred.question != poly.question)):
        return None
    event = pred.raw.get('categorySlug')
    events = poly.raw.get('events')
    if (not isinstance(event, str) or not event.strip() or not isinstance(events, list)
            or len(events) != 1 or not isinstance(events[0], dict)
            or events[0].get('slug') != event):
        return None

    bindings, evidence = [], []
    providers = set()
    labels, local_ids, provider_ids = set(), set(), set()
    for row in sorted(rows, key=lambda r: r['indexSet']):
        declared = _declared_team(row, pred.raw.get('marketVariant'))
        if declared is None:
            return None
        provider, team, sources = declared
        providers.add(provider)
        if len(providers) != 1:
            return None
        local_id, provider_id = str(team['id']), str(team['providerTeamId'])
        name = team['name']
        label = name.lower()
        if (label not in poly.outcomes or label in labels or local_id in local_ids
                or provider_id in provider_ids):
            return None
        labels.add(label); local_ids.add(local_id); provider_ids.add(provider_id)
        slot = row['indexSet']
        bindings.append(dict(label=label, polymarket_asset_id=poly.outcomes[label],
                             predict_asset_id=pred.outcomes[row['name'].lower()],
                             predict_index_set=slot,
                             predict_book_origin='native' if slot == 1 else 'derived_complement'))
        evidence.append(dict(predict_label=row['name'], polymarket_label=label,
                             predict_index_set=slot, predict_team_id=local_id,
                             declared_provider=provider, compared_fields=sources,
                             declared_provider_team_id=provider_id,
                             team_metadata_hash=digest({k:row.get(k) for k in ['team','variantData','variantDetails']})))
    if labels != set(poly.outcomes):
        return None
    return bindings, {'outcome_binding_proof': dict(
        policy='predict-declared-sports-team/2' if require_question_match else 'predict-declared-sports-team/3', event_slug=event,
        method='native_mapping_exact_event_question_and_team_names' if require_question_match else 'native_mapping_exact_event_and_team_names',
        independent_provider_team_registry_verified=False, teams=evidence)}
