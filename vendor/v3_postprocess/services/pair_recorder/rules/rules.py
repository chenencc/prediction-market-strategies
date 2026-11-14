"""Narrow, auditable rule comparison. Never modify source metadata."""
import re
from .contracts import digest, instant

CREATION_RELATIVE = re.compile(
    r'market creation|creation of (?:this|the) market|market.s creation|'
    r'(?:market|contract) (?:is |was |being )?(?:created|listed|launched)|'
    r'(?:after|since|from|of).{0,20}(?:listing|launch|inception)', re.I)
FOOTER = re.compile(
    r'\n[ \t\r]*\nCreated At:[ \t]*(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)[ \t\r\n]*\Z')


def same_target(left, right):
    """A shared template/mapping alone cannot prove identical strikes or dates.

    Only whitespace and case are immaterial here. Paraphrases stay review until
    a separate, explicit target equivalence proof exists; never drop numbers.
    """
    return (isinstance(left, str) and isinstance(right, str)
            and bool(left.strip()) and bool(right.strip())
            and ' '.join(left.split()).casefold() == ' '.join(right.split()).casefold())


def compare_rules(predict_rules, polymarket_rules, predict_created_at):
    """Return (equal, optional normalization receipt).

    Exact whitespace-normalized matches retain the previous receipt shape.
    Only a single terminal Predict metadata footer may be removed, only when
    its timestamp agrees with native createdAt within five seconds, the body
    has no recognized creation-relative condition and otherwise matches exactly.
    This does not establish settlement equivalence or interpret differing terms.
    """
    normalize = lambda value: ' '.join(value.split())
    if normalize(predict_rules) == normalize(polymarket_rules):
        return True, {}
    match = FOOTER.search(predict_rules)
    if not match or predict_rules.count('Created At:') != 1:
        return False, {}
    body = predict_rules[:match.start()]
    if CREATION_RELATIVE.search(body):
        return False, {}
    try:
        delta = abs((instant(match[1]) - instant(predict_created_at)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return False, {}
    if delta > 5 or normalize(body) != normalize(polymarket_rules):
        return False, {}
    return True, {'rules_comparison': dict(
        policy='predict-terminal-created-at/1',
        removed_field='terminal_created_at_metadata', footer_timestamp=match[1],
        native_created_at=predict_created_at, clock_difference_seconds=delta,
        original_predict_rules_hash=digest(predict_rules),
        original_polymarket_rules_hash=digest(polymarket_rules),
        compared_body_hash=digest(normalize(body)))}
