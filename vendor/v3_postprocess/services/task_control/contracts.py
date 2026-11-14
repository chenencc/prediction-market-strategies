"""Exact identity and hash contracts; timestamps are explicit Unix nanoseconds."""
from copy import deepcopy
import hashlib
import json
import re


class ContractError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise ContractError(code)


def digest(value):
    wire = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':'))
    return hashlib.sha256(wire.encode('utf8')).hexdigest()


def integer(value, code, minimum=0):
    require(type(value) is int and value >= minimum, code)
    return value


def sha(value):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value), 'invalid_sha256')
    return value


def text(value, code):
    require(isinstance(value, str) and bool(value.strip()), code)
    return value


def member(value):
    require(isinstance(value, str), 'invalid_member')
    parts = value.split(':')
    if parts[0]=='myriad':
        require(len(parts)==4 and parts[1]=='56' and parts[2]=='ob' and parts[3].isascii() and parts[3].isdecimal() and int(parts[3])>0 and str(int(parts[3]))==parts[3],
            'invalid_myriad_native_member')
        return value
    require(len(parts) == 2 and parts[0] in {'polymarket','predictfun'} and
            re.fullmatch('[A-Za-z0-9_-]+', parts[1]), 'invalid_member')
    return value


def members(values, *, allow_empty=False):
    require(isinstance(values, list), 'members_must_be_list')
    result = [member(v) for v in values]
    require(len(set(result)) == len(result), 'duplicate_member')
    require(allow_empty or bool(result), 'members_required')
    return sorted(result)


def evidence_ref(kind, sha256, *, observed_at_ns=None):
    result = dict(kind=text(kind, 'evidence_kind_required'), sha256=sha(sha256))
    if observed_at_ns is not None:
        result['observed_at_ns'] = integer(observed_at_ns, 'invalid_evidence_clock')
    return result


def evidence(values):
    require(isinstance(values, list) and bool(values), 'evidence_references_required')
    for ref in values:
        require(isinstance(ref, dict) and set(ref) <= {'kind','sha256','observed_at_ns'}, 'invalid_evidence_reference')
        evidence_ref(**ref)
    return deepcopy(values)


def seal(schema, **fields):
    result = deepcopy(dict(schema=schema, **fields))
    result['contract_hash'] = digest(result)
    return result


def verified(value, schema):
    require(isinstance(value, dict) and value.get('schema') == schema, 'contract_schema_mismatch')
    sha(value.get('contract_hash'))
    require(digest({k:v for k,v in value.items() if k != 'contract_hash'}) == value['contract_hash'], 'contract_hash_mismatch')
    return deepcopy(value)
