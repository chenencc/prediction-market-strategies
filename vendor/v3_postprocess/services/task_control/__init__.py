"""Versioned, provider-free contracts for all four collection tasks.

This package describes decisions and requests. It does not dispatch collectors,
claim a runtime lease, contact providers, publish data or authorize execution.
"""
from .registry import OPENING, PAIR, COMBINATION, CROSS_SECTIONAL, MYRIAD_PAIR, TASK5, load_definitions, load_config
from .selection import candidate, from_legacy, allocate
from .contracts import ContractError, digest, evidence_ref
from .requests import recording_request, segment, storage_route, footprint, validate_request, validate_segment
from .state import initial_state, transition, completion

__all__ = ['OPENING','PAIR','COMBINATION','CROSS_SECTIONAL','MYRIAD_PAIR','TASK5','load_definitions','load_config',
           'candidate','from_legacy','allocate','ContractError','digest','evidence_ref',
           'recording_request','segment','storage_route','footprint','validate_request','validate_segment',
           'initial_state','transition','completion']
