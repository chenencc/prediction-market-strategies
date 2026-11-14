"""Content-addressed extension objects; no whole-page or per-Pair context copies."""
from .fees import canonical,digest

def reference(payload,objects,*,available_at_ns):
    if payload is None or payload=={} or payload==[]:return None
    text=canonical(payload);ident=digest(payload)
    objects.setdefault(ident,dict(schema='shared-archive-object/1',venue='shared',market_id='',object_id=ident,
        payload_json=text,available_at_ns=available_at_ns))
    return ident
