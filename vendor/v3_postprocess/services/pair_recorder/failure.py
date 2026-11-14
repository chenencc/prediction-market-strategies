"""Diagnostic fields only: never exception messages, URLs, headers or locals."""
from pathlib import Path
import traceback

class ProtectionStop(RuntimeError):
    def __init__(self,operation):
        super().__init__(operation);self.safe_operation=operation

def details(error):
    chain=[];seen=set()
    while error is not None and id(error) not in seen and len(chain)<4:
        seen.add(id(error))
        chain.append(dict(error_type=type(error).__name__,operation=getattr(error,'safe_operation',None),
            filename=getattr(error,'safe_filename',None),errno=getattr(error,'errno',None),
            winerror=getattr(error,'winerror',None),http_status=getattr(error,'http_status',None),
            frames=[dict(file=Path(f.filename).name,function=f.name,line=f.lineno) for f in traceback.extract_tb(error.__traceback__)[-8:]]))
        error=error.__cause__ or error.__context__
    return dict(chain=chain,credentials_logged=False)
