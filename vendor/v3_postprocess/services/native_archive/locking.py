"""One local publisher per extension dataset; no waiting in a receive loop."""
from pathlib import Path
class PublicationLock:
    def __init__(self,path):
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);self.file=path.open('a+b')
        if self.file.seek(0,2)==0:self.file.write(b'0');self.file.flush()
        self.file.seek(0)
        try:
            if __import__('os').name=='nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:self.file.close();raise RuntimeError('archive_dataset_already_owned') from None
    def __enter__(self):return self
    def __exit__(self,*args):self.file.close()
