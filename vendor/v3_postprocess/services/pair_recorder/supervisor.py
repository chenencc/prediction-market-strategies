"""Single-owner persistent launcher; startup recovery and bounded crash retries.

Run as the same Windows user as the existing DPAPI store. Normal operator pauses
disable automatic re-launch; unexpected host resets retain the enabled marker.
"""
import argparse,hashlib,json,os,subprocess,sys,time,uuid
from pathlib import Path
import psutil
from marketdata.core import atomic
from .failure import details

class OwnerLock:
    def __init__(self,path):
        self.file=Path(path).open('a+b')
        if self.file.seek(0,2)==0:self.file.write(b'0');self.file.flush()
        self.file.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:self.file.close();raise RuntimeError('pair_supervisor_already_owned') from None
    def close(self):self.file.close()

def verify_release(source,release):
    pin=json.loads(Path(release).read_text())
    for name,expected in pin['files'].items():
        if hashlib.sha256((Path(source)/name).read_bytes()).hexdigest()!=expected:raise ValueError('frozen_source_changed')
    return pin['commit']

def retry_allowed(exit_code,crashes,now,*,max_crashes=3,window_seconds=600):
    recent=[at for at in crashes if now-at<window_seconds]
    if exit_code==0:return False,recent
    recent.append(now)
    return len(recent)<max_crashes,recent

def protection_failed(root,started_ns):
    for p in (Path(root)/'control/failure').glob('*.json'):
        if int(p.name.split('-')[0])<started_ns:continue
        row=json.loads(p.read_text())
        if any(c.get('error_type')=='ProtectionStop' or c.get('operation')=='resource_protection_stop' for c in row.get('diagnostic',{}).get('chain',[])):return True
    return False

def run(args):
    root=args.root.resolve();root.mkdir(parents=True,exist_ok=True);lock=OwnerLock(root/'supervisor.lock')
    try:
        service=root/'service.json'
        if not service.exists():raise ValueError('explicit_service_enable_required')
        cfg=json.loads(service.read_text())
        if not cfg.get('enabled') or cfg.get('blocked'):return 0
        commit=verify_release(args.source,args.release)
        # This probe releases immediately; the child takes the same runtime
        # lock before it can perform external work.
        probe=OwnerLock(root/'owner.lock');probe.close()
        previous=root/'supervisor-health.json';prior=json.loads(previous.read_text()) if previous.exists() else {}
        boot=psutil.boot_time();instance=uuid.uuid4().hex
        if prior.get('boot_time') and prior['boot_time']!=boot:
            atomic(root/'last-host-restart.json',dict(previous_boot=prior['boot_time'],boot_time=boot,last_supervisor_heartbeat_ns=prior.get('at_ns'),detected_at_ns=time.time_ns(),books_during_downtime_unavailable=True))
        crashes=cfg.get('recent_crashes',[])
        while json.loads(service.read_text()).get('enabled'):
            verify_release(args.source,args.release)
            attempt=root/'process-attempts'/str(time.time_ns());attempt.mkdir(parents=True)
            started_ns=time.time_ns()
            command=[sys.executable,'-B','-X','utf8','-m','pair_recorder','--root',str(root)]
            if args.dpapi_root:command+=['--dpapi-root',str(args.dpapi_root)]
            env=dict(os.environ);env['PYTHONPATH']=str(args.source/'services')
            with (attempt/'stdout.log').open('ab') as stdout,(attempt/'stderr.log').open('ab') as stderr:
                child=subprocess.Popen(command,cwd=args.source,env=env,stdout=stdout,stderr=stderr,stdin=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0,start_new_session=os.name!='nt')
                atomic(attempt/'start.json',dict(pid=child.pid,supervisor_pid=os.getpid(),started_ns=time.time_ns(),source_commit=commit,boot_time=boot))
                while child.poll() is None:
                    atomic(previous,dict(at_ns=time.time_ns(),pid=os.getpid(),child_pid=child.pid,instance=instance,boot_time=boot,source_commit=commit,attempt=str(attempt),status='running'))
                    time.sleep(5)
            ended=time.time_ns();atomic(attempt/'exit.json',dict(pid=child.pid,ended_ns=ended,exit_code=child.returncode))
            again,crashes=retry_allowed(child.returncode,crashes,time.time())
            protection=protection_failed(root,started_ns)
            if protection:again=False
            cfg=json.loads(service.read_text());cfg.update(recent_crashes=crashes)
            if not again:cfg.update(enabled=False,blocked=child.returncode!=0,stop_reason='normal_operator_pause' if child.returncode==0 else 'resource_protection_stop' if protection else 'bounded_crash_retries_exhausted')
            atomic(service,cfg)
            atomic(previous,dict(at_ns=ended,pid=os.getpid(),child_pid=child.pid,boot_time=boot,status='retry_wait' if again else cfg['stop_reason']))
            if not again:return child.returncode
            time.sleep(15)
        return 0
    finally:lock.close()

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--release',type=Path,required=True);p.add_argument('--dpapi-root',type=Path);args=p.parse_args()
    try:return run(args)
    except RuntimeError as error:
        if str(error) in ('pair_supervisor_already_owned','pair_runtime_already_owned'):return 0
        raise
    except Exception as error:
        atomic(args.root/'supervisor-failure.json',dict(at_ns=time.time_ns(),diagnostic=details(error)))
        return 2

if __name__=='__main__':raise SystemExit(main())
