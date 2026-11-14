"""Compare the first two real chunks against the published exact event fingerprints."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

REPO=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(REPO/'src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--runtime-lock',type=Path)
    args=parser.parse_args()
    from prediction_market_strategies.data.postprocess_runtime import configure_postprocess,runtime_lock_path
    configure_postprocess()
    spec=importlib.util.spec_from_file_location('runtime_check',REPO/'tools/run_v3_loop.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    runtime=module.verify_runtime(runtime_lock_path(REPO,args.runtime_lock))
    from verify_v3_snapshot import verify
    from prediction_market_strategies.data.hf_sync import read
    from prediction_market_strategies.data.nautilus_windows import catalog_paths
    from local_postprocess.nautilus import read_release,fingerprint
    from nautilus_trader.model import Currency,CurrencyType
    Currency.register(Currency('XPROB',0,0,'Dimensionless probability replay',CurrencyType.CRYPTO),overwrite=True)
    source=verify(args.root,read(REPO/'docs/data/hf-v3-e0a7982.snapshot.json'),allow_prefix=True)
    expected=read(REPO/'docs/validation/v3-portable-readback.json')['comparisons']
    if source['matched_chunks']<2:
        raise ValueError('first_two_real_chunks_required')
    results=[]
    for item in expected:
        digest=hashlib.sha256();count=0;clock=None
        with catalog_paths():
            for event in read_release(args.root/'chunks'/item['identity']/'nautilus'):
                if clock is not None and event.ts_init<clock:
                    raise ValueError('replay_clock_reversed')
                clock=event.ts_init
                digest.update(fingerprint(event).encode());digest.update(b'\n');count+=1
        if count!=item['events'] or digest.hexdigest()!=item['event_fingerprint_sha256']:
            raise ValueError('native_event_fingerprint_differs_from_reference:'+item['identity'])
        results.append(dict(identity=item['identity'],events=count,sha256=digest.hexdigest()))
    print(json.dumps(dict(status='passed_real_two_chunk_replay',runtime_commit=runtime['source_commit'],
        platform=runtime['platform'],wheel_sha256=runtime['wheel_sha256'],comparisons=results,
        full_dataset_validation=False,execution_ready=False)))


if __name__=='__main__':
    main()
