"""Activate the hash-bound, bundled V3 readers without a collector checkout."""
import hashlib
import json
import platform
from pathlib import Path
import sys


def runtime_lock_path(repo, explicit=None):
    if explicit is not None:
        return Path(explicit).resolve()
    repo = Path(repo)
    if platform.system() == 'Darwin':
        if platform.machine() != 'arm64':
            raise ValueError('fixed_macos_wheel_requires_native_apple_silicon_python_not_intel_or_rosetta')
        return repo/'runtime.compatibility.macos-arm64.lock.json'
    if platform.system() != 'Windows':
        raise ValueError('platform_runtime_requires_explicit_validated_lock')
    return repo/'runtime.compatibility.lock.json'


def configure_postprocess(collector=None):
    repo = Path(__file__).resolve().parents[3]
    root = Path(collector).resolve() if collector else repo/'vendor/v3_postprocess'
    if collector is None:
        manifest = json.loads((root/'SOURCE.json').read_bytes())
        for relative, expected in manifest['exact_files_sha256'].items():
            path = (root/relative).resolve()
            if not path.is_relative_to(root.resolve()):
                raise ValueError('bundled_source_path_outside_root')
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError('bundled_postprocessor_changed:'+relative)
    services = root/'services'
    if not (services/'local_postprocess/nautilus.py').is_file():
        raise ValueError('postprocessor_source_missing')
    # Refuse mixed import roots instead of silently reusing already loaded code.
    for name, module in list(sys.modules.items()):
        if name.split('.')[0] in {'local_postprocess','mixed_recorder','marketdata',
                                 'native_archive','pair_recorder','task_control'}:
            filename = getattr(module, '__file__', None)
            if filename and not Path(filename).resolve().is_relative_to(services.resolve()):
                raise ValueError('postprocessor_already_loaded_from_another_root:'+name)
    if str(services) not in sys.path:
        sys.path.insert(0, str(services))
    return root
