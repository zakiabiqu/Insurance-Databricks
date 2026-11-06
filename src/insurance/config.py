from pathlib import Path
 
import yaml
 
CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
 
 
def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
 
 
def load_config(env: str) -> dict:
    env_file = CONFIG_DIR / f"{env}.yaml"
    if not env_file.exists():
        raise FileNotFoundError(f"Config environment '{env}' tidak ditemukan: {env_file}")
    base = yaml.safe_load((CONFIG_DIR / "base.yaml").read_text()) or {}
    override = yaml.safe_load(env_file.read_text()) or {}
    return _deep_merge(base, override)
 
 
def src_table(cfg: dict, key: str) -> str:
    return f"{cfg['source_catalog']}.{cfg['source']['schema']}.{cfg['source']['tables'][key]}"
 
 
def out_table(cfg: dict, key: str) -> str:
    return f"{cfg['catalog']}.{cfg['target']['schema']}.{cfg['target']['tables'][key]}"
 
 
def ops_table(cfg: dict) -> str:
    return f"{cfg['catalog']}.{cfg['ops']['schema']}.{cfg['ops']['dq_table']}"