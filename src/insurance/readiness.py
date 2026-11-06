from .config import src_table
from .io import read_snapshot
 
 
def run(spark, cfg, as_of_date: str) -> None:
    """Gerbang data: pastikan data mart untuk tanggal ini sudah tersedia."""
    print("[readiness_check]")
    for key in cfg["source"]["tables"]:
        n = read_snapshot(spark, src_table(cfg, key), as_of_date).count()
        print(f"  {key}: {n} baris")
        if n == 0:
            # Job akan mencoba ulang sesuai max_retries di definisi job
            raise RuntimeError(f"Data mart '{key}' untuk {as_of_date} belum tersedia")