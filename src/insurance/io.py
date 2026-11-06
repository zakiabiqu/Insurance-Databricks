from pyspark.sql import functions as F
 
DATE_COL = "data_as_of_date"
 
 
def read_snapshot(spark, full_name: str, as_of_date: str):
    return spark.table(full_name).filter(F.col(DATE_COL) == F.to_date(F.lit(as_of_date)))
 
 
def latest_snapshot_before(spark, full_name: str, as_of_date: str):
    """Snapshot terakhir sebelum as_of_date. None kalau belum ada (first run)."""
    if not spark.catalog.tableExists(full_name):
        return None
    row = (spark.table(full_name)
           .filter(F.col(DATE_COL) < F.to_date(F.lit(as_of_date)))
           .agg(F.max(DATE_COL).alias("d"))
           .first())
    if row is None or row["d"] is None:
        return None
    print(f"  snapshot sebelumnya: {row['d']}")
    return spark.table(full_name).filter(F.col(DATE_COL) == F.lit(row["d"]))
 
 
def write_snapshot(spark, df, full_name: str, as_of_date: str) -> None:
    """Idempotent: dijalankan ulang untuk tanggal yang sama tidak membuat data dobel."""
    if spark.catalog.tableExists(full_name):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"{DATE_COL} = '{as_of_date}'")
           .saveAsTable(full_name))
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full_name)
    print(f"  → {full_name} [{as_of_date}] ditulis: {df.count()} baris")