from pyspark.sql import functions as F
 
from .config import out_table, src_table
from .io import latest_snapshot_before, read_snapshot, write_snapshot
 
# Atribut yang dibawa dari snapshot lalu kalau polis hilang dari sumber
DESCRIPTIVE_COLS = ["customer_key", "policyholder_name", "policyholder_city", "customer_segment",
                    "product_line", "product_name", "agent_name", "agent_channel",
                    "policy_start_date", "policy_end_date", "sum_insured", "annual_premium"]
 
# Metrik bulan berjalan, hanya dari snapshot sekarang
METRIC_COLS = ["has_late_payment", "late_installments", "late_payment_bucket",
               "claim_count", "open_claim_count", "total_claimed_amount", "total_approved_amount",
               "open_sla_breach_count", "rejected_claim_count", "last_claim_date"]
OUTPUT_COLS = (["data_as_of_date", "policy_id"] + DESCRIPTIVE_COLS
               + ["status", "status_date", "first_seen_date", "is_present_in_source"]
               + METRIC_COLS + ["data_version", "_processed_at"])
 
 
def _claim_features(claim):
    """Fitur klaim yang tidak tersedia di data mart polis."""
    is_open = F.col("is_open_claim") == "Y"
    return claim.groupBy("policy_id").agg(
        F.sum((is_open & (F.col("is_sla_breach") == 
"Y")).cast("int")).alias("open_sla_breach_count"),
        F.sum((F.col("claim_status") == "REJECTED").cast("int")).alias("rejected_claim_count"),
        F.max("claim_date").alias("last_claim_date"),
    )
 
 
def _build_current(policy, claim):
    cur = policy.select(
        "policy_id",
        F.concat_ws("|", F.upper(F.trim("policyholder_name")),
                    F.upper(F.trim("policyholder_city"))).alias("customer_key"),
        "policyholder_name", "policyholder_city", "customer_segment",
        "product_line", "product_name", "agent_name", "agent_channel",
        "policy_start_date", "policy_end_date", "sum_insured", "annual_premium",
        F.upper(F.trim("policy_status")).alias("status"),
        (F.col("has_late_payment") == "Y").alias("has_late_payment"),
        "late_installments", "late_payment_bucket",
        "claim_count", "open_claim_count", "total_claimed_amount", "total_approved_amount",
    )
    return (cur.join(_claim_features(claim), "policy_id", "left")
               .fillna(0, subset=["open_sla_breach_count", "rejected_claim_count"]))
 
 
def _reconcile(cur, prev, as_of_date: str):
    as_of = F.to_date(F.lit(as_of_date))
 
    if prev is None:  # first run: belum ada riwayat
        return (cur.withColumn("status_date",
                               F.when(F.col("status") == "ACTIVE", F.col("policy_start_date"))
                                .otherwise(as_of))
                   .withColumn("first_seen_date", as_of)
                   .withColumn("is_present_in_source", F.lit(True)))
 
    c, p = cur.alias("c"), prev.alias("p")
    j = c.join(p, F.col("c.policy_id") == F.col("p.policy_id"), "full")
 
    in_current = F.col("c.policy_id").isNotNull()
    is_new = in_current & F.col("p.policy_id").isNull()
    status = F.when(in_current, F.col("c.status")).otherwise(F.lit("INACTIVE"))
 
    status_date = (
        F.when(is_new, F.when(F.col("c.status") == "ACTIVE", F.col("c.policy_start_date"))
                        .otherwise(as_of))                          # polis baru
         .when(status == F.col("p.status"), F.col("p.status_date"))  # status sama: bawa tanggal lama
         .otherwise(as_of)                                           # status berubah
    )
 
    return j.select(
        F.coalesce(F.col("c.policy_id"), F.col("p.policy_id")).alias("policy_id"),
        *[F.coalesce(F.col(f"c.{x}"), F.col(f"p.{x}")).alias(x) for x in DESCRIPTIVE_COLS],
        status.alias("status"),
        status_date.alias("status_date"),
        F.coalesce(F.col("p.first_seen_date"), as_of).alias("first_seen_date"),
        in_current.alias("is_present_in_source"),
        *[F.col(f"c.{x}").alias(x) for x in METRIC_COLS],
    )
 
 
def _validate(df) -> None:
    """Padanan 'CEK NULL PRODUCT' di script lama: gagalkan sebelum menulis."""
    dup = df.count() - df.select("policy_id").distinct().count()
    null_status = df.filter(F.col("status").isNull()).count()
    if dup or null_status:
        raise ValueError(f"Validasi gagal: duplikat={dup}, status_null={null_status}")
 
 
def run(spark, cfg, as_of_date: str) -> None:
    print("[policy_portfolio]")
    target = out_table(cfg, "policy_portfolio")
 
    policy = read_snapshot(spark, src_table(cfg, "policy"), as_of_date)
    claim = read_snapshot(spark, src_table(cfg, "claim"), as_of_date)
    prev = latest_snapshot_before(spark, target, as_of_date)
 
    df = (_reconcile(_build_current(policy, claim), prev, as_of_date)
          .withColumn("data_as_of_date", F.to_date(F.lit(as_of_date)))
          .withColumn("data_version", F.lit(cfg["data_version"]))
          .withColumn("_processed_at", F.current_timestamp())
          .select(OUTPUT_COLS))
 
    _validate(df)
    write_snapshot(spark, df, target, as_of_date)