from pyspark.sql import functions as F
 
from .config import ops_table, out_table, src_table
from .io import read_snapshot, write_snapshot
 
 
def run(spark, cfg, as_of_date: str) -> None:
    print("[quality]")
    tol = float(cfg["quality"]["amount_tolerance"])
 
    src_policy = read_snapshot(spark, src_table(cfg, "policy"), as_of_date)
    src_claim = read_snapshot(spark, src_table(cfg, "claim"), as_of_date)
    pf = read_snapshot(spark, out_table(cfg, "policy_portfolio"), as_of_date)
    c360 = read_snapshot(spark, out_table(cfg, "customer_360"), as_of_date)
    pf_present = pf.filter(F.col("is_present_in_source"))
 
    results = []
 
    def check(name, severity, failed):
        results.append((name, severity, int(failed)))
        print(f"  [{'OK' if failed == 0 else 'GAGAL'}] {name} ({severity}): {failed}")
 
    def total(df, col):
        return df.agg(F.sum(col)).first()[0] or 0.0
 
    # Output valid
    check("portfolio_policy_unique", "error",
          pf.count() - pf.select("policy_id").distinct().count())
    check("portfolio_status_valid", "error",
          pf.filter(~F.col("status").isin(cfg["quality"]["allowed_status"])).count())
 
    # Parity: output harus mencerminkan sumber
    check("parity_policy_count", "error", abs(src_policy.count() - pf_present.count()))
    check("parity_annual_premium", "error",
           int(abs(total(src_policy, "annual_premium") - total(pf_present, "annual_premium")) > tol))
    check("parity_customer360_active_policies", "error",
          abs(total(c360, "active_policy_count") - pf.filter(F.col("status") == "ACTIVE").count()))
 
    # Konsistensi internal data mart (tanggung jawab tim DM, cukup dilaporkan)
    claim_sum = src_claim.groupBy("policy_id").agg(F.sum("claimed_amount").alias("s"))
    rec = (src_policy.join(claim_sum, "policy_id", "left").fillna({"s": 0.0})
           .filter(F.abs(F.col("total_claimed_amount") - F.col("s")) > tol))
    check("source_policy_vs_claim_amount", "warning", rec.count())
 
    result_df = (spark.createDataFrame(results, "check_name string, severity string, failed_count long")
                 .withColumn("data_as_of_date", F.to_date(F.lit(as_of_date)))
                 .withColumn("checked_at", F.current_timestamp()))
    write_snapshot(spark, result_df, ops_table(cfg), as_of_date)
 
    errors = [r[0] for r in results if r[1] == "error" and r[2] > 0]
    if errors:
        raise ValueError(f"Quality check gagal: {errors}")