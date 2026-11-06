from pyspark.sql import functions as F
 
from .config import out_table
from .io import read_snapshot, write_snapshot
 
 
def run(spark, cfg, as_of_date: str) -> None:
    print("[customer_360]")
    pf = read_snapshot(spark, out_table(cfg, "policy_portfolio"), as_of_date)
    active = F.col("status") == "ACTIVE"
 
    df = (pf.groupBy("data_as_of_date", "customer_key")
          .agg(F.first("policyholder_name").alias("policyholder_name"),
               F.first("policyholder_city").alias("policyholder_city"),
               F.max("customer_segment").alias("customer_segment"),
               F.count("*").alias("total_policy_count"),
               F.sum(active.cast("int")).alias("active_policy_count"),
               F.array_sort(F.collect_set(F.when(active, F.col("product_line"))))
                .alias("active_product_lines"),
               F.round(F.sum(F.when(active, F.col("annual_premium")).otherwise(0)), 2)
                .alias("active_annual_premium"),
               F.round(F.sum(F.when(active, F.col("sum_insured")).otherwise(0)), 2)
                .alias("active_sum_insured"),
               
F.max(F.col("has_late_payment").cast("int")).cast("boolean").alias("has_late_payment"),
               F.sum("open_claim_count").alias("open_claim_count"),
               F.sum("open_sla_breach_count").alias("open_sla_breach_count"),
               F.round(F.sum("total_approved_amount"), 2).alias("total_approved_claim_amount"),
               F.min("policy_start_date").alias("first_policy_start_date"))
          .withColumn("active_product_line_count", F.size("active_product_lines"))
          .withColumn("tenure_days", F.datediff("data_as_of_date", "first_policy_start_date"))
          .withColumn("data_version", F.lit(cfg["data_version"]))
          .withColumn("_processed_at", F.current_timestamp()))
 
    write_snapshot(spark, df, out_table(cfg, "customer_360"), as_of_date)