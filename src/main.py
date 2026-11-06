import argparse
import inspect
import os
import sys
from datetime import datetime

try:
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # serverless Databricks menjalankan file lewat exec(), tanpa __file__
    SRC_DIR = os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename))
sys.path.insert(0, SRC_DIR)
 
from pyspark.sql import SparkSession
 
from insurance import customer360, portfolio, quality, readiness
from insurance.config import load_config
 
STEPS = {
    "readiness_check": readiness.run,
    "policy_portfolio": portfolio.run,
    "customer_360": customer360.run,
    "quality": quality.run,
}
 
 
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Insurance customer portfolio pipeline")
    parser.add_argument("--env", required=True, choices=["dev", "stg", "prd"])
    parser.add_argument("--step", required=True, choices=list(STEPS))
    parser.add_argument("--as-of-date", required=True, help="Format YYYY-MM-DD")
    return parser.parse_args(argv)
 
 
def main(argv=None):
    args = parse_args(argv)
    datetime.strptime(args.as_of_date, "%Y-%m-%d")
 
    cfg = load_config(args.env)
    spark = SparkSession.builder.getOrCreate()
 
    print(f"[{args.env}] step={args.step} as_of={args.as_of_date} "
          f"source={cfg['source_catalog']} target={cfg['catalog']}")
    STEPS[args.step](spark, cfg, args.as_of_date)
 
 
if __name__ == "__main__":
    main()