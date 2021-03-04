import argparse
import pyspark
from pyspark.sql import SparkSession
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_input", type=str, help="s3 input path")
    parser.add_argument("--s3_output", type=str, help="s3 output path")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()

    # DOWNLOAD DATA FROM S3 PATH
    df = spark.read.csv(args.s3_input, header=True)

    
    # ==================================================
    # ============= DO PROCESSING HERE =================
    # ==================================================

    
    # UPLOAD PROCESSED DATA TO S3
    df.write.save(args.s3_output, format='csv', header=True)

