import argparse
import json
import os
import shutil
import sys
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import StructType, StructField, StringType


def log(msg: str) -> None:
    print(f"[Splitter] {msg}", flush=True)


def build_spark(master: str, driver_memory: str) -> SparkSession:
    return (
        SparkSession.builder
        .master(master)
        .appName("FileSplitter")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def build_csv_schema() -> StructType:
    fields = [
        StructField("employee_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("hire_date", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("birth_date", StringType(), True),
    ]
    return StructType(fields)


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(build_csv_schema())
        .csv(path)
    )


def read_json(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(path)


def read_fixedwidth(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "false").text(path)
        .withColumnRenamed("value", "raw_line")
    )


def read_xml(spark: SparkSession, path: str) -> DataFrame:
    rows = []
    if os.path.isfile(path):
        files = [path]
    else:
        files = [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".xml")]

    for f in files:
        try:
            tree = ET.parse(f)
            root = tree.getroot()
            for child in root:
                rows.append((ET.tostring(child, encoding="unicode"),))
        except Exception as e:
            log(f"WARN: skipping {f}: {e}")

    schema = StructType([StructField("raw_xml", StringType(), True)])
    return spark.createDataFrame(rows, schema=schema)


def write_part(df: DataFrame, part_path: str, fmt: str) -> int:
    if fmt == "CSV":
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(part_path + "_tmp")
    elif fmt == "JSON":
        df.coalesce(1).write.mode("overwrite").json(part_path + "_tmp")
    elif fmt == "FIXEDWIDTH":
        df.coalesce(1).write.mode("overwrite").text(part_path + "_tmp")
    elif fmt == "XML":
        os.makedirs(part_path, exist_ok=True)
        rows = df.collect()
        with open(part_path + ".xml", "w", encoding="utf-8") as f:
            f.write("<employees>\n")
            for r in rows:
                f.write(r["raw_xml"] + "\n")
            f.write("</employees>\n")
        return len(rows)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    tmp_dir = part_path + "_tmp"
    part_files = []
    for root, _, files in os.walk(tmp_dir):
        for fn in files:
            if fn.startswith("part-"):
                part_files.append(os.path.join(root, fn))

    if not part_files:
        raise RuntimeError(f"No part files found in {tmp_dir}")

    src = part_files[0]
    ext = "csv" if fmt == "CSV" else ("json" if fmt == "JSON" else "txt")
    final = f"{part_path}.{ext}"
    shutil.move(src, final)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return df.count()


def main() -> int:
    parser = argparse.ArgumentParser(description="PySpark file splitter")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-size-kb", type=int, required=True)
    parser.add_argument("--format", required=True,
                        choices=["CSV", "JSON", "FIXEDWIDTH", "XML"])
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--driver-memory", default="1g")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        log(f"ERROR: input not found: {args.input}")
        return 1

    size_kb = os.path.getsize(args.input) / 1024.0
    os.makedirs(args.output_dir, exist_ok=True)

    log(f"Input: {args.input}")
    log(f"Format: {args.format}")
    log(f"Target size per part: {args.target_size_kb} KB")
    log(f"Source file size: {size_kb:.2f} KB")

    spark = build_spark(args.master, args.driver_memory)

    try:
        if args.format == "CSV":
            df = read_csv(spark, args.input)
        elif args.format == "JSON":
            df = read_json(spark, args.input)
        elif args.format == "FIXEDWIDTH":
            df = read_fixedwidth(spark, args.input)
        elif args.format == "XML":
            df = read_xml(spark, args.input)
        else:
            log(f"ERROR: unsupported format {args.format}")
            return 1

        total_rows = df.count()
        log(f"Total rows: {total_rows}")

        num_parts = max(1, int((size_kb / args.target_size_kb) + 0.999))
        log(f"File: {size_kb:.2f} KB → {num_parts} parts")

        df = df.repartition(num_parts)
        df_with_pid = df.withColumn("__pid", spark_partition_id())

        part_paths = []
        total_written = 0

        for pid in range(num_parts):
            part_df = df_with_pid.filter(f"__pid = {pid}").drop("__pid")
            base_name = os.path.join(args.output_dir, f"part-{pid:05d}")
            rows = write_part(part_df, base_name, args.format)
            total_written += rows

            ext = "csv" if args.format == "CSV" else ("json" if args.format == "JSON"
                                                     else ("xml" if args.format == "XML" else "txt"))
            produced = f"{base_name}.{ext}"
            actual_size_kb = os.path.getsize(produced) / 1024.0 if os.path.exists(produced) else 0
            log(f"  Part {pid}: {produced} ({rows} rows, {actual_size_kb:.2f} KB)")
            part_paths.append(produced)

        log(f"Done. rows={total_written}, parts={len(part_paths)}")

        result = {
            "status": "SUCCESS",
            "input": args.input,
            "format": args.format,
            "totalRows": total_written,
            "partCount": len(part_paths),
            "parts": part_paths,
            "summary": {
                "sourceSizeKb": round(size_kb, 2),
                "targetSizeKb": args.target_size_kb,
                "outputDir": args.output_dir,
            },
        }
        print("__RESULT_JSON__:" + json.dumps(result), flush=True)
        return 0

    except Exception as e:
        log(f"ERROR: {e}")
        result = {"status": "ERROR", "message": str(e)}
        print("__RESULT_JSON__:" + json.dumps(result), flush=True)
        return 1

    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())