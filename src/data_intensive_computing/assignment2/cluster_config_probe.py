from __future__ import annotations

import argparse
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Probe Spark/YARN scheduling knobs without running the Part 3 ML workload. "
            "Use this on the cluster to see which spark-submit configs are honored and "
            "whether multiple driver-submitted jobs can be active concurrently."
        )
    )
    parser.add_argument("--jobs", type=int, default=12)
    parser.add_argument("--partitions-per-job", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=60.0)
    parser.add_argument("--pool-threads", type=int, default=12)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--log-level", default="WARN")
    return parser


def make_spark(args):
    builder = SparkSession.builder.appName("Assignment2-ClusterConfigProbe")
    if args.local:
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def print_conf(spark):
    sc = spark.sparkContext
    keys = [
        "spark.master",
        "spark.submit.deployMode",
        "spark.app.id",
        "spark.app.name",
        "spark.driver.host",
        "spark.driver.port",
        "spark.driver.cores",
        "spark.yarn.am.cores",
        "spark.driver.memory",
        "spark.executor.instances",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.task.cpus",
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.scheduler.mode",
        "spark.dynamicAllocation.enabled",
        "spark.yarn.executor.resource.vcores",
    ]

    print("=== effective SparkConf ===", flush=True)
    conf = dict(sc.getConf().getAll())
    for key in keys:
        print(f"{key}={conf.get(key, '<unset>')}", flush=True)
    print(f"sc.defaultParallelism={sc.defaultParallelism}", flush=True)
    print(f"python_driver_host={socket.gethostname()}", flush=True)


def print_executor_snapshot(spark):
    sc = spark.sparkContext
    print("=== executor memory status ===", flush=True)
    try:
        status = sc._jsc.sc().getExecutorMemoryStatus().toSeq()
        for idx in range(status.size()):
            item = status.apply(idx)
            host_port = item._1()
            max_mem, free_mem = item._2()._1(), item._2()._2()
            print(
                f"executor={host_port} maxMem={max_mem} freeMem={free_mem}",
                flush=True,
            )
    except Exception as exc:
        print(f"executor memory status unavailable: {type(exc).__name__}: {exc}", flush=True)


def sleeper_partition(job_id, sleep_seconds, iterator):
    host = socket.gethostname()
    print(f"task_start job={job_id} host={host} sleep={sleep_seconds}", flush=True)
    time.sleep(sleep_seconds)
    count = sum(1 for _ in iterator)
    print(f"task_done job={job_id} host={host} count={count}", flush=True)
    yield (job_id, host, count)


def run_probe_job(sc, job_id: int, partitions: int, sleep_seconds: float):
    sc.setJobGroup(
        f"probe-{job_id}",
        f"probe job {job_id}: {partitions} partitions sleep {sleep_seconds}s",
        interruptOnCancel=True,
    )
    # Keep data tiny; the sleep is the workload. Multiple submitted jobs should be
    # visible in the Spark UI/YARN tracking page while these tasks are sleeping.
    rdd = sc.parallelize(range(partitions), partitions)
    t0 = time.time()
    result = rdd.mapPartitions(
        lambda it: sleeper_partition(job_id, sleep_seconds, it)
    ).collect()
    elapsed = time.time() - t0
    sc.setJobGroup("", "")
    return job_id, elapsed, result


def main():
    args = build_parser().parse_args()
    spark = make_spark(args)
    sc = spark.sparkContext
    try:
        print_conf(spark)
        print_executor_snapshot(spark)
        print(
            "=== launching probe jobs ===\n"
            f"jobs={args.jobs} pool_threads={args.pool_threads} "
            f"partitions_per_job={args.partitions_per_job} sleep_seconds={args.sleep_seconds}",
            flush=True,
        )

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=args.pool_threads) as pool:
            futures = {
                pool.submit(
                    run_probe_job,
                    sc,
                    job_id,
                    args.partitions_per_job,
                    args.sleep_seconds,
                ): job_id
                for job_id in range(1, args.jobs + 1)
            }
            for done, fut in enumerate(as_completed(futures), start=1):
                job_id, elapsed, result = fut.result()
                print(
                    f"probe_finished {done}/{args.jobs} job={job_id} "
                    f"elapsed={elapsed:.1f}s result={result}",
                    flush=True,
                )

        print(f"probe_total_seconds={time.time() - t0:.1f}", flush=True)
        print_executor_snapshot(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
