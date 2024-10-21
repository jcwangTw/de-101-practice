from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession, functions
import os
import requests
import zipfile


# 定义下载函数
def download_multiple_files(save_path):
    base_url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/"
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']

    # 检查并创建保存目录
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for year in range(2019, 2024):
        for quarter in quarters:
            if year == 2023 and quarter == 'Q4':
                break  # 跳过2023 Q4
            zip_filename = f"data_{quarter}_{year}.zip"
            url = base_url + zip_filename
            file_path = os.path.join(save_path, zip_filename)

            # 下载文件
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded {zip_filename} to {file_path}")

                # 解压文件
                if zipfile.is_zipfile(file_path):
                    if year == 2022 or year == 2023:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            zip_ref.extractall(save_path)
                            print(f"Extracted {zip_filename} to {save_path}")
                    else:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            extract_path = os.path.join(save_path, zip_filename.replace('.zip', ''))  # 解压到子目录
                            zip_ref.extractall(extract_path)
                            print(f"Extracted {zip_filename} to {extract_path}")
                else:
                    print(f"{zip_filename} is not a valid zip file.")
            else:
                raise Exception(f"Failed to download {url}. Status code: {response.status_code}")


# 定义 Spark 分析函数
def spark_analyze_disk_failures(input_dir, output_dir):
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("Airflow Backblaze Disk Failure Analysis") \
        .getOrCreate()

    # 创建输出目录
    daily_output_path = output_dir + '/daily'
    yearly_output_path = output_dir + '/yearly'
    data_lake_path = input_dir

    os.makedirs(daily_output_path, exist_ok=True)
    os.makedirs(yearly_output_path, exist_ok=True)

    # 定义品牌名称和前缀的映射
    brand_prefixes = {
        "CT": "Crucial",
        "DELLBOSS": "Dell BOSS",
        "HGST": "HGST",
        "Seagate":"Seagate",
        "ST":"Seagate",
        "TOSHIBA": "Toshiba",
        "WDC": "Western Digital",
    }

    # 提取品牌名称
    def extract_brand(model):
        for prefix, brand in brand_prefixes.items():
            if model.startswith(prefix):
                return brand
        return "Others"

    extract_brand_udf = functions.udf(extract_brand)

    # 读取数据并合并所有CSV文件
    df = spark.read.option("header", "true").csv(data_lake_path + "/data_*" + "/*.csv")

    # 添加品牌
    df = df.withColumn("brand", extract_brand_udf(df["model"]))

    daily_summary = df.groupBy("date", "brand").agg(
        functions.count("*").alias("count"),
        functions.sum(df["failure"].cast("int")).alias("failures")
    )

    # 每日汇总结果写入CSV
    for brand in daily_summary.select("brand").distinct().rdd.flatMap(lambda x: x).collect():
        brand_summary = daily_summary.filter(daily_summary["brand"] == brand)
        brand_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            os.path.join(daily_output_path, f"{brand}"))

    df = df.withColumn("year", functions.year("date"))
    yearly_summary = df.groupBy("year", "brand").agg(
        functions.sum(df["failure"].cast("int")).alias("failures")
    )

    # 年度汇总结果写入CSV
    for brand in yearly_summary.select("brand").distinct().rdd.flatMap(lambda x: x).collect():
        brand_yearly_summary = yearly_summary.filter(yearly_summary["brand"] == brand)
        brand_yearly_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            os.path.join(yearly_output_path, f"{brand}"))

    spark.stop()


# 默认的 DAG 参数
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# 获取 DAG 文件的当前目录，并指定保存文件的子目录
dag_file_path = os.path.dirname(os.path.abspath(__file__))
download_directory = os.path.join(dag_file_path, 'data')  # 保存到 dag 子目录 'downloaded_files'
output_directory = os.path.join(dag_file_path, 'out')  # 保存分析结果的目录

# 创建 DAG
with DAG(
        dag_id='tuto',
        default_args=default_args,
        start_date=datetime(2023, 10, 17),
        schedule_interval='@daily',
        catchup=False
) as dag:
    download_task = PythonOperator(
        task_id='download_multiple_files_task',
        python_callable=download_multiple_files,
        op_kwargs={
            'save_path': download_directory  # 保存文件的目录
        },
    )

    # Step 2: 使用 Spark 进行分析并输出结果
    spark_analysis_task = PythonOperator(
        task_id='spark_analyze_disk_failures_task',
        python_callable=spark_analyze_disk_failures,
        op_kwargs={
            'input_dir': download_directory,
            'output_dir': output_directory  # 结果输出目录
        },
        dag=dag,
    )

    # 定义任务依赖关系
    download_task >> spark_analysis_task
