import os
import shutil
import time
from datetime import datetime, timedelta
import boto3
import logging
import duckdb
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logging.basicConfig(filename='./logs/user_analytics.log', level=logging.INFO)

def get_s3_folder(
    s3_bucket,
    s3_folder,
    local_folder="./temp/s3folder/"
):
    # 创建S3资源
    s3 = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000")
    bucket = s3.Bucket(s3_bucket)

    local_path = os.path.join(local_folder, s3_folder)

    if os.path.exists(local_path):
        logging.info(f"Deleting existing folder: {local_path}")
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        # 相对路径
        rel_path = os.path.relpath(obj.key, s3_folder)
        target = os.path.join(local_path, rel_path)

        os.makedirs(os.path.dirname(target), exist_ok=True)
        try:
            bucket.download_file(obj.key, target)
            logging.info(f"Downloaded {obj.key} to {target}")
        except Exception as e:
            logging.error(f"Erro r {obj.key}: {str(e)}")


with DAG(
    "user_analytics_dag_v1",
    default_args={
        "depends_on_past": False,
        "email": ["panhuida@qq.com"]
    },
    description="A DAG to Pull user data and movie review data \
        to analyze their behaviour",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 4, 10, 19, 31),
    catchup=False,
) as dag:
    user_analytics_bucket = "user-analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=user_analytics_bucket
    )

    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="./data/movie_review.csv",
        dest_key="raw/movie_review.csv",
        dest_bucket=user_analytics_bucket,
        replace=True,
    )

    user_purchase_to_s3 = SqlToS3Operator(
        task_id="user_purchase_to_s3",
        sql_conn_id="postgres_default",
        query="select * from retail.user_purchase",
        s3_bucket=user_analytics_bucket,
        s3_key="raw/user_purchase/user_purchase.csv",
        replace=True,
    )

    # PySpark自带的spark环境运行
    # movie_classifier = BashOperator(
    #     task_id="movie_classifier",
    #     bash_command="python /opt/code/dp/beginner_de_project_local/dags/scripts/spark/random_text_classification.py",
    # )

    # 部署的spark环境运行
    movie_classifier = SparkSubmitOperator(
        task_id="movie_classifier",
        conn_id="spark-conn",
        application="./dags/scripts/spark/random_text_classification.py",
        name="random_text_classification",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        # env_vars={'PYSPARK_PYTHON': '/opt/airflow/bin/python'},
        verbose=True,
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": "user-analytics", "s3_folder": "clean/movie_review"},
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": "user-analytics", "s3_folder": "raw/user_purchase"},
    )

    def create_user_behaviour_metric():
        # time.sleep(30)
        con = duckdb.connect("/data/duckdb/demo.duckdb")
        q = """
        with up as (
          select 
            * 
          from 
            './temp/s3folder/raw/user_purchase/user_purchase.csv'
        ), 
        mr as (
          select 
            * 
          from 
            './temp/s3folder/clean/movie_review/*.parquet'
        )
        insert into behaviour_metrics
        select 
          up.customer_id, 
          sum(up.quantity * up.unit_price) as amount_spent, 
          sum(
            case when mr.positive_review then 1 else 0 end
          ) as num_positive_reviews, 
          count(mr.cid) as num_reviews 
        from 
          up 
          join mr on up.customer_id = mr.cid 
        group by 
          up.customer_id
        ON CONFLICT DO NOTHING;
        """
        # duckdb.sql(q).write_csv("/opt/code/dp/beginner_de_project_local/data/behaviour_metrics.csv")
        con.sql(q)
        con.close()

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=create_user_behaviour_metric,
    )

    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    user_purchase_to_s3 >> get_user_purchase_to_warehouse

    movie_review_to_s3 >> movie_classifier >> get_movie_review_to_warehouse

    (
        [get_user_purchase_to_warehouse, get_movie_review_to_warehouse]
        >> get_user_behaviour_metric
    )
