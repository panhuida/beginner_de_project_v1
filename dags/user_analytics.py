import os
import shutil
import time
from datetime import datetime, timedelta

import boto3
import duckdb
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def get_s3_folder(s3_bucket, s3_folder, local_folder="/opt/code/dp/beginner_de_project_local/temp/s3folder/"):
    # TODO: Move AWS credentials to env variables
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="QNbVk0LrGCOUNviswayw",
        aws_secret_access_key="nLmDKpMnCunZn9UAqf1nSDL279GfA5DaPKduR8st",
        region_name="us-east-1"
    )
    bucket = s3.Bucket(s3_bucket)
    local_path = os.path.join(local_folder, s3_folder)
    # Delete the local folder if it exists
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")


with DAG(
    "user_analytics_dag",
    default_args={
        "depends_on_past": False,
        "email": ["panhuida@qq.com"]
    },
    description="A DAG to Pull user data and movie review data \
        to analyze their behaviour",
    # schedule_interval=timedelta(days=1),
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    user_analytics_bucket = "user-analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=user_analytics_bucket
    )
    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="/opt/code/dp/beginner_de_project_local/data/movie_review.csv",
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

    # movie_classifier = BashOperator(
    #     task_id="movie_classifier",
    #     bash_command="python /opt/code/dp/beginner_de_project_local/dags/scripts/spark/random_text_classification.py",
    # )

    # 在向minio写数据时会卡住
    movie_classifier = SparkSubmitOperator(
        task_id="movie_classifier",
        conn_id="spark-conn",
        application="/opt/code/dp/beginner_de_project_local/dags/scripts/spark/random_text_classification.py",
        name="random_text_classification",
        # packages="org.apache.hadoop:hadoop-aws:3.3.4",
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
        time.sleep(30)
        q = """
        with up as (
          select 
            * 
          from 
            '/opt/code/dp/beginner_de_project_local/temp/s3folder/raw/user_purchase/user_purchase.csv'
        ), 
        mr as (
          select 
            * 
          from 
            '/opt/code/dp/beginner_de_project_local/temp/s3folder/clean/movie_review/*.parquet'
        ) 
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
        """
        duckdb.sql(q).write_csv("/opt/code/dp/beginner_de_project_local/data/behaviour_metrics.csv")

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=create_user_behaviour_metric,
    )

    markdown_path = "/opt/code/dp/beginner_de_project_local/dags/scripts/dashboard"
    q_cmd = f"cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd"
    gen_dashboard = BashOperator(task_id="generate_dashboard", bash_command=q_cmd)

    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    user_purchase_to_s3 >> get_user_purchase_to_warehouse

    movie_review_to_s3 >> movie_classifier >> get_movie_review_to_warehouse

    (
        [get_user_purchase_to_warehouse, get_movie_review_to_warehouse]
        >> get_user_behaviour_metric
        >> gen_dashboard
    )
