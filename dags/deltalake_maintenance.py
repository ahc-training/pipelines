from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import timedelta, datetime
from kubernetes.client import models as k8s
import pendulum
import os
import urllib

class Configuration:
    def __init__(self):
        self.s3_endpoint: str = Variable.get('S3_ENDPOINT')
        self.s3_access_key: str = Variable.get('S3_ACCESS_KEY')
        self.s3_secret_key: str = Variable.get('S3_SECRET_KEY')
        self.spark_namespace: str = Variable.get('SPARK_NAMESPACE')
        self.spark_image: str = Variable.get('SPARK_IMAGE')
        self.spark_deltalake_maintenance: str = Variable.get('DELTALAKE_MAINTENANCE')

config = Configuration()

paths = config.spark_deltalake_maintenance.split(",")

default_args = {
    'start_date': pendulum.today('UTC').add(days=-1),
    'depends_on_past': False,
    'retries': 0
}

def create_operator(path):
    return KubernetesPodOperator(
            task_id=path.replace("s3a://", "").replace("/", "-"),
            name=path.replace("s3a://", "").replace("/", "-"),
            namespace=config.spark_namespace,
            pod_template_file="/opt/airflow/dags/repo/dags/spark-py.yml",
            labels={"spark-app": "deltalake-maintenance"},
            image_pull_policy="Always",
            image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
            is_delete_operator_pod=True,
            get_logs=True,
            log_events_on_failure=True,
            in_cluster=True,
            do_xcom_push=False,
            arguments=[
                "/opt/spark/bin/spark-submit",
                "--master", "k8s://kubernetes.default.svc:443",
                "--deploy-mode", "cluster",
                "--name", "deltalake_maintenance",
                "--conf", f"spark.kubernetes.container.image={config.spark_image}",
                "--conf", "spark.kubernetes.container.image.pullPolicy=Always",
                "--conf", "spark.kubernetes.container.image.pullSecrets=regcred",
                "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "--conf", "spark.sql.debug.maxToStringFields=100",
                "--conf", "spark.executorEnv.com.amazonaws.sdk.disableCertChecking=true",
                "--conf", f"spark.kubernetes.namespace={config.spark_namespace}",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
                "--conf", "spark.kubernetes.authenticate.executor.serviceAccountName=spark",
                "--conf", "spark.executor.instances=1",
                "--conf", f"spark.hadoop.fs.s3a.endpoint={config.s3_endpoint}",
                "--conf", f"spark.hadoop.fs.s3a.secret.key={config.s3_secret_key}",
                "--conf", f"spark.hadoop.fs.s3a.access.key={config.s3_access_key}",
                "--conf", "spark.hadoop.fs.s3a.connection.timeout=50000",
                "--conf", "spark.hadoop.fs.s3a.connection.maximum=3",
                "--conf", "spark.hadoop.fs.s3a.attempts.maximum=3",
                "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
                "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
                "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
                "--conf", "spark.kubernetes.file.upload.path=s3a://weatherforecast/pyspark",
                "--conf", "spark.databricks.delta.properties.defaults.columnMapping.mode=name",
                "--conf", f"spark.deltalake.maintenance={path}",
                "/app/git-sync/spark/deltalake_maintenance.py"]
        )

@dag(dag_id='deltalake_maintenance', schedule="30 1 * * 0", default_args=default_args, catchup=False, tags=["maintenance"])
def deltalake_maintenance():
    operators = [create_operator(path) for path in paths]
    chain(*operators)

deltalake_maintenance()