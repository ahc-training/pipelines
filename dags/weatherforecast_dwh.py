from airflow.decorators import dag
from airflow.models import Variable
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
        self.postgresql_url: str = Variable.get('PGSQL_URL')
        self.postgresql_usr: str = Variable.get('PGSQL_USR')
        self.postgresql_pwd: str = Variable.get('PGSQL_PWD')
        self.spark_namespace: str = Variable.get('SPARK_NAMESPACE')
        self.spark_image: str = Variable.get('SPARK_IMAGE')

config = Configuration()


spark_setup = [
    "/opt/spark/bin/spark-submit",
    "--master", "k8s://kubernetes.default.svc:443",
    "--deploy-mode", "cluster",
    "--name", "weatherforecast-data",
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
    "--conf", "spark.hadoop.fs.s3a.connection.timeout=60",
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
    "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
    "--conf", "spark.kubernetes.file.upload.path=s3a://weatherforecast/pyspark"
]


@dag(start_date=pendulum.today('UTC').add(days=-1), tags=["weather"], catchup=False, schedule="0 * * * *")
def weatherforecast_dwh():

    
    prep_data = KubernetesPodOperator(
        task_id=f'prepare_weatherforecast',
        name='prepare_weatherforecast',
        namespace=config.spark_namespace,
        pod_template_file="/opt/airflow/dags/repo/dags/spark-py.yml",
        labels={"spark-app": "weatherforecast-dwh"},
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
        do_xcom_push=True,
        arguments=spark_setup + ["/app/git-sync/spark/weatherforecast_dwh_prepare.py"]
    )

    load_data = KubernetesPodOperator(
        task_id=f'load_weatherforecast',
        name='load_weatherforecast',
        namespace=config.spark_namespace,
        pod_template_file="/opt/airflow/dags/repo/dags/spark-py.yml",
        labels={"spark-app": "weatherforecast-dwh"},
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
        do_xcom_push=True,
        arguments=spark_setup + [
            "--conf", f"spark.postgresql_url={config.postgresql_url}",
            "--conf", f"spark.postgresql_usr={config.postgresql_usr}",
            "--conf", f"spark.postgresql_pwd={config.postgresql_pwd}",
            "/app/git-sync/spark/weatherforecast_dwh_load.py"
        ]
    )

    prep_data >> load_data

weatherforecast_dwh()