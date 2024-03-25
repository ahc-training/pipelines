from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import timedelta, datetime
from kubernetes.client import models as k8s
from dataclasses import dataclass
from typing import List
import pendulum
import os
import urllib

default_args = {
    'start_date': pendulum.today('UTC').add(days=-1),
    'depends_on_past': False,
    'retries': 0
}

commands = ["build", "test", "run"]

def create_operator(command):
    dbt_project_path = "/usr/app/git-sync/dbt"
    return KubernetesPodOperator(
            task_id=f'dbt-{command}',
            name=f'dbt-{command}',
            namespace="dbt-jobs",
            pod_template_file="/opt/airflow/dags/repo/dags/dbt.yml",
            labels={"dbt": command},
            image_pull_policy="Always",
            image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
            is_delete_operator_pod=True,
            get_logs=True,
            log_events_on_failure=True,
            in_cluster=True,
            cmds=["dbt", command],
            arguments=["--project-dir", dbt_project_path, "--profiles-dir", dbt_project_path],
            env_vars={
                "PGSQL_HOST": "postgresql.example.com"
            },
            do_xcom_push=True
        )

@dag(dag_id='dbt_core', default_args=default_args, tags=["dbt"], catchup=False, schedule="15 1 * * *")
def dbt_core():
    operators = [create_operator(cmd) for cmd in commands]
    chain(*operators)

dbt_core()