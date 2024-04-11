from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import EmptyOperator
from pendulum import datetime


def branch_function(**kwargs):
    # Retrieve the result from the SSH task
    ssh_result = kwargs['ti'].xcom_pull(task_ids='ssh_task')

    # Decide which path to take based on the SSH task result
    if "success" in ssh_result.lower():
        return 'success_task'
    else:
        return 'failure_task'


with DAG(
        'ssh_and_branch_operator_example',
        default_args={
            'start_date': datetime(2023, 1, 1),
        },
        schedule_interval="@daily",
        catchup=False,
) as dag:
    start_task = EmptyOperator(
        task_id='start_task'
    )

    ssh_task = SSHOperator(
        task_id='ssh_task',
        ssh_conn_id='ssh_default',
        command='echo "Running remote command"',
        do_xcom_push=True
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_function,
        provide_context=True
    )

    success_task = EmptyOperator(
        task_id='success_task'
    )

    failure_task = EmptyOperator(
        task_id='failure_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> ssh_task >> branch_task
    branch_task >> [success_task, failure_task] >> end_task