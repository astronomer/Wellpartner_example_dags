from datetime import datetime
from airflow import DAG
from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator

# In this example, we have three tasks defined:
# 1. invoke_cmdlet - This task uses the cmdlet argument to invoke a PowerShell cmdlet (Get-Service).
# 2. execute_command - This task uses the command argument to execute a command using the cmd command interpreter (robocopy).
# 3. run_powershell_script - This task uses the powershell argument to run a PowerShell script that copies files from one directory to another.
# Please replace the cmdlet, command, and powershell arguments with the actual commands you wish to execute on the remote Windows server. Also, ensure that you have the necessary connection details set up in Airflow to connect to the remote server using PSRP.

# These args will get passed on to the PsrpOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 1),
}

dag = DAG(
    'example_psrp_operator',
    default_args=default_args,
    description='A simple tutorial DAG to use PsrpOperator',
    schedule_interval=None,
    catchup=False,
)

# Define a task to invoke a PowerShell cmdlet
invoke_cmdlet = PsrpOperator(
    task_id='invoke_cmdlet',
    cmdlet='Get-Service',
    # Additional parameters for the cmdlet can be provided as a list of strings
    # cmdlet_params=['-Name', 'WinRM'],
    dag=dag,
    psrp_conn_id='psrp',
)

# Define a task to execute a command using the cmd command interpreter
execute_command = PsrpOperator(
    task_id='execute_command',
    command='robocopy C:\\Logfiles\\* C:\\Drawings /S /E',
    dag=dag,
    psrp_conn_id='psrp',
)

# Define a task to run a PowerShell script
run_powershell_script = PsrpOperator(
    task_id='run_powershell_script',
    powershell='Copy-Item -Path "C:\\Logfiles\\*" -Destination "C:\\Drawings" -Recurse',
    dag=dag,
    psrp_conn_id='psrp',
)

# Set up the task dependencies
invoke_cmdlet >> execute_command >> run_powershell_script