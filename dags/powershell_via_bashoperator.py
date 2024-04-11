from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime

@dag(start_date=datetime(2022, 8, 1), schedule_interval=None, catchup=False)
def powershell_script_example_dag():
    # Note the space at the end of the command and the use of 'pwsh' to invoke PowerShell
    run_powershell_script = BashOperator(
        task_id="run_powershell_script",
        bash_command="pwsh /path/to/your/script.ps1 ",
    )

    run_powershell_script

dag = powershell_script_example_dag()