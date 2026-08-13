from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator
@dag(
        dag_id="operator_dag",
        )
def operator_dag():
    @task.bash
    def bash_task_modern():
        return "echo hello I am modern bash task "
    
    bash_task_older = BashOperator(
        task_id="bash_task_older",
        bash_command="echo Hello I am old school bash task",
    )

#set dependencies
    new=bash_task_modern()
    bash_task_older>>new
#instantiate dag
operator_dag()




