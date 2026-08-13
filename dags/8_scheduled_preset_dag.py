from airflow.sdk import dag,task
from pendulum import datetime
@dag(
        dag_id="first_scheduled_dag",
        start_date=datetime(year=2026,month=8,day=4,tz="America/Halifax"),
        schedule="@daily",
        is_paused_upon_creation=False,
        catchup=True
    )
def first_scheduled_dag():
    @task.python
    def first_task():
        print ("This is First Task")
    @task.python
    def second_task():
        print("This is Second Task")
    @task.python
    def third_task():
        print("this is third task")

    first_task()>>second_task()>>third_task()

first_scheduled_dag()



