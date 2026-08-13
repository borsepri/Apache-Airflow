from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable
@dag(
        dag_id="scheduled_cron_dag",
        start_date=datetime(year=2026,month=8,day=4,tz="America/Halifax"),
        schedule=CronTriggerTimetable("0 16 * * MON-FRI",timezone="America/Halifax"),
        is_paused_upon_creation=False,
        catchup=True
    )
def scheduled_cron_dag():
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

scheduled_cron_dag()



