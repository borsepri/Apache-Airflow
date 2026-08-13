from airflow.sdk import dag,task
from pendulum import datetime,duration
from airflow.timetables.trigger import CronTriggerTimetable,DeltaTriggerTimetable
@dag(
        dag_id="scheduled_delta_dag",
        start_date=datetime(year=2026,month=8,day=4,tz="America/Halifax"),
        schedule=DeltaTriggerTimetable(duration(days=3)),
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



