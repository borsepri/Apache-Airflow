from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable
special_dates=EventsTimetable(event_dates=[
    datetime(2026,1,26),
    datetime(2026,4,4),
    datetime(2026,8,15)
    ])

@dag(
    dag_id="special_events_dag",
    schedule=special_dates,
    start_date=datetime(2026,1,1,tz="America/Halifax"),
    end_date=datetime(2030,1,31,tz="America/Halifax"),
    catchup=True
)
def special_events_dag():
    @task.python
    def special_event_task(**kwargs):
        execution_date=kwargs['logical_date']
        print(f"Running task on special events {execution_date}")

    special_event=special_event_task()
    special_event

special_events_dag()