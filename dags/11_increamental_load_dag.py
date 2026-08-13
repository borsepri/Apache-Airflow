from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
@dag(
        dag_id="increamental_load_dag",
        schedule=CronDataIntervalTimetable("@hourly",timezone="America/Halifax"),
        start_date=datetime(year=2026,month=8,day=1,tz="America/Halifax"),
        catchup=True   
)
def increamental_load_dag():
    @task.bash
    def inc_load_bash():
       return "echo 'increamental load for interval {{data_interval_start}} to {{data_interval_end}}'"

    first=inc_load_bash()
    first
    
increamental_load_dag()