from airflow.sdk import dag, task

@dag(
        dag_id="first_dag",
        )
def first_dag():

    @task.python
    def task1():
        print("this is task1")

    @task.python
    def task2():
        print("this is task2")
    @task.python
    def task3():
        print("this is task3")

#define task dependency
    first=task1()
    second=task2()
    third=task3()

    first>>second>>third
#instantiate the DAG
first_dag()