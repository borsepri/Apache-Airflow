from airflow.sdk import dag, task

@dag(
        dag_id="versioned_dag",
        )
def versioned_dag():

    @task.python
    def task1():
        print("this is task1")

    @task.python
    def task2():
        print("this is task2")
    @task.python
    def task3():
        print("this is task3")
    @task.python
    def task_versioned():
        print("this is versioned task")

#define task dependency
    first=task1()
    second=task2()
    third=task3()
    versioned=task_versioned()
    

    first>>second>>third>>versioned
#instantiate the DAG
versioned_dag()