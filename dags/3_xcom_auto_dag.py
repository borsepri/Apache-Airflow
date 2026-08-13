from airflow.sdk import dag, task

@dag(
        dag_id="xcom_dag",
        )
def xcom_dag():

    @task.python
    def Extract():
        fetched_data={"data":[1,2,3,4,5]}
        print(f"original data={fetched_data['data']}")
        return fetched_data

    @task.python
    def Transform(data:dict):
        #transformed_data=data['data']*2
        transformed_list=data['data']
        result=list(map(lambda x: x*2 ,transformed_list))
        transformed_data_dict={"data":result}
        return transformed_data_dict

    @task.python
    def Load(data:dict):
        load_data=data
        return load_data

    first=Extract()
    second=Transform(first)
    third=Load(second)

xcom_dag()