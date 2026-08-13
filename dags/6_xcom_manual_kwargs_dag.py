from airflow.sdk import dag,task
@dag(
        dag_id="xcom_manual_with_kwargs",
     )
def xcom_manual_with_kwargs():
    @task.python
    def extract(**kwargs):
        ti=kwargs['ti']
        fetched_data={"data":[1,2,3,4,5]}
        ti.xcom_push(key='return_result',value=fetched_data)
        
    @task.python
    def transform(**kwargs):
        ti=kwargs['ti']
        fetched_data=ti.xcom_pull(task_ids='extract',key='return_result')
        transformed_data=list(map(lambda x:x*2,fetched_data['data']))
        transformed_data_dict={"data":transformed_data}
        ti.xcom_push(key='return_result',value=transformed_data_dict)

    @task.python
    def load(**kwargs):
        ti=kwargs['ti']
        load_data=ti.xcom_pull(task_ids='transform',key='return_result')['data']
        ti.xcom_push(key="return_result",value=load_data)
    #set dependencies
    first=extract()
    second=transform()
    third=load()
    first >> second >> third
#instantiate dag
xcom_manual_with_kwargs()


    



