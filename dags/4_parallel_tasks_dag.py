from airflow.sdk import dag,task

@dag(
        dag_id="parallel_dag",
             )
def parallel_dag():
    @task.python
    def extract_data(**kwargs):
        #ti-task instance
        ti=kwargs['ti']
        fetched_data={"api_data":[1,2,3],
                      "web_data":[4,5,6],
                      "s3_data":[7,8,9]}
        ti.xcom_push(key="return_value",value=fetched_data)
        
    @task.python
    def transform_web_data(**kwargs):
        ti=kwargs['ti']
        web_data=ti.xcom_pull(task_ids="extract_data")['web_data']
        transformed_data=[ i*10 for  i in web_data]
        ti.xcom_push(key="return_value",value=transformed_data)

    @task.python
    def transform_api_data(**kwargs):
        ti=kwargs['ti']
        api_data=ti.xcom_pull(task_ids="extract_data")['api_data']
        transformed_data=[ i*10 for  i in api_data]
        ti.xcom_push(key="return_value",value=transformed_data)

    @task.python
    def transform_s3_data(**kwargs):
        ti=kwargs['ti']
        s3_data=ti.xcom_pull(task_ids="extract_data")['s3_data']
        transformed_data=[ i*10 for  i in s3_data]
        ti.xcom_push(key="return_value",value=transformed_data)
    @task.bash
    def load_data(**kwargs):
        api_data=kwargs['ti'].xcom_pull(task_ids="transform_api_data")#,key="result_value")['transformed_data']
        web_data=kwargs['ti'].xcom_pull(task_ids="transform_web_data")#,key="result_value")['transformed_data']
        s3_data=kwargs['ti'].xcom_pull(task_ids="transform_s3_data")#,key="result_value")['transformed_data']
        return f"echo 'Loaded_data : {api_data},{web_data},{s3_data}'"


  

    first=extract_data()
    second=transform_api_data()
    third=transform_web_data()
    fourth=transform_s3_data()
    fifth=load_data()
    
    first>>[second,third,fourth]>>fifth

parallel_dag()