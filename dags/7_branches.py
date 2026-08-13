from airflow.sdk import dag,task

@dag(
        dag_id="branch_dag",
             )
def branch_dag():
    @task.python
    def extract_data(**kwargs):
        #ti-task instance
        ti=kwargs['ti']
        fetched_data={"api_data":[1,2,3],
                      "web_data":[4,5,6],
                      "s3_data":[7,8,9],
                      "weekend_flag":"false"}
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

    @task.branch
    def decider_task(**kwargs):
        ti=kwargs['ti']
        weekend_flag=ti.xcom_pull(task_ids="extract_data")['weekend_flag']
        if weekend_flag=="true":
            return 'no_load_task'
        else:
            return 'load_data'
        
    @task.bash
    def load_data(**kwargs):
        api_data=kwargs['ti'].xcom_pull(task_ids="transform_api_data")#,key="result_value")['transformed_data']
        web_data=kwargs['ti'].xcom_pull(task_ids="transform_web_data")#,key="result_value")['transformed_data']
        s3_data=kwargs['ti'].xcom_pull(task_ids="transform_s3_data")#,key="result_value")['transformed_data']
        return f"echo 'Loaded_data : {api_data},{web_data},{s3_data}'"  
    @task.bash
    def no_load_task(**kwargs):
        print("no loading on weekend")
        return "echo 'No load task is executed'"

    first=extract_data()
    second=transform_api_data()
    third=transform_web_data()
    fourth=transform_s3_data()
    fifth=load_data()
    sixth=no_load_task()
    
    first>>[second,third,fourth]>>decider_task()>>[fifth,sixth]

branch_dag()