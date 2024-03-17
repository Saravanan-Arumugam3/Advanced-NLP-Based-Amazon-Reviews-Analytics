from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow.operators.email_operator import EmailOperator
from src.Convert_json_csv import process_files
#from src.Bucket import list_buckets
#from src.Bucket import upload_files_in_directory_to_gcs
from src.Merge_Files import merge_files
from src.Missing_Values import download_and_clean_gcs_data
#from src.tfdv import run_tfdv_workflow
from airflow import configuration as conf

default_args={
    'owner': 'group_1',
    'start_date':datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def notify_success(context):
    success_email=EmailOperator(
        task_id='success_email',
        to='aturiphanibhavana@gmail.com',
        subject='DAG success run from Airflow',
        html_content='<p>The task succeeded.</p>',
        dag=context['dag']
    )
    success_email.execute(context=context)

def notify_failure(context):
    failure_email=EmailOperator(
        task_id='failure_email',
        to='aturiphanibhavana@gmail.com',
        subject='DAG failure run from Airflow',
        html_content='<p>The task failed.</p>',
        dag=context['dag']
    )
    failure_email.execute(context=context)

dag = DAG(
    'datapipeline',
    default_args=default_args,
    description='Airflow DAG for data pipeline',
    schedule_interval=None,
    catchup=False

)

send_email = EmailOperator(
    task_id='send_email',
    to='atluriphanibhavana@gmail.com',    # Email address of the recipient
    subject='Notification from Airflow',
    html_content="<p>This is a notification email sent from Airflow.</p>",
    dag=dag,
    on_failure_callback=notify_failure,
    on_success_callback=notify_success
)

task2 = PythonOperator(
    task_id='convert_json_to_csv',
    python_callable=process_files,
    dag=dag,
)

task5 = PythonOperator(
    task_id='merge_csv',
    python_callable=merge_files,
    dag=dag,
)

task6 = PythonOperator(
    task_id='clean_and_preprocess',
    python_callable=download_and_clean_gcs_data,
    dag=dag,
)

# task_7 = PythonOperator(
#     task_id='run_tfdv_workflow_task',
#     python_callable=run_tfdv_workflow,  # This function needs to be defined in your environment
#     op_kwargs={
#         'schema_output_path': '/output_schema.pbtxt',  # Update this to your actual GCS path
#     },
#     dag=dag,
# )


# Define the task to send an email based on the TFDV workflow's output
# send_email_task_2 = EmailOperator(
#     task_id='send_email_task_2',
#     to='atluriphanibhavana@gmail.com',
#     subject='TFDV Workflow Details',
#     html_content="{{ task_instance.xcom_pull(task_ids='run_tfdv_workflow_task', key='tfdv_details') }}",
#     dag=dag,
# )

# Set task dependencies
task2 >> task5 >> task6 >> send_email

