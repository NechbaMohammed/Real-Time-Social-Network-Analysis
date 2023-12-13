from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta




default_args = {
    'owner': 'NECHBA',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 12,14,15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'sentiment-analysis',
    default_args=default_args,
    description='Airflow pipeline for the tasks',
    schedule_interval=timedelta(days=1),  # Set the schedule interval as per your requirements
)



# Define operators for each task in the DAG
produce_reddit_operator = BashOperator(
    task_id='produce_reddit',
    bash_command="python ProducerRedit.py",
    dag=dag,
)

data_preprocessing_operator = BashOperator(
    task_id='data_preprocessing',
    bash_command="python DataPrep.py",
    dag=dag,
)

model_prediction_operator = BashOperator(
    task_id='model_prediction',
    bash_command="python Model_Prediction.py",
    dag=dag,
)

mongo_to_elasticsearch_operator = BashOperator(
    task_id='mongo_to_elasticsearch',
    bash_command="python MongoToElastic.py",
    dag=dag,
)

# Define the task dependencies
produce_reddit_operator >> data_preprocessing_operator >> model_prediction_operator >> mongo_to_elasticsearch_operator
