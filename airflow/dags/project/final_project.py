import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

# 1. Define default arguments as requested by the project rubric
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *', # Run hourly
    catchup=False,
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # 2. Staging tasks (S3 -> Redshift)
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rayan-airflow-2026", # Ensure this matches your bucket name
        s3_key="log_data",
        json_path="s3://rayan-airflow-2026/log_json_path.json" # Use JSON path file for logs
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rayan-airflow-2026", # Ensure this matches your bucket name
        s3_key="song_data",
        json_path="auto"
    )

    # 3. Load Fact Table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert
    )

    # 4. Load Dimension Tables (with 'truncate' mode)
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        mode="truncate"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        mode="truncate"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        mode="truncate"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        mode="truncate"
    )

    # 5. Data Quality Checks
    # Pass a list of SQL checks and expected results
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        dq_checks=[
            # Check 1: Ensure songid is never NULL in songs table
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
            # Check 2: Ensure userid is never NULL in users table
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
            # Check 3: Ensure songplays table has data (returns 1 if count > 0)
            {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM songplays", 'expected_result': 1}
        ]
    )
    
    end_operator = DummyOperator(task_id='Stop_execution')

    # 6. Define Task Dependencies
    # Start -> Stage
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    
    # Stage -> Fact
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    
    # Fact -> Dimensions
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    
    # Dimensions -> Quality Checks
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    
    # Quality Checks -> End
    run_quality_checks >> end_operator

final_project_dag = final_project()