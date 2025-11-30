from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 25),
    'retries': 1,
}

with DAG(
    'real_estate_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # ==================================================================
    # PHASE 1: PREPARATION & SCRAPING
    # ==================================================================

    create_local_dirs = BashOperator(
        task_id='create_local_directories',
        bash_command="""
        docker exec jupyter mkdir -p /data/data_csv_files/bayut \
                                     /data/data_csv_files/dubbizle \
                                     /data/data_csv_files/fazwaz \
                                     /data/data_csv_files/propertyfinder
        """
    )

    scrape_bayut = BashOperator(
        task_id='scrape_bayut',
        bash_command='docker exec jupyter python /data/scraping_scripts/bayut/bayutScript.py'
    )

    scrape_fazwaz = BashOperator(
        task_id='scrape_fazwaz',
        bash_command='docker exec jupyter python /data/scraping_scripts/fazwaz.com/scraping_fazwaz.py'
    )

    scrape_propertyfinder = BashOperator(
        task_id='scrape_propertyfinder',
        bash_command='docker exec jupyter python /data/scraping_scripts/propertyfinder/scraping_propertyfinder.py'
    )

    scrape_dubbizle = BashOperator(
        task_id='scrape_dubbizle',
        bash_command="""
        docker exec jupyter python /data/scraping_scripts/dubbizle/dubbizle_cairo.py && \
        docker exec jupyter python /data/scraping_scripts/dubbizle/scraping_dubbizle_alexandria.py
        """
    )

    # ==================================================================
    # PHASE 2: HADOOP DATA LAKE SETUP
    # ==================================================================

    create_hdfs_folders = BashOperator(
        task_id='create_hdfs_folders',
        bash_command="""
        docker exec namenode hdfs dfs -mkdir -p /datalake/bronze && \
        docker exec namenode hdfs dfs -mkdir -p /datalake/silver && \
        docker exec namenode hdfs dfs -mkdir -p /datalake/gold
        """
    )

    set_hdfs_permissions = BashOperator(
        task_id='set_hdfs_permissions',
        bash_command="docker exec namenode hdfs dfs -chmod -R 777 /datalake"
    )

    # ==================================================================
    # PHASE 3: INGESTION (RAW -> BRONZE)
    # ==================================================================

    ingest_to_bronze = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/ingest_to_bronze.py'
    )

    # ==================================================================
    # PHASE 4: CLEANING (BRONZE -> SILVER)
    # ==================================================================

    clean_propertyfinder = BashOperator(
        task_id='clean_propertyfinder',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Cleaning_Layer_Pyspark/clean_propertyfinder.py'
    )

    clean_bayut = BashOperator(
        task_id='clean_bayut',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Cleaning_Layer_Pyspark/clean_bayut.py'
    )

    clean_fazwaz = BashOperator(
        task_id='clean_fazwaz',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Cleaning_Layer_Pyspark/clean_fazwaz.py'
    )

    clean_dubbizle = BashOperator(
        task_id='clean_dubbizle',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Cleaning_Layer_Pyspark/clean_dubbizle.py'
    )

    # ==================================================================
    # PHASE 5: TRANSFORMATION (SILVER -> GOLD)
    # ==================================================================
    
    transform_propertyfinder = BashOperator(
        task_id='transform_propertyfinder',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Transformation_Layer_Pyspark/transform_propertyfinder.py'
    )

    transform_bayut = BashOperator(
        task_id='transform_bayut',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Transformation_Layer_Pyspark/transform_bayut.py'
    )

    transform_fazwaz = BashOperator(
        task_id='transform_fazwaz',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Transformation_Layer_Pyspark/transform_fazwaz.py'
    )

    transform_dubbizle = BashOperator(
        task_id='transform_dubbizle',
        bash_command='docker exec jupyter spark-submit --master local[*] /data/Transformation_Layer_Pyspark/transform_dubbizle.py'
    )

    # ==================================================================
    # PHASE 6: DATA WAREHOUSE LOADING (GOLD -> POSTGRES)
    # ==================================================================

    create_dwh_table = BashOperator(
        task_id='create_dwh_table',
        bash_command='docker exec jupyter python /data/create_dwh_table.py'
    )

    load_to_dwh = BashOperator(
        task_id='load_to_dwh',
        bash_command="""
        docker exec jupyter spark-submit \
        --master local[*] \
        --jars /data/postgresql-42.6.0.jar \
        /data/load_to_dwh.py
        """
    )

    # ==================================================================
    # PHASE 7: DATA MARTS (POSTGRES -> POSTGRES)
    # ==================================================================

    create_datamarts_ddl = BashOperator(
        task_id='create_datamarts_ddl',
        bash_command='docker exec jupyter python /data/create_datamarts.py'
    )

    populate_datamarts = BashOperator(
        task_id='populate_datamarts',
        bash_command='docker exec jupyter python /data/populate_datamarts.py'
    )

    # ==================================================================
    # DEPENDENCIES
    # ==================================================================
    
    # 1. Scrape
    create_local_dirs >> [scrape_bayut, scrape_fazwaz, scrape_propertyfinder, scrape_dubbizle]

    # 2. Setup HDFS
    [scrape_bayut, scrape_fazwaz, scrape_propertyfinder, scrape_dubbizle] >> create_hdfs_folders >> set_hdfs_permissions
    
    # 3. Ingest
    set_hdfs_permissions >> ingest_to_bronze

    # 4. Clean
    ingest_to_bronze >> [clean_propertyfinder, clean_bayut, clean_fazwaz, clean_dubbizle]

    # 5. Transform
    clean_propertyfinder >> transform_propertyfinder
    clean_bayut >> transform_bayut
    clean_fazwaz >> transform_fazwaz
    clean_dubbizle >> transform_dubbizle

    # 6. Load to DWH
    [transform_propertyfinder, transform_bayut, transform_fazwaz, transform_dubbizle] >> create_dwh_table >> load_to_dwh

    # 7. Create Data Marts
    load_to_dwh >> create_datamarts_ddl >> populate_datamarts
