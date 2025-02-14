from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import requests
import psycopg2
from psycopg2.extras import execute_values
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='A simple DAG to interact with ClickHouse and PostgreSQL without libraries',
    schedule_interval=None,
)


def query_clickhouse(**kwargs):
    response = requests.get('http://clickhouse_user:8123/?query=SELECT%20version()')
    if response.status_code == 200:
        print(f"ClickHouse version: {response.text}")
    else:
        print(f"Failed to connect to ClickHouse, status code: {response.status_code}")


def query_postgres(**kwargs):
    command = [
        'psql',
        '-h', 'postgres_user',
        '-U', 'user',
        '-d', 'test',
        '-c', 'SELECT version();'
    ]
    env = {"PGPASSWORD": "password"}
    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"PostgreSQL version: {result.stdout}")
    else:
        print(f"Failed to connect to PostgreSQL, error: {result.stderr}")


def read_csv_and_process(**kwargs):
    try:
        spark = SparkSession.builder \
            .appName("Read CSV file") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.20.jar") \
            .getOrCreate()

        # Схемы для DataFrame
        schema_airlines = StructType([
            StructField("IATA_CODE", StringType(), True),
            StructField("AIRLINE", StringType(), True)
        ])
        schema_airports = StructType([
            StructField("IATA_CODE", StringType(), True),
            StructField("Airport", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True)
        ])
        schema_flights = StructType([
            StructField("DATE", DateType(), True),
            StructField("DAY_OF_WEEK", StringType(), True),
            StructField("AIRLINE", StringType(), True),
            StructField("FLIGHT_NUMBER", IntegerType(), True),
            StructField("TAIL_NUMBER", StringType(), True),
            StructField("ORIGIN_AIRPORT", StringType(), True),
            StructField("DESTINATION_AIRPORT", StringType(), True),
            StructField("DEPARTURE_DELAY", DoubleType(), True),
            StructField("DISTANCE", DoubleType(), True),
            StructField("ARRIVAL_DELAY", DoubleType(), True),
            StructField("DIVERTED", IntegerType(), True),
            StructField("CANCELLED", IntegerType(), True),
            StructField("CANCELLATION_REASON", StringType(), True),
            StructField("AIR_SYSTEM_DELAY", DoubleType(), True),
            StructField("SECURITY_DELAY", DoubleType(), True),
            StructField("AIRLINE_DELAY", DoubleType(), True),
            StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
            StructField("WEATHER_DELAY", DoubleType(), True),
            StructField("DEPARTURE_HOUR", DoubleType(), True),
            StructField("ARRIVAL_HOUR", DoubleType(), True)
        ])

        # Чтение CSV-файлов
        df_airlines = spark.read.csv("/opt/airflow/data/airlines.csv", header=True, schema=schema_airlines)
        df_airports = spark.read.csv("/opt/airflow/data/airports.csv", header=True, schema=schema_airports)
        df_flights = spark.read.csv("/opt/airflow/data/flights_pak.csv", header=True, schema=schema_flights)

        # Подсчет количества строк
        print(f"Rows in airlines.csv: {df_airlines.count()}")
        print(f"Rows in airports.csv: {df_airports.count()}")
        print(f"Rows in flights_pak.csv: {df_flights.count()}")

        # Проверка пустых значений
        for df_name, df in [("airlines", df_airlines), ("airports", df_airports), ("flights", df_flights)]:
            null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
            if any(null_counts.values()):
                print(f"Null values found in {df_name}: {null_counts}")

        # Топ-5 авиалиний с наибольшей средней задержкой
        top_5_avg_delay = (
            df_flights.groupBy("AIRLINE")
            .agg(avg("ARRIVAL_DELAY").alias("AVG_DELAY"))
            .sort("AVG_DELAY", ascending=False)
        )
        print("Топ 5 авиалиний с наибольшей средней задержкой:")
        top_5_avg_delay.show(5)

        # Процент отмененных рейсов для каждого аэропорта
        cancelled_flights = (
            df_flights.groupBy("ORIGIN_AIRPORT")
            .agg(
                count("*").alias("total_flights"),
                count(when(col("CANCELLED") == 1, True)).alias("cancelled_flights")
            )
            .withColumn("percentage_cancelled", (col("cancelled_flights") / col("total_flights")) * 100)
            .orderBy(col("percentage_cancelled").desc())
        )
        print("Процент отмененных рейсов по аэропортам:")
        cancelled_flights.show()

        # Время суток с максимальными задержками
        time_of_day_delays = (
            df_flights.withColumn(
                "time_of_day",
                when((col("DEPARTURE_HOUR") >= 6) & (col("DEPARTURE_HOUR") <= 11), "Утро")
                .when((col("DEPARTURE_HOUR") >= 12) & (col("DEPARTURE_HOUR") <= 17), "День")
                .when((col("DEPARTURE_HOUR") >= 18) & (col("DEPARTURE_HOUR") <= 23), "Вечер")
                .otherwise("Ночь")
            )
            .groupBy("time_of_day")
            .agg(avg("ARRIVAL_DELAY").alias("avg_delay"))
            .sort("avg_delay", ascending=False)
        )
        print("Время суток с максимальными задержками:")
        time_of_day_delays.show()

        # Добавление новых столбцов
        df_flights_enriched = df_flights.withColumns({
            "IS_LONG_HAUL": when(col("DISTANCE") > 1000, "Дальнемагистральный").otherwise("Не дальнемагистральный"),
            "DAY_PART": when((col("DEPARTURE_HOUR") >= 6) & (col("DEPARTURE_HOUR") <= 11), "Утро")
            .when((col("DEPARTURE_HOUR") >= 12) & (col("DEPARTURE_HOUR") <= 17), "День")
            .when((col("DEPARTURE_HOUR") >= 18) & (col("DEPARTURE_HOUR") <= 23), "Вечер")
            .otherwise("Ночь")
        })
        df_flights_enriched.show()

        # Настройка подключения к PostgreSQL
        hook = PostgresHook(postgres_conn_id="conn1")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Вставка данных в таблицу flights_pak
        insert_query = """
        INSERT INTO flights_pak (
            date, day_of_week, airline, flight_number, tail_number, origin_airport, destination_airport,
            departure_delay, distance, arrival_delay, diverted, cancelled, cancellation_reason,
            air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay,
            departure_hour, arrival_hour, is_long_haul, day_part
        ) VALUES %s;
        """
        data_to_insert = [
            tuple(row.asDict().values()) for row in df_flights_enriched.limit(10000).collect()
        ]

        execute_values(cursor, insert_query, data_to_insert)
        conn.commit()

        # Вставка данных в таблицу airlines
        insert_query = """
                INSERT INTO airlines (
                    iata_code, airline
                ) VALUES %s;
                """
        data_to_insert = [
            tuple(row.asDict().values()) for row in df_airlines.collect()
        ]

        execute_values(cursor, insert_query, data_to_insert)
        conn.commit()

        # Вставка данных в таблицу airports
        insert_query = """
                INSERT INTO airports (
                    iata_code, airport, city, latitude, longitude
                ) VALUES %s;
                """
        data_to_insert = [
            tuple(row.asDict().values()) for row in df_airports.collect()
        ]

        execute_values(cursor, insert_query, data_to_insert)
        conn.commit()

        # Выполнение SQL-скрипта для подсчета общего времени полетов
        total_flight_time_query = """
        WITH flight_durations AS (
            SELECT 
                airline,
                CASE 
                    WHEN arrival_hour >= departure_hour THEN arrival_hour - departure_hour
                    ELSE (arrival_hour + 24) - departure_hour
                END AS flight_time
            FROM flights_pak
        )
        SELECT 
            airline, 
            SUM(flight_time) AS total_flight_time
        FROM 
            flight_durations
        GROUP BY 
            airline
        ORDER BY 
            total_flight_time DESC;
        """

        cursor.execute(total_flight_time_query)
        results = cursor.fetchall()
        print("Общее время полетов по авиакомпаниям:")
        for row in results:
            print(f"Авиакомпания: {row[0]}, Общее время полетов: {row[1]} часов")

    except Exception as e:
        print(f"Error processing flight data: {e}")
        if e is not None:
            raise  # Повторно выбрасываем исключение, если оно существует


task_query_clickhouse = PythonOperator(
    task_id='query_clickhouse',
    python_callable=query_clickhouse,
    dag=dag,
)

task_query_postgres = PythonOperator(
    task_id='query_postgres',
    python_callable=query_postgres,
    dag=dag,
)

task_read_csv_and_process = PythonOperator(
    task_id='read_csv_and_process',
    python_callable=read_csv_and_process,
    dag=dag,
)

# Зависимости между задачами
[task_query_clickhouse, task_query_postgres] >> task_read_csv_and_process
