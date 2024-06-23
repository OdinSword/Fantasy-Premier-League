import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from src.constants import POSTGRES_URL, POSTGRES_PROPERTIES, DB_FIELDS

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("PostgreSQL Connection with PySpark")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return spark

def create_initial_dataframe(spark_session):
    df = {}

    topics = []
    for key in DB_FIELDS:
        topics.append(key)

    for topic in topics:
        try:
            data = (
                spark_session.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
            )
            df[topic] = data
            logging.info(f"Initial dataframe for topic '{topic}' created successfully")
        except Exception as e:
            logging.warning(f"Initial dataframe for topic '{topic}' couldn't be created due to exception: {e}")
            raise

    return df

def create_final_dataframe(df):
    df_out = {}
    for key in DB_FIELDS:
        schema = StructType(
            [StructField(field, StringType(), True) for field in DB_FIELDS[key]]
        )
        df1 = (
            df[key].selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )
        df_out[key] = df1

        print(df_out.keys())

    return df_out


def write_to_postgres(df, table_name):
    """
    Write DataFrame to PostgreSQL table
    """
    try:
        df.write.jdbc(
            POSTGRES_URL, table_name, mode="append", properties=POSTGRES_PROPERTIES
        )
        logging.info(f"Data written to {table_name} successfully.")
    except Exception as e:
        logging.error(f"Error writing data to {table_name}: {e}")

def start_streaming(df_parsed):
    """
    Starts the streaming to tables in PostgreSQL
    """
    queries = []
    for table_name, df in df_parsed.items():
        logging.info(f"Starting streaming into {table_name}")
        
        # Define the query to trigger once a day

        print("table_name: ", table_name)
        query = df.writeStream \
                  .trigger(processingTime='24 hours') \
                  .foreachBatch(lambda batch_df, _: write_to_postgres(batch_df, table_name)) \
                  .option("checkpointLocation", f"/tmp/checkpoints/{table_name}") \
                  .start()
        
        queries.append(query)

    for query in queries:
        query.awaitTermination()



if __name__ == "__main__":
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df)
    start_streaming(df_final)
    # write_to_postgres(df_final)



