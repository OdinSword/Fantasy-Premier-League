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

# def write_to_postgres(batch_df, table_name):
#     """
#     Write batch DataFrame to PostgreSQL table
#     """
#     try:
#         batch_df.write.jdbc(
#             POSTGRES_URL, table_name, mode="append", properties=POSTGRES_PROPERTIES
#         )
#         logging.info(f"Batch written to {table_name} successfully.")
#     except Exception as e:
#         logging.error(f"Error writing batch to {table_name}: {e}")

# def start_streaming(df_parsed, spark):
#     """
#     Starts the streaming to tables in PostgreSQL
#     """
#     queries = []
#     for table_name, df in df_parsed.items():
#         logging.info(f"Starting streaming into {table_name}")
#         query = df.writeStream \
#                   .foreachBatch(lambda batch_df, _: write_to_postgres(batch_df, table_name)) \
#                   .option("checkpointLocation", f"/tmp/checkpoints/{table_name}") \
#                   .start()
#         queries.append(query)

#     for query in queries:
#         query.awaitTermination()


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



# from pyspark.sql import SparkSession
# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     StringType,
# )
# from pyspark.sql.functions import from_json, col
# from src.constants import POSTGRES_URL, POSTGRES_PROPERTIES, DB_FIELDS
# import logging


# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
# )


# def create_spark_session() -> SparkSession:
#     spark = (
#         SparkSession.builder.appName("PostgreSQL Connection with PySpark")
#         .config(
#             "spark.jars.packages",
#             "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",

#         )
#         .getOrCreate()
#     )

#     logging.info("Spark session created successfully")
#     return spark


# def create_initial_dataframe(spark_session):
#     # """
#     # Reads the streaming data and creates the initial dataframe accordingly.
#     # """
#     # try:
#     #     # Gets the streaming data from topic random_names
#     #     df = (
#     #         spark_session.readStream.format("kafka")
#     #         .option("kafka.bootstrap.servers", "kafka:9092")
#     #         .option("subscribe", "rappel_conso")
#     #         .option("startingOffsets", "earliest")
#     #         .load()
#     #     )
#     #     logging.info("Initial dataframe created successfully")
#     # except Exception as e:
#     #     logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
#     #     raise

#     df = {}
#     topics = ['elements', 'teams', 'fixtures']

#     for topic in topics:
#         try:
#             data = (
#                 spark_session.readStream.format("kafka")
#                 .option("kafka.bootstrap.servers", "kafka:9092")
#                 .option("subscribe", topic)
#                 .option("startingOffsets", "earliest")
#                 .load()
#             )

#             df[topic] = data
#             logging.info("Initial dataframe created successfully")

#         except Exception as e:
#             logging.warning(f"Initial dataframe for topic '{topic}' couldn't be created due to exception: {e}")
#             raise

#     return df


# def create_final_dataframe(df):
#     """
#     Modifies the initial dataframe, and creates the final dataframe.
#     """
#     # schema = StructType(
#     #     [StructField(field_name, StringType(), True) for field_name in DB_FIELDS]
#     # )
#     # df_out = (
#     #     df.selectExpr("CAST(value AS STRING)")
#     #     .select(from_json(col("value"), schema).alias("data"))
#     #     .select("data.*")
#     # )
#     # return df_out

#     df_out = {}

#     for key in DB_FIELDS:
#         schema = StructType(
#             [StructField(field, StringType(), True) for field in DB_FIELDS.get(key)]
#         )

#         df1 = (
#             df[key].selectExpr("CAST(value AS STRING)")
#         .select(from_json(col("value"), schema).alias("data"))
#         .select("data.*")
#         )

#         df_out[key] = df1

    
#     return df_out



# # def start_streaming(df_parsed, spark):
# #     # """
# #     # Starts the streaming to table spark_streaming.rappel_conso in postgres
# #     # """
# #     # # Read existing data from PostgreSQL
# #     # existing_data_df = spark.read.jdbc(
# #     #     POSTGRES_URL, "rappel_conso_table", properties=POSTGRES_PROPERTIES
# #     # )

# #     # unique_column = "reference_fiche"

# #     # logging.info("Start streaming ...")
# #     # query = df_parsed.writeStream.foreachBatch(
# #     #     lambda batch_df, _: (
# #     #         batch_df.join(
# #     #             existing_data_df, batch_df[unique_column] == existing_data_df[unique_column], "leftanti"
# #     #         )
# #     #         .write.jdbc(
# #     #             POSTGRES_URL, "rappel_conso_table", "append", properties=POSTGRES_PROPERTIES
# #     #         )
# #     #     )
# #     # ).trigger(once=True) \
# #     #     .start()

# #     # return query.awaitTermination()


# #     for keys, df in df_parsed.items():
# #         logging.info("Start streaming into {keys}")

# #         query = df.write.jdbc(
# #             POSTGRES_URL, keys, "overwrite", properties=POSTGRES_PROPERTIES
# #         )
        

# #     return query.awaitTermination()


# def write_to_postgres(batch_df, batch_id, table_name):
#     """
#     Write batch DataFrame to PostgreSQL table
#     """
#     try:
#         # Use append mode to avoid overwriting existing data
#         batch_df.write.jdbc(
#             POSTGRES_URL, table_name, mode="append", properties=POSTGRES_PROPERTIES
#         )
#         logging.info(f"Batch {batch_id} written to {table_name} successfully.")
#     except Exception as e:
#         logging.error(f"Error writing batch {batch_id} to {table_name}: {e}")


# def start_streaming(df_parsed, spark):
#     """
#     Starts the streaming to tables in PostgreSQL
#     """
#     queries = []
#     for table_name, df in df_parsed.items():
#         logging.info(f"Starting streaming into {table_name}")
#         query = df.writeStream \
#                   .foreachBatch(lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, table_name)) \
#                   .option("checkpointLocation", f"/tmp/checkpoints/{table_name}") \
#                   .start()
#         queries.append(query)

#     for query in queries:
#         query.awaitTermination()


# # def start_streaming(df_parsed, spark):
# #     """
# #     Starts the streaming to tables in PostgreSQL
# #     """
# #     def write_to_postgres(batch_df, batch_id, table_name):
# #         """
# #         Write batch DataFrame to PostgreSQL table
# #         """
# #         try:
# #             # Use append mode to avoid overwriting existing data
# #             batch_df.write.jdbc(
# #                 POSTGRES_URL, table_name, mode="append", properties=POSTGRES_PROPERTIES
# #             )
# #             logging.info(f"Batch {batch_id} written to {table_name} successfully.")
# #         except Exception as e:
# #             logging.error(f"Error writing batch {batch_id} to {table_name}: {e}")

# #     queries = []

# #     for table_name, df in df_parsed.items():
# #         logging.info(f"Starting streaming into {table_name}")

# #         query = df.writeStream.foreachBatch(
# #             lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, table_name)
# #         ).start()

# #         # queries.append(query)
# #         query.awaitTermination()

#     # for query in queries:
#     #     query.awaitTermination()


# # def write_to_postgres():
# #     spark = create_spark_session()
# #     df = create_initial_dataframe(spark)
# #     df_final = create_final_dataframe(df)
# #     start_streaming(df_final, spark=spark)


# if __name__ == "__main__":
#     spark = create_spark_session()
#     df = create_initial_dataframe(spark)
#     df_final = create_final_dataframe(df)
#     start_streaming(df_final, spark=spark)
