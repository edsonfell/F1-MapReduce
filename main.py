import pandas as pd
from pyspark.sql import SparkSession
from utils import csv_reader


def main():
    print("Reading the csv results file...\n")
    race_results_df = csv_reader('./csv_files/results.csv')

    print("Converting some columns to integer\n")
    race_results_df['position'] = pd.to_numeric(race_results_df['position'], errors='coerce').fillna(0)
    race_results_df['raceId'] = pd.to_numeric(race_results_df['raceId'], errors='coerce').fillna(0)
    race_results_df['constructorId'] = pd.to_numeric(race_results_df['constructorId'], errors='coerce').fillna(0)

    print("Preparing a dataframe only with pole position result and the columns needed\n")
    pole_position = race_results_df.query('position == 1')[['position', 'raceId', 'constructorId']]

    print("Starting spark session and context...\n")
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    print("Doing Map...\n")
    df_spark = spark.createDataFrame(pole_position)
    mapped_rdd = df_spark.rdd.flatMap(lambda row: [(row['constructorId'], 1)] if row['position'] == 1 else [])
    print("Doing Reduce...\n")
    reduced_pandas_df = mapped_rdd.reduceByKey(lambda a, b: a + b).toDF(
        ["constructorId", "total_pole_positions_1950_to_2023"]).toPandas()

    print("MapReduce finished. Preparing to merge the final results with the constructors dataframe...\n")
    constructors_df = csv_reader('./csv_files/constructors.csv')
    constructors_df['constructorId'] = pd.to_numeric(constructors_df['constructorId'], errors='coerce').fillna(0)
    final_results_df = pd.merge(constructors_df, reduced_pandas_df, on="constructorId")
    final_results_df = final_results_df.drop(columns=['constructorRef'])

    print("Exporting the result to a CSV file named 'final_result.csv'\n")
    final_results_df.to_csv(path_or_buf="./final_result.csv", sep=";", index=False)

main()
