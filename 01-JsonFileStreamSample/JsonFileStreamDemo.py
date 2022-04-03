from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

#from lib.logger import Log4j

#https://hackersandslackers.com/structured-streaming-in-pyspark/

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Json File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    raw_df = spark.readStream.format("json") \
       .option("path", "ResultSet/input") \
       .option("maxFilesPerTrigger", 1) \
       .load()

    raw_df.printSchema()

    selectdf = raw_df.select("AddressLineOne", "AddressLineTwo")
    selectdf.printSchema()

    #writeAddrDF = selectdf.writeStream \
     #   .format("console") \
      #  .outputMode("append") \
       # .start() \
        #.awaitTermination()

    writeOutputDF = selectdf.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "ResultSet/output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start() \
        .awaitTermination()



