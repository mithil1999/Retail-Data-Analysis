# Importing all the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialising the Spark Session
spark = SparkSession\
        .builder\
        .appName('Retail-Data Analysis')\
        .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel('ERROR')

# Defining the utility funtions 
def is_order(type):
    return 1 if type=='ORDER' else 0

def is_return(type):
    return 1 if type=='RETURN' else 0

def total_items_cnt(items):
    if items is not None:
         item_cnt=0
         for item in items:
             item_cnt += item['quantity']
         return item_cnt
    
def total_cost(items,type):
    if items is not None:
        total_cst = 0
    for item in items:
        total_cst += item['quantity']*item['unit_price']
    
    if type=='RETURN':
        return total_cst * -1
    else:
        return total_cst
    
# Defining UDF's for the above created utility funtions
is_order = udf(is_order, IntegerType())
is_return = udf(is_return, IntegerType())
total_items_cnt = udf(total_items_cnt, IntegerType())
total_cost = udf(total_cost, DoubleType())

# Read input from Kafka Stram
raw_data = spark\
           .readStream\
           .format('kafka')\
           .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
           .option('subscribe', 'real-time-project')\
           .option('startingOffsets','latest')\
           .load()


# Define the schema 
myschema = StructType()\
           .add('invoice_no',LongType())\
           .add('country',StringType())\
           .add('timestamp',TimestampType())\
           .add('type',StringType())\
           .add('items',ArrayType(StructType([
               StructField('SKU',StringType()),
               StructField("title", StringType()),
               StructField("unit_price", FloatType()),
               StructField("quantity", IntegerType()) 
           ])))

# Extracting the value from raw_data and applying the myschema.

stream_data = raw_data.select(from_json(col('value').cast('string'),myschema).alias('data')).select('data.*')

# Calculating additional columns
stream_data_op = stream_data.withColumn('total_cost',total_cost(stream_data.items,stream_data.type)) \
                            .withColumn('total_items',total_items_cnt(stream_data.items))\
                            .withColumn('is_order',is_order(stream_data.type))\
                            .withColumn('is_return',is_return(stream_data.type))

# Write the final summarized input table to console
stream_data_batch = stream_data_op\
                    .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
                    .writeStream\
                    .outputMode('append')\
                    .format('console')\
                    .option('truncate','false')\
                    .option('path','/Console_output')\
                    .trigger(processingTime="1 minute") \
                    .start()

# Calculating the Time based KPI values

Time_based_agg = stream_data_op\
                 .withWatermark('timestamp','1 minutes')\
                 .groupBy(window('timestamp','1 minute'))\
                 .agg(sum('total_cost').alias('Total_volume_of_sales'),
                 count('invoice_no').alias('OPM'),
                 avg('is_return').alias('Rate_of_return'),
                 avg('total_cost').alias('Average_transaction_size'))\
                 .select("window.start","window.end","OPM","Total_volume_of_sales","Average_transaction_size","Rate_of_return")

# Writing the Time based KPI values to json
Time_based_KPI = Time_based_agg\
                 .writeStream\
                 .outputMode('append')\
                 .format('json')\
                 .option('truncate','false')\
                 .option('path','Time_KPI_values')\
                 .option('checkpointLocation','Time_KPI_values/checkpoints/')\
                 .trigger(processingTime='1 minute')\
                 .start()

# Calculating the Time & Country based KPI values

Time_country_based_agg = stream_data_op\
                 .withWatermark('timestamp','1 minutes')\
                 .groupBy(window('timestamp','1 minute'),'country')\
                 .agg(sum('total_cost').alias('Total_volume_of_sales'),
                 count('invoice_no').alias('OPM'),
                 avg('is_return').alias('Rate_of_return'))\
                 .select("window.start","window.end",'country',"OPM","Total_volume_of_sales","Rate_of_return")

# Writing the Time & Country based KPI values to json
Time_country_based_KPI = Time_country_based_agg\
                 .writeStream\
                 .outputMode('append')\
                 .format('json')\
                 .option('truncate','false')\
                 .option('path','Time_country_KPI_values')\
                 .option('checkpointLocation','Time_country_KPI_values/checkpoints/')\
                 .trigger(processingTime='1 minute')\
                 .start()
                 
stream_data_batch.awaitTermination()
Time_country_based_agg.awaitTermination()
Time_country_based_KPI.awaitTermination()
                 



