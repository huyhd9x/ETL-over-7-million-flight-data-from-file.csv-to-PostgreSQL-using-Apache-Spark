from pyspark.sql import SparkSession 
from pyspark import SparkConf
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import concat_ws, col, count, when, isnan, mean, round
import os 

#Connection details with PostgreSQL
PSQL_SERVERNAME = "192.168.1.3"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "mydb"
PSQL_USRRNAME = "etl"
PSQL_PASSWORD = "2207"
URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

# Name_Table you want to create in the database 
TABLE_POSTGRES = "flight_2008"

# Connect SparkSession
conf = SparkConf()
path = os.getenv("PATH_POSTGRES_DRIVER")
conf.set("spark.jars", path) 
spark = SparkSession.builder.config(conf=conf).appName("connect postgresql").getOrCreate()

def extract_data(spark):
    path = os.getenv("PATH_FILE_CSV")
    df = spark.read.option('inferSchema',True).option('header' , True).csv(path)

    return df

def transform_data(df):
    # Define Schema
    df_write = df.select('year','month','dayofmonth','origin','dest','distance','carrier_name','airtime','depdelay','origin_city','dest_city','cancelled')
    
    df_write = df_write.withColumn("airtime",df_write["airtime"].cast(FloatType()))\
        .withColumn("depdelay",df_write["depdelay"].cast(IntegerType()))
    df_write.printSchema()
    
    '''
    # Check null, nan
    df_write.select([count(when(col(c).contains('Null') | \
                                (col(c) == '' ) | \
                                col(c).isNull() | \
                                isnan(c), c 
                               )).alias(c)
                        for c in df_write.columns]).show()
    '''

    # Transform missing data
    mean_airtime = df_write.select(round(mean(df_write['airtime']),2)).collect()[0][0]
    
    df_write = df_write.fillna(value=mean_airtime, subset=['airtime'])\
        .fillna(value=0, subset=['depdelay'])
    
    # Merge year, month, dayofmonth columns into date column, and remove year, month, dayofmonth columns
    df_write = df_write.withColumn("date",concat_ws("-",col("year"),col("month"),col("dayofmonth")).cast("date"))\
        .drop('year', 'month','dayofmonth')\
        .select('origin','dest','date','distance','carrier_name','airtime','depdelay','origin_city','dest_city','cancelled')
    df_write.show(20)
    
    return(df_write)

def load_data(df_write):
    df_write.write\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", TABLE_POSTGRES)\
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .option("driver", "org.postgresql.Driver") \
        .save()
    print('-------------------')
    print('|Load Successfully|')
    print('-------------------')

if __name__ == "__main__":
    extract = extract_data(spark)
    transform = transform_data(extract)
    load_data(transform)