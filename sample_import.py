from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row


print("hello 555")


#https://github.com/HatemMS2012/filehosting/blob/master/mysql-connector-java-5.1.38.jar
spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()


db_url = "jdbc:mysql://100.66.1.15:5901/student?user=root&password=root"
db_driver = "com.mysql.jdbc.Driver"
table_name = "student"




def insert_dataframe_into_db(df,table_name,db_url,db_driver):
    df.write.jdbc(url=db_url,
                  table=table_name,
                  mode="append",
                  properties={"driver": db_driver})







print("mysql config  3 start ")
sql = SQLContext(spark)
df = sql.read.format('jdbc').options(
   url='jdbc:mysql://100.66.1.15:5901/student',
   user='root',
   password='root',
   driver="org.mariadb.jdbc.Driver",
   dbtable='student'
).load()

print("mysql config 3")
#df_json = spark.read.json("file://student.json", multiLine=True)
# Create a data frame manually

data =  Row(_id= '1', company='apple', frist_name='jack' , last_name= 'BLUE')


df_json = spark.createDataFrame([data])

df_json.printSchema()

print("inserting the table")

print(df_json)

insert_dataframe_into_db(df_json,table_name,db_url,db_driver)


print("done processing done done fads fdf ")
