from pyspark.sql import SparkSession



#https://github.com/HatemMS2012/filehosting/blob/master/mysql-connector-java-5.1.38.jar
spark = SparkSession.builder.appName("Python Spark SQL basic example").\
    config('spark.driver.extraClassPath','https://raw.github.com/HatemMS2012/filehosting/blob/master/mysql-connector-java-5.1.38.jar')\
    .config("spark.master","local[*]").getOrCreate()


db_url = "jdbc:mysql://mysql.marathon.l4lb.thisdcos.directory:3306/student?user=root&password=root"
db_driver = "com.mysql.jdbc.Driver"
table_name = "student"

def insert_dataframe_into_db(df,table_name,db_url,db_driver):
    df.write.jdbc(url=db_url,
                  table=table_name,
                  mode="append",
                  properties={"driver": db_driver})


df_json = spark.read.json("/home/hmoussel/myscript/student.json", multiLine=True)

df_json.printSchema()


print("inserting the table")

print(df_json)
insert_dataframe_into_db(df_json,table_name,db_url,db_driver)


print("done")
