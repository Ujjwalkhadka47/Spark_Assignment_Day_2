from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit

# Create a Spark session
spark = SparkSession.builder.appName("day2").getOrCreate()

data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Assignment_2023/Day_2/occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)


occupation.printSchema()

q1 = occupation.select('user_id',"age","occupation")
q1.show()

older_than_30 = occupation.filter(occupation.age>30)
older_than_30.show()

occupation.groupBy("occupation").count().withColumnRenamed("count","user_count").show()


age_grp = occupation.withColumn("age_group",
                      when((occupation.age).between(18,25), "18-25")
                      .when((occupation.age).between(26,35), "26-35")
                      .when((occupation.age).between(36,50), "36-50")
                      .otherwise("51+")
                      )

age_grp.show()


unknown_gender = occupation.withColumn("gender",lit("unknown"))
check = unknown_gender.withColumnRenamed("age","Years")
check.show()


filtered_task = check.filter(occupation.age>30)
sorted_result = filtered_task.orderBy(occupation.age.desc())
sorted_result.show()

# from pyspark.sql import SparkSession

def create_session():
  spk = SparkSession.builder \
      .master("local") \
      .appName("dataframe_building") \
      .getOrCreate()
  return spk

spark1 = create_session()

df = spark1.createDataFrame([
    ("james","","smith","36636","M",3000),
    ("micheal","rose","","40288","M",4000),
    ("robert","","williams","42114","M",4000),
    ("maria","anne","jones","39192","F",4000),
    ("jen","mary","brown","","F",-1),
],["firstname","middlename","lastname","id","gender","salary"])

df.show()

df.printSchema()






# repartitioned_df = df.coalesce(2)

# # Collect and display all rows in the driver
# all_rows = repartitioned_df.collect()
# for row in all_rows:
#     print(row)

# # Get the number of partitions
# num_partitions = repartitioned_df.rdd.getNumPartitions()
# print(num_partitions)