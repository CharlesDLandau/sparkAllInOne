import pyspark
import sys

sc = pyspark.SparkContext()

# Dummy task
terms = ["fodder" for x in range(9000)] + ["spartan", "sparring", "sparrow"]
words = sc.parallelize(terms)
words = words.filter(lambda w: w.startswith("spar"))

# Output target was passed to args config field
bucket = sys.argv[1]
output_directory = 'gs://{}/pyspark_output'.format(bucket)

# For csv output
sqlContext = pyspark.SQLContext(sc)
df = sqlContext.createDataFrame(words, pyspark.sql.types.StringType())
df.coalesce(1).write.format('com.databricks.spark.csv'
            ).options(header='true').save(output_directory)