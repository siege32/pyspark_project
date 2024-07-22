from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("task_for_job").getOrCreate()

products_data = [(1, "product1"), (2, "product2"), (3, "product3"), (4, "product4"), (5, "product5"), (6, "product6")]
categories_data = [(1, "category1"), (2, "category2"), (3, "category3")]
relations_data = [(1, 1), (1, 2), (2, 1), (3, 2), (4, 1), (5, 2)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

pairs_df = relations_df.join(products_df, "product_id").join(categories_df, "category_id") \
    .select("product_name", "category_name")

# Продукты без категорий
products_with_no_category_df = products_df.join(relations_df, "product_id", "left") \
    .filter(col("category_id").isNull()) \
    .select("product_name")

# Показ результатов
pairs_df.show()
products_with_no_category_df.show()
