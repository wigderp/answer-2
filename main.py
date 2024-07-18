from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("product_category_join").getOrCreate()

products_df = spark.read.format("csv").options(header="true").load("path_to_products.csv")
categories_df = spark.read.format("csv").options(header="true").load("path_to_categories.csv")

# Выполнение объединения продуктов по общему ключу
joined_df = products_df.join(categories_df, products_df.product_id == categories_df.product_id, "left")

# Вывод всех пар "Имя продукта - Имя категории"
joined_df.select("product_name", "category_name").show()

# Вывод имен всех продуктов, у которых нет категорий
products_without_category = joined_df.filter(col("category_name").isNull()).select("product_name")
products_without_category.show()