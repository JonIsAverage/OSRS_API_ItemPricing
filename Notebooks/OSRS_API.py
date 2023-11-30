# Databricks notebook source
import requests, json, time
from pyspark.sql.functions import from_json, lit
#URL Variables
urlPrice = 'https://prices.runescape.wiki/api/v1/osrs/latest'
urlItem = 'https://oldschool.runescape.wiki/?title=Module:GEIDs/data.json&action=raw&ctype=application%2Fjson'

# COMMAND ----------

response = requests.get(urlItem)
json_data = response.json()
popped = json_data.pop('%LAST_UPDATE%')
popped = json_data.pop('%LAST_UPDATE_F%')
df_items = spark.createDataFrame(list(json_data.items()), ["ItemName", "ItemID"]).selectExpr("CAST(ItemID AS INT)", "ItemName")

# COMMAND ----------

response = requests.get(urlPrice)
data = response.json()
item_data = data.get('data', {})
ts_epoch = int(time.time())

# Convert the data to a Spark DataFrame
df_itemprice = spark.createDataFrame(
    [(item_id, item.get("high", 0), item.get("highTime", 0), item.get("low", 0), item.get("lowTime", 0)) for item_id, item in item_data.items()],
    ["id","high", "highTime", "low", "lowTime"]
)
df_itemprice = df_itemprice.withColumn("ScanTime", lit(ts_epoch))

# COMMAND ----------

df_itemprice.createOrReplaceTempView("tmp_itemprice_n")
df_items.createOrReplaceTempView("tmp_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_itemprice AS
# MAGIC SELECT CONCAT(id, ScanTime) as ScanID,
# MAGIC     id as ItemID,
# MAGIC     high as HighPrice,
# MAGIC     highTime as HighTime,
# MAGIC     low as LowPrice,
# MAGIC     lowtime LowTime,
# MAGIC     ScanTime FROM tmp_itemprice_n;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert or update item information
# MAGIC MERGE INTO default.item_info USING tmp_items
# MAGIC ON default.item_info.ItemID = tmp_items.ItemID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     ItemID = tmp_items.ItemID,
# MAGIC     ItemName = tmp_items.ItemName
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     ItemID,
# MAGIC     ItemName
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     tmp_items.ItemID,
# MAGIC     tmp_items.ItemName
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC   --Insert or delete item prices based on a match from source to destination table
# MAGIC   MERGE INTO default.item_prices USING tmp_itemprice
# MAGIC   ON default.item_prices.ScanID = tmp_itemprice.ScanID
# MAGIC   WHEN MATCHED THEN DELETE
# MAGIC   WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     ScanID,
# MAGIC     ItemID,
# MAGIC     HighPrice,
# MAGIC     HighTime,
# MAGIC     LowPrice,
# MAGIC     LowTime,
# MAGIC     ScanTime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     tmp_itemprice.ScanID,
# MAGIC     tmp_itemprice.ItemID,
# MAGIC     tmp_itemprice.HighPrice,
# MAGIC     tmp_itemprice.HighTime,
# MAGIC     tmp_itemprice.LowPrice,
# MAGIC     tmp_itemprice.LowTime,
# MAGIC     tmp_itemprice.ScanTime
# MAGIC   );
