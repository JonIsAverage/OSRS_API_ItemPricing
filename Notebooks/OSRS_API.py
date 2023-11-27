# Databricks notebook source
import requests, json, pandas as pd, time
#URL Variables
urlPrice = 'https://prices.runescape.wiki/api/v1/osrs/latest'
urlItem = 'https://oldschool.runescape.wiki/?title=Module:GEIDs/data.json&action=raw&ctype=application%2Fjson'

# COMMAND ----------

page = requests.get(urlItem)
data = json.loads(page.text)
df = pd.DataFrame(data.items())
#Remove first two lines
df = df.iloc[2:]
df.columns = ['ItemName','ItemID']
df_items = df
del df

# COMMAND ----------

page = requests.get(urlPrice)

df_itemprice = pd.DataFrame(columns=['ScanID','ItemID','HighPrice','HighTime','LowPrice','LowTime','ScanTime'])

lst = []

for row in df_items.itertuples():
    ItemID=row[0]
    ts_epoch = int(time.time())
    ScanID = str(ItemID) + str(ts_epoch)
    HighPrice = ''
    HighTime = ''
    LowPrice = ''
    LowTime = ''

    json_data = page.json()
    value = json_data.get('data')
    value = value.get(str(ItemID), "empty")

    if value == 'empty':
        HighPrice = 'null'
        HighTime = 'null'
        LowPrice = 'null'
        LowTime = 'null'
    else:
        fullinfo = json_data['data'][str(ItemID)]
        HighPrice = str(fullinfo['high'])
        HighTime = str(fullinfo['highTime'])
        LowPrice = str(fullinfo['low'])
        LowTime = str(fullinfo['lowTime'])
        
        lst.append({'ScanID': ScanID, 'ItemID': ItemID, 'HighPrice': HighPrice, 'HighTime': HighTime, 'LowPrice': LowPrice, 'LowTime': LowTime, 'ScanTime': ts_epoch})
df_itemprice = pd.DataFrame(lst, columns=['ScanID','ItemID','HighPrice','HighTime','LowPrice','LowTime','ScanTime'])

# COMMAND ----------

#Write the data to a table
#Create as spark dataframe
spark_df_itemprice = spark.createDataFrame(df_itemprice)
spark_df_items = spark.createDataFrame(df_items)
del df_itemprice
del df_items
#Create temp view from spark dataframe
spark_df_itemprice.createOrReplaceTempView("tmp_itemprice")
spark_df_items.createOrReplaceTempView("tmp_items")

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.item_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.item_info
