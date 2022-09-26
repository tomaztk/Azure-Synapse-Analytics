#!/usr/bin/env python
# coding: utf-8

# ## Read_Data_to_SparkDF
# 
# 
# 

# # Import data from blob store to Spark Data Frame

# In[2]:


# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "holidaydatacontainer"
blob_relative_path = "Processed"
blob_sas_token = r""


# In[3]:


# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)


# In[4]:


# SPARK read parquet, note that it won't load any data yet by now
df = spark.read.parquet(wasbs_path)
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source')


# In[5]:


# Display top 10 rows
print('Displaying top 10 rows: ')
display(spark.sql('SELECT * FROM source LIMIT 10'))


# In[6]:


# Write to Parquet file 
df.write.parquet("abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/holidays")

