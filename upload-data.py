#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd


# In[7]:


df = pd.read_parquet("/Users/bernardo/workspace/yellow_tripdata_2021-01.parquet", engine="pyarrow")


# In[18]:


print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


# In[15]:


from sqlalchemy import create_engine


# In[16]:


engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")


# In[17]:


engine.connect()


# In[19]:


df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")


# In[20]:


get_ipython().run_line_magic('time', 'df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")')

