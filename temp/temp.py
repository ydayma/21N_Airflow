import pandas as pd

df = pd.read_sql_query("SELECT * from 21N_queue WHERE creationdate like '2021-06-16%'",
con='mysql://yantra:0FRZ*j7,>M>C,&tJ@172.31.23.233:3306/21north') 

df.to_sql(name='21N_queue',
         con='mysql://analytics:k9%^88*Lf5EDC7KP@asia-database-instance-1.cdiynpmpjjje.ap-southeast-1.rds.amazonaws.com',
         schema='analytics',
         if_exists='append',
         chunksize=10000)

         