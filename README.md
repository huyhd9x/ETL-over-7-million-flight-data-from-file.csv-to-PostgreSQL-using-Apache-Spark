# ETL-over-7-million-flight-data-from-file.csv-to-PostgreSQL-using-Apache-Spark

This project contains 2 files and 1 folder (before the README.md).

1. etl.py file is written based on pyspark for the purpose of ETL (extract, transform and load data) into Postgresql.

2. link_download_data_3.2Gb.txt file contains link to download data (over 7 milion flights data).

3. Docker-Spark folder used for config spark cluster on docker.

EXTRACT DATA FROM FILE.CSV

Data consists of 7009728 rows and 56 columns

![image](https://user-images.githubusercontent.com/103510278/169942544-8b65c024-c280-4c86-9303-1efb53464c63.png)

TRANSFORM DATA

Select the required columns and find nulls to process.

![image](https://user-images.githubusercontent.com/103510278/169942576-12d531c7-e578-4f87-ac39-59153444b7f0.png)

Fill missing data and merge year, month, dayofmonth columns into date column, and remove year, month, dayofmonth columns.

![image](https://user-images.githubusercontent.com/103510278/169942644-d4f64a6e-d541-4162-8425-e0dc9c2548bb.png)

LOAD DATA

Query the data that has just been uploaded to Postgresql.

![image](https://user-images.githubusercontent.com/103510278/169942677-4f4a9a27-3894-48cc-851d-1739500f67ef.png)

We can monitor spark-submit running via "docker logs spark-submit -f" command line.

![image](https://user-images.githubusercontent.com/103510278/169942709-c73d7a04-84d5-415d-ac6e-f64633e1db45.png)

Check the completion time of spark-cluster.

![image](https://user-images.githubusercontent.com/103510278/169942736-f64d2b84-dedc-428d-980d-e1090e6ae643.png)
