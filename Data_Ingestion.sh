mv *.csv finalproj
cd finalproj
hdfs dfs -put winequality-red.csv finalproj/redwine.csv
hdfs dfs -put winequality-white.csv finalproj/whitewine.csv

spark-shell --deploy-mode client -i  Final_project_clean_zw2972.scala

hdfs dfs -ls finalproj/output/redwine
hdfs dfs -ls finalproj/output/whitewine
#After running the cleaning code

hdfs dfs -mv finalproj/output/whitewine/part-00000-d8da95a7-7093-4b34-a319-361b306f9787-c000.csv finalproj/whitewinecleaned.csv
hdfs dfs -mv finalproj/output/redwine/part-00000-f0ed6311-2c96-4b72-91a4-1ebe66fcd8b0-c000.csv finalproj/redwinecleaned.csv