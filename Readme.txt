Wine Quality Predictions
Dec.1.2023
Jacob Wang zw2972


This is a project about Wines Quality Predictions, For this project I run it on the Dataproc HPC, which Spark and hadoop has been installed. 

For all scala files, people can run it by 
		spark-shell --deploy-mode client -i  FILENAME.scala

The two data sets are under the Data directory
1) winequality-red.csv [84KB] 2)winequality-red.csv[263KB]

For initial data loading, after uploading the initial data set to VM or local machines, run Data_ingestion.sh is added, 
including commands for putting the files to the HDFS, and the commands for running the data cleaning codes, and adding the cleaned csv in hdfs. 


In Clean.scala, The code has removed the rows which have density that is bigger than 1, And it also simplified the density column to 
4 digits after decimal points and the alcohol column to 1 digit after decimal points and outputs the cleaned csv to "desired directory"

In Profiling.scala, The code has print the number of counts of the data sets before cleaning and after cleaning for the two data sets. 
In addition to that it also print the number of counts for each quality group in the two data sets, and also the number of digits for each column.

In Analysis.scala, the code is printing the mean, sd, max, and min values for each quality group for every column in the two data sets, and also printed them after combining the two data sets. 

In the Predtion_model.scala, the code first did simple linear regression, and then doing Random forest with K-fold cross-validation for 10 folds.



