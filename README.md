Assumptions:

1. Java 8 is installed
2. Spark latest version is installed
3. docker is installed
4. maven 3 is installed

Given:

1. project code 
2. init.sql
3. Dockerfile

Postgres installation:

1. Go to folder named 'postgres' and Run below command:
	docker build -t my-postgres-image .
	
2. Post installation of postgres in docker run container as below command	
	docker run -d --name my-postgres-container -p 5555:5432 my-postgres-image


Steps:

1. clone git project
2. run "mvn install"
3. run "mvn package"
4. set JAVA_HOME, SPARK_HOME, HADOOP_HOME
5. program needs two argument in command. Run below command
	spark-submit --class fifaproblem.fifa.analytics.FifaAnalytics --master local[2] <path of jar file> <input file path> <output folde path>\
	
	e.g. 
	spark-submit --class fifaproblem.fifa.analytics.FifaAnalytics --master local[2] C:\Users\prathameshj\workspace\fifa.analytics\target\fifa.analytics-0.0.1-SNAPSHOT.jar C:\Users\prathameshj\Documents\FIFA\data.csv C:\Users\prathameshj\Documents\FIFA\

