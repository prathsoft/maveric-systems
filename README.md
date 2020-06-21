### Output:
***
1. question_2a : Club : KFC Uerdingen 05, Galatasaray SK,Zaragoza, Sevilla FC, Borussia Dortmund

   Approach    : Applied filter for age and mid filder as given , skipped null clubs , aggregated count by clubs. Taken max values by count	
	 
2. question_2b : Club:Real Madrid

   Approach    : calculated max overall ranking for Club, Position where postion for 4-4-2 and GK are considered as given then grouped by Club . Taken max average.
		 
3. question_2c : Club:Paris Saint-Germain. This team has not largest wage bill

   Approach    : Extracted numbers from Value and Wage. converted every value in THoudsand Euros. Calculated sum of value and wage for every CLub. Taken highest sumed Valued. Compared sumed Wage for Club against max summed wage.
		 
4. question_2d : Position:RF

   Approach    : Extracted numbers from Wage. Calculated maximum average for each position against wages.

5. question_2e : Attributes : GKReflexes, Reactions, GKDiving, GKPositioning

   Approach    : calculated average skills for best goalkeeper (GK). Then transposed the skill output to select top 4 valued skills.
   
6. question_2f : Reactions, ShotPower, Positioning, Composure, Jumping

   Approach    : calculated average skills for best stricker (ST). Then transposed the skill output to select top 5 valued skills.	
### Assumptions:
***
1. Java 8 is installed
2. Spark latest version is installed
3. docker is installed
4. maven 3 is installed

### Given:
***
1. project code 
2. init.sql
3. Dockerfile

### Postgres installation:
***
1. Go to folder named 'postgres' and Run below command:
	docker build -t my-postgres-image .
	
2. Post installation of postgres in docker run container as below command	
	docker run -d --name my-postgres-container -p 5555:5432 my-postgres-image
	docker exec -it my-postgres-container bash
	psql -U postgres
	
3. create database fifa;
4. \connect fifa
5. create schema info;

### Steps:
***
1. clone git project
2. run "mvn install"
3. run "mvn package"
4. set JAVA_HOME, SPARK_HOME, HADOOP_HOME
5. program needs two argument in command. Run below command
	spark-submit --class fifaproblem.fifa.analytics.FifaAnalytics --master local[2] <path of jar file> <input file path> <output folde path>\
	
	e.g. 
	spark-submit --class fifaproblem.fifa.analytics.FifaAnalytics --master local[2] C:\Users\prathameshj\workspace\fifa.analytics\target\fifa.analytics-0.0.1-SNAPSHOT.jar C:\Users\prathameshj\Documents\FIFA\data.csv C:\Users\prathameshj\Documents\FIFA\


