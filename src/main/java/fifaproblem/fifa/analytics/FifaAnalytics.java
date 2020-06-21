package fifaproblem.fifa.analytics;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
public class FifaAnalytics {

	public static void main(String[] args) throws AnalysisException {
		if (args.length == 0) {
			System.out.println("No files provided.");
			System.exit(0);
		}
		String input = args[0];
		String output =args[1];

		resolve_2(input,output);
		resolve_3(input,output);
	}

	private static void resolve_3(String input, String output) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");

		Dataset<Row> sqlResult = spark.sql(
				"select T.ID, T.Overall, T.Position, T.Nationality, T.Name, T.Club,( T.Joined ) as Joined,"
						+ " cast((case when T.valUnit = 'M' then T.value * 1000 else T.value end) as DECIMAL(10,2)) as Value ,"
						+ " cast((case when T.wageUnit = 'M' then T.wage * 1000 else T.wage end) as DECIMAL(10,2)) as Wage "
						+ " from ("
						+ " select ID, Overall, Position, Nationality, Name, Club, Joined ,"
						+ " regexp_extract(Value,'[0-9](\\.[0-9]+)?',0) as value,regexp_extract(Value,'[A-Z]',0) as valUnit,"
						+ " regexp_extract(Wage,'[0-9](\\.[0-9]+)?',0) as wage ,regexp_extract(Wage,'[A-Z]',0) as wageUnit"
						+ " from fifa"
						+ " )T"

				);

		sqlResult.show(false);

		try {
			Properties connectionProperties = new Properties();
			connectionProperties.put("user", "postgres");
			connectionProperties.put("password", "postgres");
			sqlResult.write().mode(SaveMode.Append).format("parquet")
			.jdbc("jdbc:postgresql://localhost:5555/fifa", "info.players", connectionProperties);

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}

		System.out.println("Opened database successfully");
	}

	private static void resolve_2(String input, String output) throws AnalysisException {
		question_2a(input, output);
		question_2b(input, output);
		question_2c(input, output);
		question_2d(input, output);
		question_2e(input, output);
		question_2f(input, output);
	}

	private static void question_2b(String input, String output) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");

		Dataset<Row> sqlResult = spark.sql(
				"select T.Club, T.Overall, T.Position "
						+ " from ( select Club, Overall, Position, "
						+ " rank() over (partition by Club,Position order by Overall desc) as rank"
						+ " from fifa "
						+ " where Position in ('RB','CB','RCB','CB','LCB','LB','RM','RWM','LCM','CM','RCM','CM','LM','LWM','RF','CF','LF','ST','GK')"
						+ " and isnotnull(Club)"
						+ " )T  "
						+ " where T.rank = 1"
						+ " order by T.Club asc"
				);

		sqlResult.createTempView("temp");
		sqlResult = spark.sql(
				" select Club, avg(Overall) as overallAvg"
						+ " from temp group by Club order by avg(Overall) desc"
				);

		sqlResult.createTempView("temp1");
		Dataset<Row> sqlResult_final = spark.sql(
				"SELECT Club, overallAvg "
						+" FROM temp1"
						+" WHERE overallAvg = (select max(overallAvg) from temp1) ");

		sqlResult_final.show(false);
		writeToFile(sqlResult_final, output +"\\question_2b.txt");
	}

	private static void question_2e(String input, String output) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");

		Dataset<Row> sqlResult = spark.sql(
				"select Overall, Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes "
						+ "from fifa "
						+ "where Position='GK' "

				);
		sqlResult.createTempView("temp");
		Dataset<Row> sqlResult_final = spark.sql(
				"SELECT Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes "
						+" FROM temp"
						+" WHERE Overall = (select max(Overall) from temp) ");

		sqlResult_final.show(false); 
		getTopAttributes(sqlResult_final, 4, output +"\\question_2e.txt");
	}

	private static void question_2f(String input, String output) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");

		Dataset<Row> sqlResult = spark.sql(
				"select Overall, Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes "
						+ "from fifa "
						+ "where Position='ST' "

				);
		sqlResult.createTempView("temp");
		Dataset<Row> sqlResult_final = spark.sql(
				"SELECT Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes "
						+" FROM temp"
						+" WHERE Overall = (select max(Overall) from temp) ");

		sqlResult_final.show(false); 
		getTopAttributes(sqlResult_final, 5, output +"\\question_2f.txt");
	}

	private static void getTopAttributes(Dataset<Row> sqlResult_final, int count,String file) {
		FileWriter myWriter = null;
		try {

			myWriter = new FileWriter(file);

			Row row = sqlResult_final.collectAsList().get(0);

			Map<String, Integer> map = new HashMap<>();
			for(String column : sqlResult_final.columns()){
				map.put(column, Integer.parseInt(row.getAs(column).toString()));
			}

			map = sortByValue(map);
			int limit=0;
			myWriter.write("Top " + count + " Attributes-----------------\n");
			for(String key:map.keySet()){
				if(limit==count){
					break;
				}
				myWriter.write("Attribute : " + key + "\nAvg(Overall) : " + map.get(key) + "\n\n");
				limit++;
			}
			System.out.println("Successfully wrote to the file.");
		} catch (IOException e) {
			System.out.println("An error occurred.Exception:" + e.toString());
		}
		finally {
			if(myWriter!=null){
				try {
					myWriter.close();
				} catch (IOException e) {
					System.out.println("An error occurred.Exception:" + e.toString());
				}
			}
		}

	}

	private static void writeToFile(Dataset<Row> sqlResult_final, String file) {
		FileWriter myWriter =null;
		try {

			myWriter = new FileWriter(file);

			for(Row row:sqlResult_final.collectAsList()){
				for(String column : sqlResult_final.columns()){
					myWriter.write(column + ":" + row.getAs(column) + "\n");
				}
				myWriter.write("\n\n");
			}

			System.out.println("Successfully wrote to the file.");
		} catch (IOException e) {
			System.out.println("An error occurred.Exception:" + e.toString());
		}
		finally {
			if(myWriter!=null){
				try {
					myWriter.close();
				} catch (IOException e) {
					System.out.println("An error occurred.Exception:" + e.toString());
				}
			}
		}
	}

	public static Map<String, Integer> sortByValue(final Map<String, Integer> wordCounts) {
		return wordCounts.entrySet()
				.stream()
				.sorted((Map.Entry.<String, Integer>comparingByValue().reversed()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	}

	private static void question_2a(String input, String output) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");


		Dataset<Row> sqlResult = spark.sql(
				"select T.Club, count(T.Club) as Score from ( select Club from fifa"
						+ " where `Preferred Foot`= 'Left' and Position in ('RWM','RM','RCM','CM','CAM','CDM','LCM','LM','LWM')"
						+ " and cast(Age as INT) < 30 and isnotnull(Club)"
						+ " )T  "
						+ " group by T.Club "
				);
		sqlResult.createTempView("temp");
		Dataset<Row> sqlResult_final = spark.sql("SELECT Club, Score"
				+" FROM temp"
				+" WHERE Score = (SELECT MAX(Score) FROM temp)");

		sqlResult_final.show(false); 
		writeToFile(sqlResult_final, output +"\\question_2a.txt");
	}

	private static void question_2c(String input, String output) throws AnalysisException {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");

		Dataset<Row> sqlResult1 = spark.sql(
				"select T.Club,"
						+ " cast((case when T.valUnit = 'M' then T.value * 1000 else T.value end) as DECIMAL(10,2)) as Value ,"
						+ " cast((case when T.wageUnit = 'M' then T.wage * 1000 else T.wage end) as DECIMAL(10,2)) as Wage "
						+ " from ("
						+ " select Club, regexp_extract(Value,'[0-9](\\.[0-9]+)?',0) as value,regexp_extract(Value,'[A-Z]',0) as valUnit,"
						+ " regexp_extract(Wage,'[0-9](\\.[0-9]+)?',0) as wage ,regexp_extract(Wage,'[A-Z]',0) as wageUnit"
						+ " from fifa"
						+ " )T"

				);
		sqlResult1.createTempView("temp1");

		Dataset<Row> sqlResult2 = spark.sql(
				"select Club, sum(Value) as Value, sum(Wage) as Wage"
						+ " from temp1"
						+ " group by Club "
				);
		sqlResult2.createTempView("temp2");

		Dataset<Row> sqlResult_final = spark.sql("SELECT Club, Value, case when (select MAX(wage) from temp2) == wage then true else false end as isLargesWageBill"
				+" FROM temp2"
				+" WHERE Value = (SELECT MAX(Value) FROM temp2) ");

		sqlResult_final.show(false); 
		writeToFile(sqlResult_final, output +"\\question_2c.txt");
	}

	private static void question_2d(String input, String output) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL Example")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("sep", ",")
				.option("path",input)
				.load();

		df.createOrReplaceTempView("fifa");
		Dataset<Row> sqlResult3 = spark.sql(
				"select Position, avg(regexp_extract(Wage,'[0-9](\\.[0-9]+)?',0)) AvgWage"
						+ " from fifa"
						+ " group by Position "
				);
		sqlResult3.createTempView("temp3");
		Dataset<Row> sqlResult_final = spark.sql("SELECT Position, AvgWage"
				+" FROM temp3"
				+" WHERE AvgWage = (SELECT MAX(AvgWage) FROM temp3)");
		sqlResult_final.show(false); //for testing
		writeToFile(sqlResult_final, output +"\\question_2d.txt");
	}




}
