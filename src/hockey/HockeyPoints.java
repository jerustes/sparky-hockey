package hockey;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class HockeyPoints {

	public static void main(String[] args) throws Exception {
		// String inputFile = args[0];
		// String outputFile = args[1];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("hockeyPoints"); // single thread
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load input data.
		JavaRDD<String> input = sc.textFile("input.txt");
		// Split up into lines (/n).
		JavaRDD<Player> players = input.flatMap(new FlatMapFunction<String, Player>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8405861725911108925L;

			@Override
			public Iterator<Player> call(String arg0) throws Exception {
				String[] items = arg0.split(", ");
				Player player = new Player();
				player.Name = items[0];
				String[] values = items[1].split(" ");
				int goals = Integer.parseInt(values[0]);
				int assists = Integer.parseInt(values[1]);
				player.goals = goals;
				player.assists = assists;
				// List<Jugador> jugadores = new ArrayList<>();
				// jugadores.add(jugador);
				// return jugadores.iterator();
				Player[] res = { player };
				return Arrays.asList(res).iterator();
			}
		});

		JavaRDD<Player> pm = players.map(new Function<Player, Player>() {

			@Override
			public Player call(Player arg0) throws Exception {
				arg0.points = arg0.goals + arg0.assists;
				return arg0;
			}
		});

		
		 JavaPairRDD<String, Integer> res = players.mapToPair(new PairFunction<Player,
				 String, Integer>() {
		 
			 @Override public Tuple2<String, Integer> call(Player arg0) throws Exception {
				 return new Tuple2<String, Integer>(arg0.Name, arg0.goals + arg0.assists);
			 } 
		 });

		 
		 JavaPairRDD<String, Integer> res2 = res.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return new Integer(arg0 + arg1);
			}
		});

		/*
		 * JavaPairRDD<Integer, String> res = players.mapToPair(new PairFunction<Player, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Player arg0) throws Exception {
				return new Tuple2<Integer, String>(arg0.goals + arg0.assists, arg0.Name);

			}
		});*/

		//res.sortByKey();


		
		/* 
		JavaPairRDD<Integer, String> res2 = res.filter(new Function<Tuple2<Integer, String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, String> arg0) throws Exception {
				if (arg0._1 < 10)
					return false;
				return true;
			}
		});
		*/
		
		JavaRDD<String> res3 = res2.flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, Integer> arg0) throws Exception {
				return Arrays.asList(new String[] { arg0._1 + "::" + arg0._2() }).iterator();
			}
		});

/*		JavaPairRDD<String, Integer> out = res2.reduce(new Function2<String, String>() {
			
			@Override
			public String call(String arg0, String arg1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		})
	*/	
		// JavaPairRDD<String, List<Integer> > points = lines.mapToPair(new
		// PairFunction<String, Integer, Integer>)
		
		
		res3.saveAsTextFile("output" + (new Random()).nextInt() + ".txt");
		
		
		
		 /* // Transform into word and count. JavaPairRDD<String, Integer> counts =
		 * words.mapToPair(new PairFunction<String, String, Integer>() { public
		 * Tuple2<String, Integer> call(String x) { return new Tuple2(x, 1); }
		 * }).reduceByKey(new Function2<Integer, Integer, Integer>() { public Integer
		 * call(Integer x, Integer y) { return x + y; } }); // Save the word count back
		 * out to a text file, causing evaluation.
		 * counts.saveAsTextFile("outputFile.txt");
		 */

	}
}
