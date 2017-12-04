package basketball;

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

public class BasketballPoints {

	public static void main(String[] args) throws Exception {
		// String inputFile = args[0];
		// String outputFile = args[1];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("basketballPoints"); // single thread
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load input data.
		JavaRDD<String> input = sc.textFile("ballinput.txt");
		// Split up into lines (/n).
		JavaRDD<Player> lines = input.flatMap(new FlatMapFunction<String, Player>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8405861725911108925L;

			@Override
			public Iterator<Player> call(String arg0) throws Exception {
				String[] items = arg0.split(", ");
				Player bballer = new Player();
				player.Name = items[0];
				String[] positiveValues = items[1].split(" ");
				int points = Integer.parseInt(positiveValues[0]);
				int rebounds = Integer.parseInt(positiveValues[1]);
        int assists = Integer.parseInt(positiveValues[2]);
				int steals = Integer.parseInt(positiveValues[3]);
        int blocks = Integer.parseInt(positiveValues[4]);
				bballer.points = points;
        bballer.rebounds = points;
				bballer.assists = assists;
        bballer.steals = steals;
        bballer.blocks = blocks;
        String[] negativeValues = items[2].split(" ");
        int missedFG = Integer.parseInt(negativeValues[0]);
				int missedFT = Integer.parseInt(negativeValues[1]);
        int turnovers = Integer.parseInt(negativeValues[2]);
				int fouls = Integer.parseInt(negativeValues[3]);
        int ejections = Integer.parseInt(negativeValues[4]);
				Player[] res = { player };
				return Arrays.asList(res).iterator();
			}
		});

		
		 JavaPairRDD<String, Integer> players = lines.mapToPair(new PairFunction<Player,
		 String, Integer>() {
		 
		 @Override public Tuple2<String, Integer> call(Player b) throws Exception {
		 return new Tuple2<String, Integer>(b.Name, (b.points + b.rebounds + b.assists + b.steals + b.blocks) - 
                (b.missedFG + b.missedFT + b.turnovers + b.fouls + b.ejections));
		 
		  } 
      });

		 
		 JavaPairRDD<String, Integer> result = players.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer player0, Integer player1) throws Exception {
				return new Integer(player0 + player1);
			}
		});

		
		JavaRDD<String> output = result.flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, Integer> player) throws Exception {
				return Arrays.asList(new String[] { player._1 + "::" + player._2() }).iterator();
			}
		});

		
		output.saveAsTextFile("output" + (new Random()).nextInt() + ".txt");


	}
}
