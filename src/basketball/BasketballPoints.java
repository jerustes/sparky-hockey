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

import basketball.BPlayer;
import hockey.Player;


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
		JavaRDD<BPlayer> lines = input.flatMap(new FlatMapFunction<String, BPlayer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8405861725911108925L;

			@Override
			public Iterator<BPlayer> call(String arg0) throws Exception {
				String[] items = arg0.split(", ");
				BPlayer bballer = new BPlayer();
				bballer.Name = items[0];
				String[] positiveValues = items[1].split(" ");
				//Game stats:
				int points = Integer.parseInt(positiveValues[0]);
				int rebounds = Integer.parseInt(positiveValues[1]);
				int assists = Integer.parseInt(positiveValues[2]);
				int steals = Integer.parseInt(positiveValues[3]);
				int blocks = Integer.parseInt(positiveValues[4]);
				//player attr initialize
				bballer.points = points;
				bballer.rebounds = points;
				bballer.assists = assists;
				bballer.steals = steals;
				bballer.blocks = blocks;
				// ?????
				String[] negativeValues = items[2].split(" ");
				int missedFG = Integer.parseInt(negativeValues[0]);
				int missedFT = Integer.parseInt(negativeValues[1]);
				int turnovers = Integer.parseInt(negativeValues[2]);
				int fouls = Integer.parseInt(negativeValues[3]);
				int ejections = Integer.parseInt(negativeValues[4]);
				BPlayer[] res = { bballer };
				return Arrays.asList(res).iterator();
			}
		});
		
		/*-------------
		JavaRDD<BPlayer> res = lines.map(new Function<BPlayer, BPlayer>() {

			@Override
			public BPlayer call(BPlayer plyr) throws Exception {
			
				return plyr;
			}
		});
		*/
		
		JavaPairRDD<String, Integer> res1 = lines.mapToPair(new PairFunction<BPlayer,
				String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(BPlayer bp) throws Exception {

				return new Tuple2<String, Integer>(bp.Name, ((bp.points + bp.rebounds + bp.assists + bp.steals + bp.blocks) - 
						(bp.missedFG + bp.missedFT + bp.turnovers + bp.fouls + bp.ejections)) );
			} 
      	});
		
		 
		JavaPairRDD<String, Integer> res2 = res1.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer player0, Integer player1) throws Exception {
				return new Integer(player0 + player1);
			}
		});

		
		JavaRDD<String> output = res2.flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, Integer> player) throws Exception {
				return Arrays.asList(new String[] { player._1 + "::" + player._2() }).iterator(); //WTF ._2() ???????
			}
		});

		
		output.saveAsTextFile("output" + (new Random()).nextInt() + ".txt");


	}
}
