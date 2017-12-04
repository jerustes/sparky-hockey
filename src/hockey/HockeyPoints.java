package hockey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class HockeyPoints {

	public static void main(String[] args) throws Exception {
        // String inputFile = args[0];
        // String outputFile = args[1];
        // Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("hockeyPoints").setMaster("local[1]"); //single thread
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    // Load input data.
        JavaRDD<String> input = sc.textFile("input.txt");
        // Split up into lines (/n).
        JavaRDD<Jugador> players = input.flatMap(new FlatMapFunction<String, Jugador>() {

			@Override
			public Iterator<Jugador> call(String arg0) throws Exception {
				String[] items = arg0.split(", ");
				Jugador jugador = new Jugador();
				jugador.Name = items[0];
				String[] values = items[1].split(" ");
				int goals = Integer.parseInt(values[0]);
				int assists = Integer.parseInt(values[1]);
				jugador.goals = goals;
				jugador.asists = assists;
				//List<Jugador> jugadores = new ArrayList<>();
				//jugadores.add(jugador);
				//return jugadores.iterator();
				Jugador[] res = {jugador};
				return Arrays.asList(res).iterator();
			}
		}  );
        //JavaPairRDD<String, List<Integer> > points = lines.mapToPair(new PairFunction<String, Integer, Integer>) 
        
        lines.saveAsTextFile("output.txt");
        /*
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) {
                        return x + y;
                }
        });
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile("outputFile.txt");
    	*/
    
	}
}
