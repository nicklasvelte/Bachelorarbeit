package main.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

//create the right (value, value) tuples
class valRDD implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		String strRddVal = String.valueOf(orgPair._2());
		
		if (spRed.boolChange == true){
			spRed.oldkey = strRddVal;
	    	spRed.boolChange = false;
		}
		if (spRed.oldkey != strRddVal && spRed.oldkey != ""){
			return new Tuple2(spRed.oldkey, strRddVal);
		}
		else{
			spRed.oldkey = strRddVal;
		}
		return new Tuple2(0,0);
	}
}

//filter by key with the given keyPointer
class filterRDDs implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		String strRddPos = String.valueOf(orgPair._1());
		int intRddPos = Integer.valueOf(strRddPos);
		if (intRddPos == spRed.keyPointer){
			return new Tuple2(intRddPos, String.valueOf(orgPair._2()));
		}
		return new Tuple2(0,0);
	}
}

//get the first n items of the key-group to match the max-key-limit
class limitRDDs implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		if (spRed.maxKeyPointer <= spRed.maxItems){
			spRed.maxKeyPointer = spRed.maxKeyPointer + 1;
			return new Tuple2(0,0);
		}
		return new Tuple2(String.valueOf(orgPair._1()), String.valueOf(orgPair._2()));
	}
}


//return the dataset with the key firstVal
class getFirst implements Function<Tuple2<Integer, String>, Boolean>{
    public Boolean call(Tuple2<Integer, String> keyValue) {
	      String s = String.valueOf(keyValue._2());
	      return (s.equals(spRed.firstVal));
	    }
}

//return biggest key
class lastKey implements VoidFunction<Tuple2<Integer, String>>{
  public void call(Tuple2<Integer, String> keyValue) {
	      String s = String.valueOf(keyValue._1());
	      int key = Integer.valueOf(s);
	      if (key > spRed.maxKey){
	    	  spRed.maxKey = key;
	      }
  }
}

//replace key and value.
class swapKV implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		return new Tuple2(String.valueOf(orgPair._2()), String.valueOf(orgPair._1()));
	}
} 

//rename duplicates.
class markDuplicates implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		String strVal = String.valueOf(orgPair._2());
		String resVal = strVal;
		String[] dupArr = spRed.dupVals.split(";");
		List<String> dupList = Arrays.asList(dupArr);
		if (dupList.contains(strVal)){
			resVal = strVal + "_";
		}
		else{
			spRed.dupVals = spRed.dupVals + strVal + ";";
		}
		return new Tuple2(orgPair._1(), resVal);
	}
} 

//unmark duplicates.
class unmarkDuplicates implements PairFunction<Tuple2<Integer, String>,Integer, String>{
	public Tuple2<Integer, String> call(Tuple2<Integer, String> orgPair) {
		String strKey = String.valueOf(orgPair._1());
		String strVal = String.valueOf(orgPair._2());
		
		if (strKey.substring(strKey.length()-1).equals("_")){
			strKey = strKey.substring(0, strKey.length()-1);
		}
		if (strVal.substring(strVal.length()-1).equals("_")){
			strVal = strVal.substring(0, strVal.length()-1);
		}
		
		return new Tuple2(strKey, strVal);
	}
} 

public class spRed {

	public static int keyPointer;
	public static int maxKeyPointer;
	public static int maxKey;
	public static int maxVal;
	public static int maxItems;
	public static String oldkey;
	public static String output;
	public static String firstVal;
	public static String dupVals;
	public static Boolean boolChange;
	
	
	public static void main(String[] args) throws IOException {

		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-ITEM_SET_MAX")) {
				maxItems = calcMaxListItems(Integer.parseInt(args[++i]));
			}
		}
		
		System.out.println("START SPRED");
		BufferedWriter out;
		OutputStream outStream = System.out;
		out = new BufferedWriter(new OutputStreamWriter(outStream));
		

		BufferedReader in;
		InputStream inStream = System.in;		
		in = new BufferedReader(new InputStreamReader(inStream));
		
		String result = "";
		try {
			String line = "";

			while ((line = in.readLine()) != null) {
				result = result + line;
			}
	    } finally {
	        in.close();
	        inStream.close();
	    }
		
		List<String> input = Arrays.asList(result.split("\n"));
		
		//fetch Inputdata and turn it into a RDD(words) and then into a PairRDD (wordPair); set up spark driver
	    SparkConf conf = new SparkConf().setAppName("Red")
	            .setMaster("yarn-cluster");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> lines = sc.parallelize(input);	    
	    JavaRDD<Tuple2<Integer, String>> words = lines.map(
		  	      new Function<String, Tuple2<Integer, String>>() {
		  	        public Tuple2<Integer, String> call(String s) {
		  	        	String[] arr = s.split("\t");
		  	        	String i = arr[0];
		  	        	String j = arr[1];
		  	          return new Tuple2(i, j);
		  	        }
		  	      }
		  	    );
	    JavaPairRDD<Integer, String> wordPair =  JavaPairRDD.fromJavaRDD(words);
	    
	    //get the biggest key
	    wordPair.foreach(new lastKey());
	    
	    //generate a RDD with a (0, 0) tuple
	    JavaRDD<Tuple2<Integer, String>> zeroTuple = lines.map(
		  	      new Function<String, Tuple2<Integer, String>>() {
		  	        public Tuple2<Integer, String> call(String s) {
		  	          return new Tuple2(0, 0);
		  	        }
		  	      }
		  	    );
	    JavaPairRDD<Integer, String> zeroRDD =  JavaPairRDD.fromJavaRDD(zeroTuple);
	    
	    JavaPairRDD<Integer, String> resultRDD = null;
	    JavaPairRDD<Integer, String> wordPair_filtered = null;
	    
	    //walk through the datasets by key
	    for (int i = 0; i < maxKey + 1; i++){
	    	keyPointer = i;
	    	oldkey = "";
	    	//filteres the datasets by keyPointer. Datasets with the key i go to wordPair_filtered. The rest stays in wordPair.
	    	wordPair_filtered = wordPair.mapToPair(new filterRDDs());
	    	wordPair = wordPair.subtractByKey(wordPair_filtered);
	    	wordPair_filtered = wordPair_filtered.subtractByKey(zeroRDD); //deletes 0-tuples which are inserted by filterRDDs()
		    
	    	//get number of values with the same key
		    maxVal = (int) wordPair_filtered.keys().count();
		    boolChange = false;
	    	
		    while (maxVal > maxItems){
			    maxKeyPointer = 0;
			    maxVal = (int) wordPair_filtered.keys().count();
			    wordPair_filtered = wordPair_filtered.mapToPair(new limitRDDs());
		    	wordPair_filtered = wordPair_filtered.subtractByKey(zeroRDD);
		    }
		    dupVals = "";
		    wordPair_filtered = wordPair_filtered.mapToPair(new markDuplicates());
		    
		    //walk through the datasets with the same keys
	    	for (int j = 1; j < maxVal; j++){
		    	
	    		//orders wordPair_filtered by value (sometimes the RDDs lose their order!)
		    	wordPair_filtered = wordPair_filtered.mapToPair(new swapKV()).sortByKey().mapToPair(new swapKV());
		    	
		    	//bring the values in the right (value, value) tuples
		    	resultRDD = wordPair_filtered.mapToPair(new valRDD()).cache();
	    		resultRDD = resultRDD.subtractByKey(zeroRDD).cache(); //deletes 0-tuples which are inserted by valRDD()
		    	
	    		//deletes first dataset
	    		firstVal = wordPair_filtered.values().first();
	    		wordPair_filtered = wordPair_filtered.subtract(wordPair_filtered.filter(new getFirst())).cache();
	    		
		    	resultRDD = resultRDD.mapToPair(new unmarkDuplicates());
		    	
	    		//build output
	    		resultRDD.foreach(new VoidFunction<Tuple2<Integer, String>>(){
	    		    public void call(Tuple2<Integer, String> keyValue) throws IOException {
	    		    	String result = "";
	    		    	output = "";
	    		    	result = result + keyValue._1() + '\t' + keyValue._2() + '\n';
	    				output = result;
	    		    }
	    		});
			    out.write(output);
		    	boolChange = true;
	    	}
	    }
	    
	}    
	private static int calcMaxListItems(int x) {
		return (int) (Math.sqrt(8L * x + 1) + 1) / 2;
	}
}
