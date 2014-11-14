package TempCalcs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.stat.Statistics;

import com.clearspring.analytics.util.Lists;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class TempCalcs {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TempCalcs <file>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Spark TempCalcs");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		
		Function<String, TempData> mapper = new Function<String, TempData>() {
            private static final long serialVersionUID = 1L;
            private static final int MISSING = 9999;

            public TempData call(String line)
                    throws Exception {
                String year = line.substring(15,19);
                int airTemperature;
                
                if (line.charAt(87) == '+') {
                    //parseInt doesn't like leading plus signs
                    airTemperature = Integer.parseInt(line.substring(88,92));
                } else {
                    airTemperature = Integer.parseInt(line.substring(87, 92));
                }
                
                String quality = line.substring(92, 93);
                return new TempData(airTemperature, quality, year);
            }
        };		
		
		Function<TempData, Boolean> filterFunc = new Function<TempData, Boolean>() {
            public Boolean call(TempData v1) throws Exception {
                return v1.getQuality().matches("[019]");
            }
        };
        
        PairFunction<TempData, String, Integer> pairMapper = new PairFunction<TempData, String, Integer>() {
            public Tuple2<String, Integer> call(TempData t) throws Exception {
                return new Tuple2<String, Integer>(t.getYear(), t.getTemp());
            }
        };
		
		Function<Integer, AvgCount> createAccumulator = new Function<Integer, AvgCount>(){
            public AvgCount call(Integer v1) throws Exception {
                return new AvgCount(v1, 1);
            }
		};
		
		Function2<AvgCount, Integer, AvgCount> addCount = new Function2<AvgCount, Integer, AvgCount>() {
            public AvgCount call(AvgCount avg, Integer val) throws Exception {
                int newAcc = avg.getAccumulator() + val;
                avg.setAccumulator(new Integer(newAcc));
                int newCount = avg.getCount() + 1;
                avg.setCount(newCount);
                return avg;
            }
        };
        
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
                v1.setAccumulator(v1.getAccumulator() + v2.getAccumulator());
                v1.setCount(v1.getCount() + v2.getCount());
                return v1;
            }
        };

        JavaRDD<TempData> tempData = lines.map(mapper);
        JavaRDD<TempData> filteredTemps = tempData.filter(filterFunc);
        JavaPairRDD<String, Integer> airTemps = filteredTemps.mapToPair(pairMapper);
		JavaPairRDD<String, AvgCount> avgCounts = airTemps.combineByKey(createAccumulator, addCount, combine);
		
		JavaPairRDD<String, Integer> maxVals = airTemps.reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.max(v1, v2);
            }
		});
		
		JavaPairRDD<String, Integer> minVals = airTemps.reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.min(v1, v2);
            }
        });
		
		List<Tuple2<String, String>> rddList = Lists.newArrayList();
		
		String averageKey = "Average Values\n---------------------------------\n";
		String averageValue = "";
		Map<String, AvgCount> countMap = avgCounts.collectAsMap();
		for (Entry<String, AvgCount> entry : countMap.entrySet()) {
//		    System.out.println(entry.getKey().toString() + ":" + entry.getValue().avg() + "\n");
		    averageValue = averageValue.concat(entry.getKey().toString() + ":" + entry.getValue().avg() + "\n");
		}
//		System.out.println("\n\nAverageValue:" + averageValue + "\n\n");
		Tuple2<String, String> averageTuple = new Tuple2<String, String>(averageKey, averageValue);
		rddList.add(averageTuple);
		
		String maxKey = "Maximum Values\n---------------------------------\n";
        String maxValue = "";
        Map<String, Integer> maxMap = maxVals.collectAsMap();
        for (Entry<String, Integer> entry : maxMap.entrySet()) {
            maxValue = maxValue.concat(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        Tuple2<String, String> maxTuple = new Tuple2<String, String>(maxKey, maxValue);
        rddList.add(maxTuple);
        
        String minKey = "Minimum Values\n---------------------------------\n";
        String minValue = "";
        Map<String, Integer> minMap = minVals.collectAsMap();
        for (Entry<String, Integer> entry : minMap.entrySet()) {
            minValue = minValue.concat(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        Tuple2<String, String> minTuple = new Tuple2<String, String>(minKey, minValue);
        rddList.add(minTuple);
        
        JavaPairRDD<String, String> outputRDD = ctx.parallelizePairs(rddList);
        
//        System.out.println(averageTuple);
//        System.out.println(minTuple);
//        System.out.println(maxTuple);
        outputRDD.saveAsTextFile("/user/root/Project/TempCalcs");

	}
}
