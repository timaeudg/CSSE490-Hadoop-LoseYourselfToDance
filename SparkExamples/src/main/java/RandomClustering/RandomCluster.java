package RandomClustering;

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

import java.util.HashMap;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

import com.clearspring.analytics.util.Lists;

/**
 * Classification and regression using decision trees.
 */
public final class RandomCluster {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Vector> randomRDD = RandomRDDs.normalJavaVectorRDD(sc, 10000, 2).cache();
        
        KMeansModel bestModel = null;
        double bestWSSSE = -1.0;
        
        int maxClusters = 5;
        int maxIterations = 41;
        for (int clusters = 2; clusters <= maxClusters ; clusters++) {
            for (int iterations = 1; iterations <= maxIterations ; iterations += 10) {
                System.out.println("\n\nCurrently at: ");
                System.out.println("clusters: " + clusters);
                System.out.println("iterations : " + iterations + "\n\n");
                KMeansModel clusteredModel = KMeans.train(randomRDD.rdd(), clusters, iterations);
                
                double WSSSE = clusteredModel.computeCost(randomRDD.rdd());
                if (bestModel == null || bestWSSSE == -1.0 || WSSSE < bestWSSSE) {
                    bestModel = clusteredModel;
                    bestWSSSE = WSSSE;
                }
            }
        }
        System.out.println("Best Within Set Sum of Squared Errors: " + bestWSSSE);
        System.out.println("Best Model: \n" + RandomCluster.printBestModel(bestModel));
    }
    
    public static String printBestModel(KMeansModel model) {
        String outputString = "";
        for (Vector vec : model.clusterCenters()) {
            String center = "(";
            for (double value : vec.toArray()) {
                center += value + ",";
            }
            center += ")";
            outputString += "Cluster center at: " + center + "\n";
        }
        return outputString;
    }
}