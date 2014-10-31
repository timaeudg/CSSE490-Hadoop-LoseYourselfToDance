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

import scala.Tuple2;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

/**
 * Classification and regression using decision trees.
 */
public final class ExampleDecisionTreeClassification {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: JavaDecisionTree <libsvm format data file>");
            System.exit(1);
        }
        String datapath = args[0];
        SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load and parse the data file.
        // Cache the data since we will use it again to compute training error.
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

        // Set parameters.
        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 100;

        // Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo,
                impurity, maxDepth, maxBins);

        // Evaluate model on training instances and compute training error
        JavaPairRDD<Double, Double> predictionAndLabel = data
                .mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

                    public Tuple2<Double, Double> call(LabeledPoint p) throws Exception {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                    
                });
        Double trainErr = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {

            public Boolean call(Tuple2<Double, Double> pl) throws Exception {
                return !pl._1().equals(pl._2());
            }
            
        }).count() / data.count();
        System.out.println("Training error: " + trainErr);
        System.out.println("Learned classification tree model:\n" + model);
    }
}