/* SimpleApp.java */
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

public class SimpleCorrelation {
  public static void main(String[] args) {
	if (args.length != 2) {
		throw new IllegalArgumentException("Requires 2 arguments");
	}
    String inputPath = args[0]; // Should be some file on your system
    String outputPath = args[1];
    
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Vector> pairData = sc.textFile(inputPath)
    		.cache().
    		map(new Function<String, Vector>(){

				public Vector call(String pairLine) throws Exception {
					
					String[] stringValues = pairLine.split(",");
					double[] values = parseDoubles(stringValues);
					Vector outVec = Vectors.dense(values);
					return outVec;
				}
    			
    		});
    
    Matrix correlationVal = Statistics.corr(pairData.rdd(), "pearson");
    String matString = betterMatToString(correlationVal);
    Writer writer = null;

    try {
        writer = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(outputPath), "utf-8"));
        writer.write(matString);
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
       try {writer.close();} catch (Exception ex) {}
    }
    System.out.println("Correlation Matrix: \n" + matString);
  }
  
  private static double[] parseDoubles(String[] inVals) {
	double[] nums = new double[inVals.length];
	for (int i = 0; i < nums.length; i++) {
		nums[i] = Double.parseDouble(inVals[i]);
	}
	return nums;
  }
  
  private static String betterMatToString(Matrix m) {
	  Double[][] matArr = new Double[m.numRows()][m.numCols()];
	  for (int i = 0; i < m.numRows(); i++) {
		  for (int j = 0; j < m.numCols(); j++) {
			  matArr[i][j] = m.apply(i, j);
		  }
	  }
	  StringBuilder formatBuilder = new StringBuilder();
	  StringBuilder builder = new StringBuilder();
	  for (int i = 0; i < matArr.length; i++) {
		  formatBuilder.append("%").append(i+1).append("$-10f\t");
	  }
	  String formatString = formatBuilder.toString();
	  System.out.println("FormatString: "+formatString);
	  for (int i = 0; i < matArr.length; i++) {
		  builder.append(String.format(formatString, matArr[i])).append("\n");
	  }
	  return builder.toString();
  }
}