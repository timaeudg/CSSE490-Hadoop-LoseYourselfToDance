/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.linalg.*;

public class SimpleCorrelation {
  public static void main(String[] args) {
	if (args.length != 1) {
		throw new IllegalArgumentException("No input file");
	}
    String inputPath = args[0]; // Should be some file on your system
    
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
    System.out.println("Correlation Matrix: \n" + correlationVal.toString());
  }
  
  private static double[] parseDoubles(String[] inVals) {
	double[] nums = new double[inVals.length];
	for (int i = 0; i < nums.length; i++) {
		nums[i] = Double.parseDouble(inVals[i]);
	}
	return nums;
  }
}