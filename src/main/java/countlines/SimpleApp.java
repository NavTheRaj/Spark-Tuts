package countlines;

/* SimpleApp.java */
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static final String HDFS_PATH = "hdfs://localhost:9000/user/navraj.khanal/spark-data/";

    public static void main(String[] args) {


        String logFile = "dummy.csv"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().
                              appName("SimpleApp")
                             .config("spark.master", "local")
                             .getOrCreate();

        Dataset <String> logData = spark.read().textFile(HDFS_PATH + logFile).cache();

//        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}