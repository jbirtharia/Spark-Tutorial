package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * @author JayendraB
 * Created on 25/03/23
 */
public class Test {

    public static void main(String[] args) {

        // Create Spark Configuration
        SparkConf conf = new SparkConf().setAppName("ReadParquetFileExample").setMaster("local[*]");

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("ReadParquetFileExample")
                .config(conf)
                .getOrCreate();

        // Read Parquet file into a DataFrame
        Dataset<Row> df = sparkSession.read().parquet("/Users/jayendrabirtharia/Downloads/parquet-testing-master/data/alltypes_dictionary.parquet");

        //df.filter(col("bool_col").equalTo("true")).show();
        Dataset<Row> df_temp = sparkSession.read().parquet("/Users/jayendrabirtharia/Downloads/parquet-testing-master/data/alltypes_plain.parquet");

        // Display the DataFrame
        df.show();

        Dataset<Row> mergeDataset = df.join(df_temp, "id");

        df_temp.show();

        mergeDataset.show();
    }
}
