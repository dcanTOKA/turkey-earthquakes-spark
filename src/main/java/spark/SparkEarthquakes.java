package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class SparkEarthquakes {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "src/main/resources/hadoop-common-2.2.0-bin-master/bin");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final String VALUE = "value";

        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingEarthquakes")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 8989)
                .load();

        Dataset<String> data = rawData.as(Encoders.STRING());

        data.printSchema();

        Dataset<Row> reconstructedData = data.select(functions.split(functions.col(VALUE), ",").getItem(0).as("tarih"),
                functions.split(functions.col(VALUE), ",").getItem(1).as("saat"),
                functions.split(functions.col(VALUE), ",").getItem(2).as("enlem"),
                functions.split(functions.col(VALUE), ",").getItem(3).as("boylam"),
                functions.split(functions.col(VALUE), ",").getItem(4).as("derinlik"),
                functions.split(functions.col(VALUE), ",").getItem(5).as("buyukluk"),
                functions.split(functions.col(VALUE), ",").getItem(6).as("yer"),
                functions.split(functions.col(VALUE), ",").getItem(7).as("sehir")).drop(VALUE);

        reconstructedData.printSchema();

        Dataset<Row> groupByLocation = reconstructedData.groupBy(reconstructedData.col("yer")).count();

        Dataset<Row> sortedGroupByLocation = groupByLocation.orderBy(functions.col("count").desc());

        sortedGroupByLocation.writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", false)
                .start()
                .awaitTermination();

    }
}

