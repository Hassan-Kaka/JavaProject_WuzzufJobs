package net.codejava;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WuzzufJopsDAO {
    public Dataset<Row> getDataset() {
        final SparkSession sparkSession = SparkSession.builder().appName("Wuzzuf Jobs Project").master("local[6]")
                .getOrCreate();
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        Dataset<Row> wuzzufDF = dataFrameReader.csv("Wuzzuf_Jobs.csv");
        wuzzufDF = wuzzufDF.select("Title", "Company", "Location", "Type", "Level", "YearsExp", "Country", "Skills");

        final Dataset<Row> wuzzufDFNoNullDF = wuzzufDF.na().drop();
        final Dataset<Row> wuzzufDFNoDuplicates = wuzzufDFNoNullDF.dropDuplicates();

        return wuzzufDFNoDuplicates;
    }
}
