import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by ALINA on 29.09.2017.
  */
object gdelt_analyzer {

  def saveDF(df: DataFrame, output: String): Unit = {

    df.repartition(1)
      .write
      .option("delimiter", "\t")
      .csv(output)
  }

  def main(args: Array[String]): Unit = {

    val gdeltFile = args(0)
    val cameoFile = args(1)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Initialize GDELT DataFrame
    val gdeltDF = new GDELTdata(sparkSession, gdeltFile)

    val gdeltUSA = gdeltDF.readCountryDataFrame("'US'")

    //Join with cameo codes dictionary
    val cameoCodes = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(cameoFile)

    cameoCodes.show()

    gdeltUSA.join(cameoCodes,
      gdeltUSA.col("EventCode") === cameoCodes.col("CAMEOcode"), "inner")
      .select("GLOBALEVENTID", "EventCode", "EventDescription", "ActionGeo_CountryCode", "ActionGeo_Lat", "ActionGeo_Long")
      .show()
  }
}
