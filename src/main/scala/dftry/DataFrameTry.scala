package dftry

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
/**
  * Created by parshwa on 17/1/17.
  */
object DataFrameTry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new SQLContext(sc)
    val peopleDemographirCSVPath = "/home/parshwa/SparkHandsOn/src/main/resources/person-demo.csv"
    val peopleDemographicReadDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(peopleDemographirCSVPath)
    println("People Demographic Table")
    peopleDemographicReadDF.show()
    val peopleHealthCSVPath = "/home/parshwa/SparkHandsOn/src/main/resources/person-health.csv"
    val peopleHealthReadDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(peopleHealthCSVPath)
    println("People Health Table")
    peopleHealthReadDF.show()

    val peopleInsuranceCSVPath = "/home/parshwa/SparkHandsOn/src/main/resources/person-insurance.csv"
    val peopleInsuranceReadDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema","true")
      .option("header","true")
      .load(peopleInsuranceCSVPath)
    println("People Insurance Table")
    peopleInsuranceReadDF.show()

    val personDF = peopleHealthReadDF
      .join(broadcast(peopleDemographicReadDF),
        peopleDemographicReadDF("id")===peopleHealthReadDF("id"),
        "right_outer"
      ).drop(peopleDemographicReadDF("id"))

    println("PersonDF:- JOIN of Health And Demographic Table")
    personDF.show()



    val personDFFinal = personDF
      .join(peopleInsuranceReadDF,
        personDF("id") === peopleInsuranceReadDF("id"),
        "right_outer"
      ).drop(personDF("id"))

    println("PersonDFFinal:- JOIN of all three tables")
    personDFFinal.show()

    // Age less than 50
    val ageLessThan50DF = personDF.filter(personDF("age")<50)
    ageLessThan50DF.show()
    ageLessThan50DF
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("src/resources/AgeLessThan50")

    // Payer Total Amount
    val payerAmountDF = peopleInsuranceReadDF.groupBy("payer").agg(sum("amount"))
    payerAmountDF.show()
    payerAmountDF
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("src/resources/PayerSum")
  }
}
