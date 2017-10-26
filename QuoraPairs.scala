package linReglogReg

import math.log
import java.util.Date
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
//import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, explode, length, split, substring}
import org.apache.spark.sql.DataFrame
// ML Feature Creation, Tuning, Models, and Model Evaluation
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.mllib.linalg.{ Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.regression.{LinearRegression}

/* 
 * Initial Notebook for IFT 472 Project, assumes Quora Dataset availability.
 * The objective is to follow the initial process outlined by Anokas' kernel at
 * https://www.kaggle.com/anokas/data-analysis-xgboost-starter-0-35460-lb/notebook
 * 
 * Sam Hughel
 * 10/18/2017
 * 
 * We will take two questions, compare strings for possible predictor features.
 * The test data is formatted as:
 * 
 *  ID,  QID1,  QID2,  question1,  question2,  is_duplicate
 *  0     1      2     SomeString  SomeString       0
 *  
 * (Target Variable = is_duplicate)
 * 
 */

object QuoraPairs extends App{
  
 /* 
  * One Possibility:
  * 
  * 1. Compare strings for words, compare for characters (as in original Kernel)
  * 2. Find the difference between strings (score),
  * 3. Find the common characters (number)
  * 4. Find the unique characters (number)
  * 5. Assemble 1-4 into dataframe, 
  * 6. Fit training data
  * 7. run Spark ML logistic regression
  *  
  */
  
  // Spark Session 
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder
            .master("local[*]")
            .appName("Dataframes 9-17")
            .getOrCreate()
    import spark.implicits._
    println(s" Spark version , ${spark.version} ")
  // End Spark Session 
    
  // Spark.read dataframe (Viewing Only)
    val dframe = spark.read
        .format("csv") 
        .option("header", "true") //start reading after the header
        .option("mode", "DROPMALFORMED") // drop malformed CSV
        .load("C:/Users/organ/Desktop/IFT 333/quoratrain.csv")
               
       dframe.show()
       dframe.printSchema()
  // End Spark.read DF
       
  // Spark Context DF (Pref. for editing)      
  val sc = spark.sparkContext
  val fn2 = "C:/Users/organ/Desktop/IFT 333/quoratrain.csv"
  
  val data = sc.textFile(fn2)
  
  data.toDF().show(2, false) 
  
  val header = data.first() /* HEADER */
  val input = data.filter(row => row != header) /*removed header */
  /*
  input.toDF.show(10, false) // will not truncate
  input.toDF.printSchema()
  */
  
  // End SC DF
  


   
   
  
}