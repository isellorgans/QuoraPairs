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

import org.apache.spark.mllib.{linalg => mllib}
import org.apache.spark.ml.{linalg => ml}

import org.apache.spark.mllib.linalg.{ Vectors}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.regression.{LinearRegression}

/* 
 * Initial Notebook for IFT 472 Project, assumes local Quora Dataset availability.
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
        .load("C:/Users/organ/Desktop/IFT 333/quoratrain.csv") // LOCAL
               
       dframe.show()
       dframe.printSchema()
  // End Spark.read DF
    
       
  // Spark Context DF (View for DF / DS Structure and testing)      
  /*
  val sc = spark.sparkContext
  val fn2 = "C:/Users/organ/Desktop/IFT 333/quoratrain.csv"
  
  val data = sc.textFile(fn2)
  
  data.toDF().show(2, false) 
  
  val header = data.first() /* HEADER */
  val input = data.filter(row => row != header) /*removed header */
  
  */
  
  // End SC DF
  
  // Chop up DF to what we need (just the questions for now)
  
   dframe.createOrReplaceTempView("input")
   val sqlDF = spark.sql("SELECT question1, question2, is_duplicate FROM input")
   
   // UDF to count the number of words in the question sentence
     val qCount = udf {
                  (set1:String) =>
                    (set1.split(" ").toSeq.size)
                    }
   
   // End Count
   
  //  UDF to find words in common (Intersect).
   val toRemove = "?!".toSet //filter out symbols so words can match properly
                             // For example Word != Word?
   val intersect = udf {
      					   (set1:String, set2:String) =>
                    (set1.filterNot(toRemove).split(" ")
                        .intersect(set2.filterNot(toRemove).split(" ")).toSeq) 
                    }
  // End intersect
    
  // UDF to find the size or result arrays from defined string comparison functions.  
    val size = udf {
                 (item:Seq[String]) =>
                   (item.size)
                  }
  // End size  
    
  // Function for distinct words
    val distinct = udf {
                      
                     (set1:String, set2:String) =>
                       (
                       ((set1.filterNot(toRemove).split(" ").toList)
                           diff
                       (set2.filterNot(toRemove).split(" ").toList))
                    
                       )

                   }
    
        
    // Adding function-driven columns to the Data frame.
    val wipDF = sqlDF.withColumn("Q1 Word Count", qCount($"question1").cast(DoubleType))
                     .withColumn("Q2 Word Count", qCount($"question2").cast(DoubleType))
                     .withColumn("intersect", intersect($"question1", $"question2"))
                     .withColumn("NumberInCommon", size($"intersect").cast(DoubleType))
                     .withColumn("Distinct Words", distinct($"question1", $"question2"))
                     .withColumn("NumberDistinct", size($"Distinct Words").cast(DoubleType))
                     .withColumn("is_duplicate", ($"is_duplicate").cast(DoubleType))
    
                     
    
    wipDF.show()
    wipDF.printSchema()
    
    // Next, we'll format a DF for Features and Label as needed for regression.
    
    val finalDF = wipDF.select("Q1 Word Count", "Q2 Word Count", "NumberInCommon",
                            "NumberDistinct", "is_duplicate").limit(500)
                            
        finalDF.show()
        
        
    // Use VectorAssembler to construct dataframe on label and feature columns.
        
        val assembler = new VectorAssembler()
                        .setInputCols(Array("Q1 Word Count", "Q2 Word Count", "NumberInCommon",
                                            "NumberDistinct"))
                        .setOutputCol("features")
          
        val output = assembler.transform(finalDF)
                         .select("is_duplicate", "features")
                         .toDF("label", "features")
                
    // Splitting the data source into training and test data randomly. 
                          
    val split = output.randomSplit(Array(0.7, 0.3))
 
    val training = split(0) // 70%
    val test = split(1)    // 30%
   
    
    //val training = output.select("is_duplicate", "features")
      //                   .toDF("label", "features")
                         
          
    // LR Model, fit, and transform 
  
    val model = new LogisticRegression().fit(training)
    val predictions = model.transform(test)
  
    predictions.select("features", "prediction").show()
    
    predictions.show(false)
    predictions.printSchema()
    
    // CSV only takes atomic values, so we will break the probability array 
    // into columns for export.
 
    // udf to access Vector column to array for selection 
   // MUST convert from old mllib vectors to new ML, as they are not compatible. 
    
   val first = udf((v: ml.Vector) => mllib.Vectors.fromML(v).toArray(0))
   val second = udf((v: ml.Vector) => mllib.Vectors.fromML(v).toArray(1))
     
    val resultDF = predictions
                      .withColumn("Probability 0", first($"probability"))
                      .withColumn("Probability 1", second($"probability"))
                      .drop("probability")
                      .withColumn("Raw Prediction 0", first($"rawPrediction"))
                      .withColumn("Raw Prediction 1", second($"rawPrediction"))
                      .drop("rawPrediction")
                      .drop("label")
                      .drop("features")
                      
  // Raw Prediction gives a measure of confidence in each possible label (where larger = more confident).
   resultDF.show()
   resultDF.printSchema()
    
  // Outpur result DF into one (coalesce) CSV file for use in any data visualization tool. 
  resultDF.coalesce(1).write.option("header", "true").csv(/* "OUTPUT PATH" */)
 
}