# QuoraPairs
IFT-472 Project. Application of Cloud Computing with Big Data through Apache Spark.

Spark / Scala code to read in Quora Pairs input data, compare question strings, and predict if they are asking the same question.
Constructs feature Vector from word count, words in common, and distint words to be used in Logistic Regression model.

This provides no semantic or sentiment analysis, and as a result the model does not do a very good job of predicting the correct outcome. When running the model against the training data, it seemed to be correct about 65% of the time (at a glance). 

I assume this level of analysis is sufficient as the project is not for a Big Data class, and serves only as a foray into the 'Rabbit Hole' of Spark / Machine Learning.

# To Do List:
1. Upload data set to Amazon S3, add location to Scala code.
2. Prepare Scala code for hand-off to cluster manager.
3. Configure Cluster Mangager with output CSV location.
