# spark-ml
Sample application built using spark-ml, spark-streaming to classify news data.
Logistic regression from sparkml lib is used to train dataset and build model, news dataset is generated by reading 
news rss feeds from  news website and saved to text file.

Current code use Spark File Streaming to read new news from files and predict it's category using model generated by spark-ml.
Once predicted, news feeds are sent through kafka-consumer.

# spark-data deduplication
Sample application using spark-ml (pipeline), spark-streaming to detect near duplicates unstrucutred data by finding jaccard similarity between datasets.
