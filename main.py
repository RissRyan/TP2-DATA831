from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 # create Spark context with Spark configuration
    conf = SparkConf()
    sc = SparkContext(conf=conf)
 # read in text file and split each document into words
    words = sc.textFile("data/ok.txt").flatMap(lambda line: line.split(" "))
 # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
    wordCounts.saveAsTextFile("data/outputs/output.txt")