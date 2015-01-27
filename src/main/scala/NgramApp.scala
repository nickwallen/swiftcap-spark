import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NgramApp {
    
    // extracts the ngrams from a phrase
    def toNgrams (phrase: String, n: Int) = {
    
        // split the phrase into words
        val words = phrase.split(" ")
                          .filter(_.matches("\\w+"))
                          .map(_.toLowerCase())
    
        // split the words into ngrams
        val ngrams = for (i <- 1 to n) yield words.sliding(i)
                                                  .map(_.mkString(" "))
                                                  .toList
        ngrams.flatMap(x => x)
    }
    
    def main(args: Array[String]) {
    
        val argCount = args.length
        if (argCount != 2) {
            println("NgramApp [input-dir] [output-dir]")
            System.exit(1)
        }
            
        // collect the program arguments
        val input = args(0)
        val output = args(1)
        
        // initialize Spark
        val conf = new SparkConf().setAppName("NgramApp")
        val sc = new SparkContext(conf)
        
        // split the text into ngrams
        val lines = sc.textFile(input)
        val sentences = lines.flatMap(_.split("[.?!]+"))
        val ngrams = sentences.flatMap(sent => toNgrams(sent, 3))

        // count the number of each ngram
        val counts = ngrams.map (word => (word, 1)).reduceByKey(_+_)
        val sorted = counts.filter(_._2 > 3).sortByKey()
        sorted.saveAsTextFile(output)
    }
}
