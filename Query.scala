package search.sol

import java.io._

import search.src.PorterStemmer.stem
import search.src.StopWords.isStopWord
import search.src.FileIO
import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
  * Represents a query REPL built off of a specified index
  *
  * @param titleIndex    - the filename of the title index
  * @param documentIndex - the filename of the document index
  * @param wordIndex     - the filename of the word index
  * @param usePageRank   - true if page rank is to be incorporated into scoring
  */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]()

  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxFreqs = new HashMap[Int, Double]()

  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new HashMap[Int, Double]()

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]();

  /**
   * Get the inverse document frequency of a word
   * @param word the word
   * @return the IDF
   */
  private def getIDF(word: String): Double = {
    val numDocs = idsToTitle.size.toDouble
    val numDocsWithWord = wordsToDocumentFrequencies(word).size.toDouble
    Math.log(numDocs / numDocsWithWord)
  }

  /**
   * Returns an array with the top 10 highest scores from a map that maps IDs
   * to scores for that ID based on a query
   * @param scores the map
   * @return the array with highest score being at index 0 and the lowest
   *         at index 9
   */
  private def sortScores(scores: HashMap[Int, Double]): Array[Int] = {
    val size = math.min(10,scores.size)
    val results = new Array[Int](size)
    var i = 0
    while(i < size){
      // Get the current highest scoring ID
      val highestScore = scores.foldLeft(scores.keys.toList.head)((max,tuple) => {
        if(tuple._2 > scores(max)){
          tuple._1
        } else {
          max
        }
      })
      // Add the current highest scoring ID to results and remove it from
      // scores
      results(i) = highestScore
      scores.remove(highestScore)
      i += 1
    }
    results
  }

  /**
    * Handles a single query and prints out results
    * @param userQuery - the query text
    */
  private def query(userQuery: String) {

    // First process the query text by stemming and stopping
    val regex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")
    val userWords = regex.findAllMatchIn(userQuery).toList.foldLeft(
      List[String]())((rest, aMatch) => {
      val string = aMatch.matched.toLowerCase()
      if (!isStopWord(string)) {
        stem(string) :: rest
      } else {
        rest
      }
    })

    // Now assign a score to every document based on the query
    val scores = new HashMap[Int, Double]()
    for(string <- userWords) {
      wordsToDocumentFrequencies.get(string) match {
        case None =>
        case Some(idsToFreqs) => {
          for ((id,freq) <- idsToFreqs) {
            var rank = 1.0
            // Case in which page rank is enabled
            if(usePageRank){
              rank = idsToPageRank(id)
            }
            val score = (freq/idsToMaxFreqs(id))*getIDF(string)*rank
            scores.get(id) match {
              case None => {
                scores += (id -> score)
              }
              case Some(scr) =>
                scores.update(id,scr+score)
            }
          }
        }
      }
    }

    // Return the results
    if(scores.isEmpty){
      println("No Results")
    } else {
      printResults(sortScores(scores))
    }


  }

  /**
    * Format and print up to 10 results from the results list
    *
    * @param results - an array of all results
    */
  private def printResults(results: Array[Int]) {
    for (i <- 0 until Math.min(10, results.size)) {
      println("\t" + (i + 1) + " " + idsToTitle(results(i)))
    }
  }

  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }

  /**
    * Starts the read and print loop for queries
    */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))

    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()

    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }

      // Handle the query for the single line of input
      query(userQuery)

      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }
}

object Query {
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.size == 4 && args(0) == "--pagerank") {
        pageRank = true;
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.size != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex), args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
