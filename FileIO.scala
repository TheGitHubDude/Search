package search.src

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}

import scala.collection.mutable.HashMap

object FileIO {

  /**
    * Print title index for each document, formatted as follows:
    * ID_1::Title_1
    * ID_2::Title_2
    * ...
    */
  def printTitleFile(titleFile: String, idsToTitles: HashMap[Int, String]) {
    val titleWriter = new BufferedWriter(new FileWriter(titleFile))
    for ((id, title) <- idsToTitles) {
      titleWriter.write(id + "::" + title + "\n")
    }
    titleWriter.close()
  }

  /**
    * Print euclidean normalization and page rank for each document,
    * formatted as follows:
    * ID_1 Ed_1 PageRank_1
    * ID_2 Ed_2 PageRank_2
    * ...
    */
  def printDocumentFile(documentFile: String,
                        idsToMaxCounts: HashMap[Int, Double],
                        idsToPageRanks: HashMap[Int, Double]) {
    val docWriter = new BufferedWriter(new FileWriter(documentFile))
    for ((id, ed) <- idsToMaxCounts) {
      docWriter.write(id + " " + ed + " " + idsToPageRanks(id) + "\n")
    }
    docWriter.close()
  }

  /**
    * Prints documents ID with frequency
    * for each word, formatted as follows:
    * Word_1 ID_a Freq_a ID_b Freq_b ...
    * Word_2 ID_c Freq_c ID_d Freq_d ...
    * ...
    */
  def printWordsFile(wordsFile: String,
                     wordsToDocumentFrequencies: HashMap[String, HashMap[Int, Double]]) {
    val wordWriter = new BufferedWriter(new FileWriter(wordsFile))
    for ((word, freqMap) <- wordsToDocumentFrequencies) {
      wordWriter.write(word + " ")
      // print ids of documents followed by frequency of word in that document
      for ((id, frequency) <- freqMap) {
        wordWriter.write(id + " " + frequency + " ")
      }
      wordWriter.write("\n")
    }
    wordWriter.close()
  }

  /**
    * Reads the title index and populates idsToTitle map
    */
  def readTitles(titleIndex: String, idsToTitle: HashMap[Int, String]) {
    val titlesReader = new BufferedReader(new FileReader(titleIndex))
    var line = titlesReader.readLine()

    while (line != null) {
      val tokens = line.split("::")
      // Create map of document ids to document titles
      idsToTitle(tokens(0).toInt) = tokens(1)
      line = titlesReader.readLine()
    }

    titlesReader.close()
  }

  /**
    * Reads the word index and populates wordToDocuments
    */
  def readWords(wordIndex: String,
                wordsToDocumentFrequencies: HashMap[String, HashMap[Int, Double]]) {
    val wordsReader = new BufferedReader(new FileReader(wordIndex))
    var line = wordsReader.readLine()

    while (line != null) {
      val tokens = line.split(" ")
      // Create map of document ids to frequencies of current word in that document
      wordsToDocumentFrequencies(tokens(0)) = new HashMap[Int, Double]
      for (i <- 1 until (tokens.size - 1) by 2) {
        // wordsToDocumentFrequencies(tokens(0))(tokens(i).toInt) = tokens(i + 1).toInt
        wordsToDocumentFrequencies(tokens(0)) += (tokens(i).toInt -> tokens(i + 1).toDouble)
      }

      line = wordsReader.readLine()
    }

    wordsReader.close()
  }

   /**
    * Reads the document index and populates idstoEd and idsToPageRank
    */
  def readDocuments(documentIndex: String, idsToMaxFreqs: HashMap[Int, Double], idsToPageRank: HashMap[Int, Double]) {
    val documentsReader = new BufferedReader(new FileReader(documentIndex))
    var line = documentsReader.readLine()

    while (line != null) {
      val tokens = line.split(" ")

      // Save max word frequency for each document in map
      idsToMaxFreqs(tokens(0).toInt) = tokens(1).toDouble
      // Save page rank for each document in map
      idsToPageRank(tokens(0).toInt) = tokens(2).toDouble

      line = documentsReader.readLine()
    }

    documentsReader.close()
  }

}
