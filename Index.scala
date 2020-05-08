package search.sol


import java.io.{FileNotFoundException, IOException}
import search.src.FileIO
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.matching.Regex
import scala.xml.Node
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stem
import search.src.PorterStemmer.stemArray

class Index(file: String) {

  // Stores the main node of the xml file
  private val mainNode: Node = xml.XML.loadFile(file)

  // Map that stores ids mapped to titles
  private val titleMap = new HashMap[Int,String]()

  // A intermediate data structure that maps docs to all processed words
  // and all titles the doc links to
  private val storageMap = new HashMap[Int, (List[String], HashSet[String])]()

  // Another in intermediate map that maps all ids to all ids that a particular
  // id links to
  private val linkMap = new HashMap[Int,HashSet[Int]]()

  // Maps a each word to a map that maps a doc to the frequency of that word in
  // that doc
  private val frequencies = new HashMap[String, HashMap[Int, Double]]()

  // Maps each doc to the frequency of the most frequent word in that doc
  private val idsToMaxCounts = new HashMap[Int, Double]()

  /**
   * Take raw XML and create ID title hashmap
   * @return the hashmap
   */
  private def parseTitles(): Unit = {
    val idSeq = (mainNode \ "page"\ "id")
      .map(x => x.text.replaceAll("""^\s+|\s+$""",""))
    val titleSeq = (mainNode \ "page"\ "title")
      .map(x => x.text.replaceAll("""^\s+|\s+$""",""))
    for(i <- idSeq.indices){
      titleMap += (idSeq(i).toInt -> titleSeq(i))
    }
  }

  /**
   * Map Titles to IDs
   * @return the map
   */
  private def parseTitlesReverse(): HashMap[String,Int] = {
    val revMap = new HashMap[String,Int]()
    for((id,title) <- titleMap){
      revMap += (title -> id)
    }
    revMap
  }

  /**
   * Take raw XML and create ID String hashmap
   * @return the hashmap
   */
  private def parseTexts(): HashMap[Int,String] = {
    val idSeq = (mainNode \ "page"\ "id")
      .map(x => x.text.replaceAll("""^\s+|\s+$""",""))
    val textSeq = (mainNode \ "page"\ "text").map(x => x.text)
    val textMap = new HashMap[Int,String]()
    for(i <- idSeq.indices){
      textMap += (idSeq(i).toInt -> textSeq(i))
    }
    textMap
  }

  /**
   * Populate StorageMap
   */
  private def processXML(): Unit = {
    val regex = new Regex("""\[\[[^\[]*\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
    val allDocs = parseTexts()
    // Iterate through every document and its text string
    for((k, v) <- allDocs) {
      // The links for the doc
      val links = new HashSet[String]()
      // Use the regex to create a list out of the text string and process
      // each item of the list
      val stringList = regex.findAllMatchIn(v).
        toList.foldLeft(List[String]())((rest, aMatch) => {
        val string = aMatch.matched
        // The case where the item matched is a link
        if (string.matches("""\[\[[^\[]*\]\]""")) {
          // Remove the brackets from the link
          val noBrackets = string.replaceAll("""[\[\]]""", "")
          // The case where the link is a pipe link:
          // 1) split on the bars in the link,
          // 2) Add the first item in array to the links for that doc
          // 3) process every item but the first one in the resulting array to
          // be added to the words for that doc
          if (noBrackets.matches("""[^:]*\|.*""")) {
            val noBars = noBrackets.split("""\s*\|\s*""")
            links.add(noBars(0))
            var counter = 1
            var lst = List[String]()
            while (counter < noBars.size) {
              // Process item: lower case, split on non-word characters, remove stops,
              // and stem
              lst = stemArray(noBars(counter)
                .toLowerCase().split("""[\W_]+""")).toList.filter(s =>
                !isStopWord(s)) ++ lst
              counter = counter + 1
            }
            lst ++ rest
            // Case where link is a metapage link:
            // 1) Split on the colon
            // 2) Add the second item in the resulting array to links
            // 3) Process every item in array to be added to words for this doc
          } else if (noBrackets.matches(""".*:.*""")) {
            val titleArray = noBrackets.split("""\s*:\s*""")
            if (titleArray.size > 1) {
              links.add(noBrackets.split("""\s*:\s*""")(1))
            }
            // Process items
            stemArray(noBrackets.toLowerCase().split("""[\W_]+""")).
              toList.filter(s => !isStopWord(s)) ++ rest
            // Case where the link is a regular link:
            // 1) Add link
            // 2) Process link to be added to words for this doc
          } else {
            links.add(noBrackets)
            // Process item
            stemArray(noBrackets.toLowerCase().split("""[\W_]+""")).
              toList.filter(s => !isStopWord(s)) ++ rest
          }
          // Case where the item matched is not a link
        } else {
          // Process the item so that it can be added to the words for this doc
          val str = string.toLowerCase
          if (!isStopWord(str)) {
            stem(str) :: rest
          } else {
            rest
          }
        }
      })
      // Get words in title
      val includeTitle = stemArray(titleMap(k).toLowerCase().split("""[\W_]+""")).
        toList.filter(s => !isStopWord(s))
      // Create a new mapping in storage map for this ID to all the essential
      // terms for that ID as well as all the title it links to
      storageMap += (k -> (stringList++includeTitle,links))
    }
  }

  /**
   * Process the links in storageMap and populate linkMap (Applies the
   * four rules for links)
   */
  private def populateLinkMap(): Unit = {
    val revTitles = parseTitlesReverse()
    // Iterate through every Doc in storage map, isolating the links
    for((id, (words,links)) <- storageMap){
      // The hashset type will not allow repeat IDs
      val idSet = new HashSet[Int]()
      // Case where a doc links to nothing
      if (links.isEmpty) {
        // Have that doc link to every ID but itself
        for ((i, t) <- storageMap) {
          if (i != id) {
            idSet.add(i)
          }
        }
        // Case where doc links to some other docs
      } else {
        // Iterate through all the titles the doc links to
        for (title <- links) {
          // Finds the ID associated with the title
          revTitles.get(title) match {
              // Ignores docs outside corpus
            case None => Nil
              // Adds the ID to the set if it is not the same as the ID being
              // looked at currently (links to itself are ignored)
            case Some(i) => {
              if (i != id) {
                idSet.add(i)
              }
            }
          }
        }
      }
      // Adds the set of IDs to linkMap
      linkMap += (id -> idSet)
    }
  }

  /**
   * Finds how many times a word appears in a text
   * @param word the word in a string
   * @param text the text which is a list of strings
   * @return the amount of times word appears in text
   */
  private def findWordCount(word: String, text: List[String]): Double = {
    text.count(s => s.equals(word))
  }

  /**
   * Populate both frequencies map and idsToMaxCounts while deleting from
   * storageMap to save space
   */
  private def handleWords(): Unit = {
    // Iterate through all IDs in storage map and isolate the essential
    // terms associated with that doc
    for((id, (words,links)) <- storageMap) {
      for (word <- words) {
        frequencies.get(word) match {
            // Case in which the word is not yet a key
            // 1) Create a new HashMap
            // 2) Map the current ID to the words frequency in this ID
          case None => {
            val f = findWordCount(word, words)
            val newMap = new HashMap[Int, Double]()
            newMap += (id -> f)
            frequencies += (word -> newMap)
          }
            // Case where word is already a key
            // 1) Add a new key to this words HashMap that is the
            // current ID and map it to this words frequency for that ID
          case Some(map) => {
            // Check if we've already seen this word in this doc (saves time)
            if(!map.contains(id)) {
              val f = findWordCount(word, words)
              map += (id -> f)
              frequencies.update(word, map)
            }
          }
        }
      }
      // Once the frequency map has stored every word in this doc with this ID
      // mapped to a frequency, we now populate the max frequency map which
      // maps this ID to the count of the most frequent word
      idsToMaxCounts += (id -> words.foldLeft(0.0)((max, s) => {
        val current = frequencies(s)(id)
        if (current > max) {
          current
        } else {
          max
        }
      }))
      // Remove the current ID from storage map because the information is no
      // longer needed
      storageMap.remove(id)
    }
  }

  /**
   * Accesses linkMap to calculate the weight that doc k gives to doc j
   * @param j the document which is recieving weight
   * @param k doc that is giving weight
   * @return the weight
   */
  def getWeight(j: Int, k: Int): Double = {
    val e = 0.15
    val n = linkMap.size
    val nk = linkMap(k).size
    if (linkMap(k).contains(j)) {
      e/n + (1.0 - e)*(1.0/nk)
    } else {
      e/n
    }
  }

  /**
   * Calculates the Euclidean distance between two rank maps
   * @param r one rank map
   * @param rprime the other rank map
   * @return the distance
   */
  private def calcEuclideanDist(r: HashMap[Int, Double],
                        rprime: HashMap[Int, Double]): Double = {
    var sum = 0.0
    for ((id, rank) <- r) {
      sum = sum + math.pow(rprime(id) - r(id), 2)
    }
    Math.sqrt(sum)
  }

  /**
   * Generates the HashMap mapping every ID in the corpus to its pagerank
   * @return the Map
   */
  private def pageRank(): HashMap[Int, Double] = {
    val delta = 0.001
    val n = linkMap.size
    // Generate the two rank maps according to the algorithm
    var r = linkMap.map{ case (id, wts) => (id, 0.0)}
    val rprime: HashMap[Int, Double] = linkMap.map{ case (id, wts) => (id, 1.0/n)}
    // Flesh out the pseudocode in accordance with our implementation
    while (calcEuclideanDist(r, rprime) > delta) {
      // Copy contents of rprime to r
      r = rprime.map{case (doc,rank) => (doc,rank)}
      for ((j, rank) <- rprime) {
        // Reset rprime to 0
        rprime.update(j, 0.0)
        for ((k, rank1) <- rprime) {
          // Do the calculation
          rprime.update(j, rprime(j) + getWeight(j,k)*r(k))
        }
      }
    }
    // Return the rank map once the euclidean distance is within delta
    rprime
  }
}


object Index {
  def main(args: Array[String]) {
    try {
      if (args.length != 4) {
        println("Incorrect Arguments")
      } else {
        val index = new Index(args(0))
        val titles = args(1)
        val words = args(3)
        val docs = args(2)
        index.parseTitles()
        index.processXML()
        index.populateLinkMap()
        index.handleWords()
        FileIO.printTitleFile(titles, index.titleMap)
        FileIO.printWordsFile(words, index.frequencies)
        index.frequencies.clear()
        FileIO.printDocumentFile(docs, index.idsToMaxCounts, index.pageRank())
      }
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
