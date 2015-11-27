/*** PageRank.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PageRank {
  def main(args: Array[String]) {
   // val logFile =  // Should be some file on your system
    val sc = new SparkContext()
    
	// Load the edges as a graph
	val graph = GraphLoader.edgeListFile(sc, args(0))
	// Run PageRank
	val ranks = graph.pageRank(0.0001).vertices
	// Join the ranks with the usernames
	val pages = sc.textFile(args(1)).map { line =>
	val fields = line.split("\\s")
	(fields(1).toLong, fields(0))
	}
	val ranksByURLs = pages.join(ranks).map {
	 case (id, (url, rank)) => (url, rank)
	}
	// Print the result
	println(ranksByURLs.collect().mkString("\n"))
  }
}
