/*** PageRank.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PageRank {
  def main(args: Array[String]) {
    val logFile = "/home/craig/spark/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "PageRank", "$SPARK_HOME", List("target/scala-2.11/pagerank_2.11-1.0.jar"))
    
	// Load the edges as a graph
	val graph = GraphLoader.edgeListFile(sc, "/home/ankit/Downloads/example_arcs")
	// Run PageRank
	val ranks = graph.pageRank(0.0001).vertices
	// Join the ranks with the usernames
	val pages = sc.textFile("/home/ankit/Downloads/example_index").map { line =>
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
