package com.maple.akka.stream.helloworld

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Copyright (c) 2018 XiaoMi Inc. All Rights Reserved.
  * Authors: Maple <leihuazhe@xiaomi.com> on 19-2-25 18:51
  */

final case class Author(handle: String)

final case class Hashtag(name: String)

final case class Tweet(author: Author, timestamp: Long, body: String) {
  def hashtags: Set[Hashtag] = {
    val res = body.split(" ").collect {
      case t if t.startsWith("#") ⇒ Hashtag(t.replaceAll("[^#\\w]", ""))
    }.toSet
    res
  }
}

/*
  Streams 总是从 Source[Out,M1] 开始流动, 然后可以继续通过 Flow[In,Out,M2] 或更高级的操作进行转换,最终被 Sink[In,M3] 消耗

  为了 materialize 和开始计算流, 我们需要将 Flow 连接到一个将使 流运行的 Sink

  materializing and running


 */
object BrowserEmbedded {
  val tweets: Source[Tweet, NotUsed] = Source(
    Tweet(Author("rolandkuhn"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("patriknw"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("bantonsson"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("drewhk"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("ktosopl"), System.currentTimeMillis, "#akka on the rocks!") ::
      Tweet(Author("mmartynas"), System.currentTimeMillis, "wow #akka !") ::
      Tweet(Author("akkateam"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("bananaman"), System.currentTimeMillis, "#bananas rock!") ::
      Tweet(Author("appleman"), System.currentTimeMillis, "#apples rock!") ::
      Tweet(Author("drama"), System.currentTimeMillis, "we compared #apples to #oranges!") ::
      Nil)


  implicit val system = ActorSystem("reactive-tweets")
  implicit val materializer = ActorMaterializer()


  def main(args: Array[String]): Unit = {
    materializedValues
  }

  def test1(): Unit = {
    val akkaTag = Hashtag("#akka")
    tweets.filter(_.hashtags.contains(akkaTag)).map(_.author).runWith(Sink.foreach(println))
    //    val hashTags: Source[Hashtag, NotUsed] = tweets.mapConcat(_.hashtags.toList)

    tweets
      .map(_.hashtags) // Get all sets of hashtags ...
      .reduce(_ ++ _) // ... and reduce them to a single set, removing duplicates across all tweets
      .mapConcat(identity) // Flatten the set of hashtags to a stream of hashtags
      .map(_.name.toUpperCase) // Convert all hashtags to upper case
      .runWith(Sink.foreach(println)) // Attach the Flow to a Sink that will finally print the hashtags
  }


  /**
    *
    * @param fileName
    * @return
    */
  def linkSink(fileName: String) = {
    Flow[String].map(s => ByteString(s + "\n"))
      .toMat(FileIO.toPath(Paths.get(fileName)))(Keep.right)
  }

  def graphs() = {
    val writeAuthors: Sink[Author, NotUsed] = ???
    val writeHashtags: Sink[Hashtag, NotUsed] = ???
    //GraphDSL.create() return a Graph
    val g: RunnableGraph[NotUsed] = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Tweet](2))
      tweets ~> bcast.in
      bcast.out(0) ~> Flow[Tweet].map(_.author) ~> writeAuthors
      bcast.out(1) ~> Flow[Tweet].mapConcat(_.hashtags.toList) ~> writeHashtags
      ClosedShape
    })
    g.run()
  }


  def backPressure(): Unit = {
    tweets
      .buffer(10, OverflowStrategy.dropHead)
      //      .map(slowComputation)
      .runWith(Sink.ignore)

  }

  def materializedValues(): Unit = {
    //1.构造一个可重复用的 Flow, 它的作用是改变每一个输入 Tweet 为 1个数字 1
    val count: Flow[Tweet, Int, NotUsed] = Flow[Tweet].map(_ => 1)
    //2.我们通过 Sink.fold 来 组合他们，然后 sum all Int element.
    val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)


    //接下来，我们将tweets流连接到via计数。 最后，我们使用toMat将Flow连接到之前准备的Sink。

    val counterGraph = tweets.via(count).toMat(sumSink)(Keep.right)

    val sum = counterGraph.run()

    sum.foreach(c ⇒ println(s"Total tweets processed: $c"))


    val sum2: Future[Int] = tweets.map(t ⇒ 1).runWith(sumSink)


  }


  def test5(): Unit = {
    /*val sumSink = Sink.fold[Int, Int](0)(_ + _)
    val counterRunnableGraph: RunnableGraph[Future[Int]] =
      tweetsInMinuteFromNow
        .filter(_.hashtags contains akkaTag)
        .map(t ⇒ 1)
        .toMat(sumSink)(Keep.right)

    // materialize the stream once in the morning
    val morningTweetsCount: Future[Int] = counterRunnableGraph.run()
    // and once in the evening, reusing the flow
    val eveningTweetsCount: Future[Int] = counterRunnableGraph.run()
*/

  }


}