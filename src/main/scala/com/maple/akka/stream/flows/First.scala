package com.maple.akka.stream.flows

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}

import scala.concurrent.Future

/**
  * 主要讲 Source Sink Flow
  *
  * Copyright (c) 2018 XiaoMi Inc. All Rights Reserved.
  * Authors: Maple <leihuazhe@xiaomi.com> on 19-2-26 14:27
  */
object First {

  private implicit val system: ActorSystem = ActorSystem("QuickStart")

  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    wireUpDifferentPart()
  }


  def simpleSourceAndSink(): Unit = {
    val source = Source(1 to 1000000000)
    val sink = Sink.fold[Int, Int](0)(_ + _)

    val runnable: RunnableGraph[Future[Int]] = source.toMat(sink)(Keep.right)

    val sum: Future[Int] = runnable.run()

    sum.onComplete(println)(system.dispatcher)
  }

  def simpleRunWith(): Unit = {
    val source = Source(1 to 1000)
    val sink = Sink.fold[Int, Int](0)(_ + _)

    // materialize the flow, getting the Sinks materialized value
    //both materializes the stream and returns the materialized value of the given sink or source.
    val sum: Future[Int] = source.runWith(sink)

    sum.onComplete(println)(system.dispatcher)
  }

  /**
    * 需要将改变的值赋值给新的变量
    *
    * It is worth pointing out that since operators are immutable, connecting them returns a new operator,
    * instead of modifying the existing instance,
    * so while constructing long flows,
    * remember to assign the new value to a variable or run it
    *
    */
  def longStream(): Unit = {
    val source = Source(1 to 10)
    source.map(_ ⇒ 0) // has no effect on source, since it's immutable

    source.runWith(Sink.fold(0)(_ + _)) // 55

    val zeroes = source.map(_ ⇒ 0) // returns new Source[Int], with `map()` appended

    zeroes.runWith(Sink.fold(0)(_ + _)) // 0

  }

  def multipleStreamMaterialized(): Unit = {
    // connect the Source to the Sink, obtaining a RunnableGraph
    val sink = Sink.fold[Int, Int](0)(_ + _)

    val runnable: RunnableGraph[Future[Int]] =
      Source(1 to 10).toMat(sink)(Keep.right)


    // get the materialized value of the FoldSink
    val sum1: Future[Int] = runnable.run()
    val sum2: Future[Int] = runnable.run()
    // sum1 and sum2 are different Futures! even though we used the same sink to refer to the future:

  }

  def multiConstruct(): Unit = {
    // Create a source from an Iterable
    Source(List(1, 2, 3))

    // Create a source from a Future
    Source.fromFuture(Future.successful("Hello Streams!"))

    // Create a source from a single element
    Source.single("only one element")

    // an empty source
    Source.empty

    // Sink that folds over the stream and returns a Future
    // of the final result as its materialized value
    Sink.fold[Int, Int](0)(_ + _)

    // Sink that returns a Future as its materialized value,
    // containing the first element of the stream
    Sink.head

    // A Sink that consumes a stream without doing anything with the elements
    Sink.ignore

    // A Sink that executes a side-effecting call for every element of the stream
    Sink.foreach[String](println(_))
  }


  def wireUpDifferentPart(): Unit = {
    // Explicitly creating and wiring up a Source, Sink and Flow
    Source(1 to 6).via(Flow[Int].map(_ * 2)).to(Sink.foreach(println(_))).run()

    // Starting from a Source
    val source = Source(1 to 6).map(_ * 2)
    source.to(Sink.foreach(println(_))).run()

    // Starting from a Sink
    val sink: Sink[Int, NotUsed] = Flow[Int].map(_ * 2).to(Sink.foreach(println(_)))
    Source(1 to 6).to(sink).run()

    // Broadcast to a sink inline
    val otherSink: Sink[Int, NotUsed] =
      Flow[Int].alsoTo(Sink.foreach(println(_))).to(Sink.ignore)
    Source(1 to 6).to(otherSink).run()
  }


}
