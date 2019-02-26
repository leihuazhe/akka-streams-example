package com.maple.akka.stream.helloworld

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Keep, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Copyright (c) 2018 XiaoMi Inc. All Rights Reserved.
  * Authors: Maple <leihuazhe@xiaomi.com> on 19-2-25 16:49
  */
object MapleStreamSpec {

  implicit val system = ActorSystem("QuickStart")

  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    test3

    /* val source: Source[Int, NotUsed] = Source(1 to 100)
     val done: Future[Done] = source.runForeach(i ⇒ println(i))(materializer)

     implicit val ec = system.dispatcher
     done.onComplete(_ ⇒ system.terminate())
 */
    //    Source(Array("funny", "sad").toVector).runForeach(println)

  }

  def test2(): Unit = {
    val source: Source[Int, NotUsed] = Source(1 to 100)

    val factorials: Source[BigInt, NotUsed] = source.scan(BigInt(1))((acc, next) => acc * next)

    /* val result: Future[IOResult] =
       factorials.map(num => ByteString(s"$num\n"))
         .runWith(FileIO.toPath(Paths.get("factorials.txt")))*/

    val result: Future[IOResult] =
      factorials.map(_.toString()).runWith(linkSink("factorials1.txt"))


    implicit val ec = system.dispatcher

    result.onComplete(_ ⇒ system.terminate())

  }

  def test3(): Unit = {
    val source: Source[Int, NotUsed] = Source(1 to 100)

    val factorials: Source[BigInt, NotUsed] = source.scan(BigInt(1))((acc, next) => acc * next)


    factorials.zipWith(Source(0 to 100))((num, idx) => s"$idx! = $num")
      .throttle(1, 1.second)
      .runForeach(println)

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


}
