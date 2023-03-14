import scala.io.Source

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.BalancingPool
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.collection.immutable.HashMap

//Counts the characters in each chunk of text
class Mapper() extends Actor {

  //if the character is not a letter, return a null char. If it is a letter, make it lowercase
  def removeSpecial(m: (Char, Seq[Char])) : (Char, Int) = {
    if (m._1.isLetter) {
      (m._1.toLower, m._2.length)
    } else {
      ('\u0000', 0)
    }
  }

  def receive : Receive = {
    //receive chunk of text to process from Overseer
    case s: Seq[Char] =>
      //group by character, resulting in Map(character -> List(each occurence of character in chunk))
      val mapped = s.groupBy(identity) 
    
      //For each character in map, count the list to get the number of occurences for the character, resulting in HashMap(Character -> Count)
      //Also remove null char map
      val combined = mapped.map(removeSpecial).toMap
  
      //Send HashMap to OverSeer
      sender ! combined
  }
}

class Overseer extends Actor {
  //Actor to send the final HashMap to
  var sendResultTo : ActorRef = context.self
 
  //keep track of how many chunks sent and how many counted chars returned so as to know when to send the final HashMap to main 
  var sent : Int = 0
  var received : Int = 0
  
  //Map to concatinate all the Actor results in
  var reducedMap : HashMap[Char, Int] = new HashMap()
  
  def receive : Receive = {
    case s : String =>
      partition(s)
      sendResultTo = sender()
    //Recieve HashMap of counted chars
    case hm : HashMap[Char, Int] =>
      //store that a result was recieved
      received += 1
      //merge recieved HashMap into reducedmap, calling collision function for each key with a collision
      reducedMap = reducedMap.merged(hm)(collision) 
      //if all actors returned
      if (sent == received) {
        //send result back to Main
        sendResultTo ! reducedMap - '\u0000'
      }
    }

  //Get available number of CPU cores
  val runtime = Runtime.getRuntime()
  val numCores : Int = runtime.availableProcessors()

  //Create a balancing pool for balancing work accross all cores
  val router = context.actorOf(BalancingPool(numCores).props(Props[Mapper]))

  //Partition file
  def partition(dir: String) : Unit = {

    //file object
    val file = Source.fromFile(dir)

    //number of chars to chunk file into
    val chunkSize = 30000

    //Lazy load file in chunks
    for (chars <- file.iter.grouped(chunkSize)) {

      router ! chars

      sent += 1

    }
  }
  
  //On a key collision, i.e. the same character, add the value of both 
  def collision(a: (Char, Int), b: (Char, Int)) : (Char, Int) = {
    a._1 -> (a._2 + b._2)
  }
}


object Main {
  
  //pass file dir in first argument and optionally wait time in second argument
 def main(args: Array[String]) : Unit = {
    //If no args provided use hardcoded file, otherwise use file provided in argument
    Option(args) match {
      case Some(Array()) => readLines("/home/struan/Development/University/Big Data/bleak2.txt")
      case _ => 
        //allow specifying wait times (time until promise returns error) when passing a file as an argument
        if (args.length < 2) {
          readLines(args(0))
        } else {
          readLines(args(0), args(1).toInt)
      }
    }  
    
  }
  
  //print results nicely
  def prettyPrint(hm: HashMap[Char, Int]) : Unit = {
    
    println("*--------------*Total Characters*--------------*")

    var totalChars: Int = 0
    hm.foreach {
      case (key, value) => 
        totalChars += value 
        println(s"$key -> $value")
    }
  
    println(s"$totalChars total characters")

      }
  
  //Read lines from file, send to overseer and await answer
  def readLines(dir: String, waitTime: Int = 100) : Unit = {

    val system = ActorSystem("mappers")
    val mainActor = system.actorOf(Props[Overseer])

    
    val timeout = Timeout (FiniteDuration(waitTime, SECONDS))
    val future = ask (mainActor, dir) (timeout)
    val result = Await.result (future, timeout.duration)
    
    val hm : HashMap[Char, Int] = result.asInstanceOf[HashMap[Char, Int]]
    
    prettyPrint(hm)  

    system.terminate()
  }  
  
}
