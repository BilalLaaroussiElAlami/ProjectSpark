import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._


object Main extends App{
  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]") //!!!!! TO REMOVE WHEN TESTING ON ISABELLE !!!!!!!
  val sc = new SparkContext(conf)
  val x = sc.parallelize(List(1,2,3))
  val y  = x.map(_*2);
  println(y.collect().mkString("Array(", ", ", ")"))


  type Tag = String
  type Likes = Int
  case class Tweet(text: String,
                   user: String,
                   hashTags: Array[Tag],
                   likes: Likes)

  //assumes the inner tweet of an embedded tweet !
  def parseTweet(tweet: String): Tweet = {
    val json: JsValue = Json.parse(tweet)
    val text =  (json \ "text").asOpt[JsValue].map(_.toString()).getOrElse("")

    //val hashtagsString = hashtags.map(_.toString()).getOrElse("No hashtags found")
    val likes = (json \ "likes").asOpt[JsValue].map(_.toString()).getOrElse("0").toInt
    Tweet(text,"",Array() ,likes)
  }

  def isEmbedded(tweet:String):Boolean = {
    val json: JsValue = Json.parse(tweet)
    val retweet = (json \ "retweeted_status").toOption.isDefined
    val quoted_tweet = (json \ "quoted_status").toOption.isDefined
    retweet || quoted_tweet
  }

  def getOriginal(tweet:String): String = {
    val json: JsValue = Json.parse(tweet)
    if((json \ "retweeted_status").toOption.isDefined){
      val retweetedStatus = (json \ "retweeted_status").asOpt[JsValue]
      val retweetedStatusString = retweetedStatus.map(_.toString()).getOrElse("Retweeted status not found")
      retweetedStatusString
    }
    else if((json \ "quoted_status").toOption.isDefined){
      val quotedStatus = (json \ "quoted_status").asOpt[JsValue]
      val quotedStatusString = quotedStatus.map(_.toString()).getOrElse("Retweeted status not found")
      quotedStatusString
    }
    else{
      throw new Exception("TWEET SHOULD BE RETWEET OR QUOTE")
    }
  }
/*
  def preProcess(tweet:String):String = {
    val postfix = "')#('"
    if (tweet.endsWith(postfix)) {
      tweet.dropRight(postfix.length)
    } else {
      tweet
    }
  }

*/
  def stringToJson(tweet: String): Option[JsValue] = {
    try {
      // Attempt to parse the string to JSON
      Some(Json.parse(tweet))
    } catch {
      case _: Exception => None // Return None if parsing fails
    }
  }

  def getEmbeddedTweet(tweet:JsValue): Option[JsValue] = {
    if ((tweet \ "retweeted_status").toOption.isDefined) {
      val retweetedStatus = (tweet \ "retweeted_status").asOpt[JsValue]
      retweetedStatus
    }
    else if ((tweet \ "quoted_status").toOption.isDefined) {
      val quotedStatus = (tweet \ "quoted_status").asOpt[JsValue]
      quotedStatus
    }
    else {
      None
    }
  }

  val pathData =  "./data/tweets"
  val tweetsJsonsRDD = sc.textFile(pathData).map(stringToJson).filter(_.nonEmpty).map(_.get).persist()    //persisting because we'll use this source multiple times so persisting avoids the data being read each time
  tweetsJsonsRDD.foreach(println)

  print("====================================================================")
  val embeddedTweetsJsonsRDD = tweetsJsonsRDD.map(getEmbeddedTweet).filter(_.nonEmpty).map(_.get)
  embeddedTweetsJsonsRDD.foreach(println("x", _))



  /*
  def testEmbedded() = {
    val first10Tweets = tweetsRDD.take(10)
    first10Tweets.zipWithIndex.foreach{ case (tweetstr, index) =>
      print(s"${index+1}'th tweet embedded?", isEmbedded(tweetstr))
      if(isEmbedded(tweetstr)) print(s" original: ${getOriginal(tweetstr)}")
      println()}
  }
  */

  /*
  def tweetObjectsRDD = tweetsRDD.map(parseTweet)

  tweetObjectsRDD.take(15).foreach(println)
   */


}
