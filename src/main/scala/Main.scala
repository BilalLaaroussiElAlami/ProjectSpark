import org.apache.spark.rdd.RDD
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
                   hashTags: Array[Tag],
                   followers_count: Int,
                   reply_count: Int,
                   retweet_count:Int,
                   likes: Likes)

  def stringToJson(tweet: String): Option[JsValue] = {
    try {
      // Attempt to parse the string to JSON
      Some(Json.parse(tweet))
    } catch {
      case _: Exception => None // Return None if parsing fails
    }
  }

  //returns an option of the 'inner tweet'
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

  /*
    important : if there are missing values we use default values like "" for when no text is defined for example
    the alternative is only considering jsons where all wanted values are defined
   */
  def JsonToTweet(jsonTweet: JsValue): Tweet = {
    val text = (jsonTweet \ "text").asOpt[String].getOrElse("")
    val hashtags = Array("")
    val followers_count = (jsonTweet \ "user" \ "followers_count").asOpt[Int].getOrElse(0)
    val likes = (jsonTweet \ "user" \ "favourites_count").asOpt[Int].getOrElse(0)

    /*
    println(jsonTweet \ "user")
    println(jsonTweet \ "user" \ "favourites_count")
    println( (jsonTweet \ "user" \ "favourites_count").asOpt[Int].getOrElse(0))
    println()*/
    val reply_count = (jsonTweet \ "reply_count").asOpt[Int].getOrElse(0)
    val retweet_count = (jsonTweet \ "user" \ "retweet_count").asOpt[Int].getOrElse(0)
    Tweet(text,hashtags,followers_count,reply_count,retweet_count, likes)
  }

  def parseTweet(tweet:String):Option[Tweet] = {
    val json = stringToJson(tweet)
    if (json.isEmpty) return None

    val embeddedTweetJSON = getEmbeddedTweet(json.get)
    if(embeddedTweetJSON.isEmpty) return None

    val finalTweetObject = JsonToTweet(embeddedTweetJSON.get)
    Option(finalTweetObject)
  }

  val pathData =  "./data/tweets"
  /*
  val tweetsJsonsRDD = sc.textFile(pathData).map(stringToJson).filter(_.nonEmpty).map(_.get).persist()    //persisting because we'll use this source multiple times so persisting avoids the data being read each time
  tweetsJsonsRDD.foreach(println)
  print("====================================================================")
  val embeddedTweetsJsonsRDD = tweetsJsonsRDD.map(getEmbeddedTweet).filter(_.nonEmpty).map(_.get)
  embeddedTweetsJsonsRDD.foreach(println("x", _))
*/
  val tweets = sc.textFile(pathData).map(parseTweet).filter(_.isDefined).map(_.get).persist()  //persisting because this source might be used in multiple pipelines
  tweets.collect().foreach(println)   //calling collect is not necessary locally

  /*
  type Feature = Float
  type FeatureTuple = (Feature, Feature, Likes)
  def extractFeatures(tweets: RDD[Tweet]): RDD[FeatureTuple] = ???
  val featureRDD = extractFeatures(tweets)

  def scaleFeatures (featureRDD: RDD[FeatureTuple]): RDD[FeatureTuple] = ???
  val scaledFeatureRDD = scaleFeatures(featureRDD)
*/



}
