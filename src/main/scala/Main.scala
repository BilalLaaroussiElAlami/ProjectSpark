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

  type TextLength = Int
  type Tag = String
  type FollowersCount = Int
  type ReplyCount = Int
  type RetweetCount = Int
  type Likes = Int
  case class Tweet(text: String,
                   textLength: TextLength,
                   hashTags: Array[Tag],
                   followers_count: FollowersCount,
                   reply_count: ReplyCount,
                   retweet_count:RetweetCount,
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

    val reply_count = (jsonTweet \ "reply_count").asOpt[Int].getOrElse(0)
    val retweet_count = (jsonTweet \ "user" \ "retweet_count").asOpt[Int].getOrElse(0)
    Tweet(text, text.length,  hashtags,followers_count,reply_count,retweet_count, likes)
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
  text: String
  ,
  textLength: TextLength
  ,
  hashTags: Array[Tag]
  ,
  followers_count: Followers_count
  ,
  reply_count: Reply_count
  ,
  retweet_count: Retweet_count
  ,
  likes: Likes
  )
*/

  type Feature = Float
  type X0 = Int
  type FeatureTuple = (X0, TextLength, FollowersCount, ReplyCount, RetweetCount, Likes)
  def extractFeatures(tweets: RDD[Tweet]): RDD[FeatureTuple] = tweets.map( twt => (1, twt.textLength, twt.followers_count,twt.reply_count,twt.retweet_count, twt.likes))
  val featureRDD = extractFeatures(tweets)


  /*
  calculating sum and count --> average
  take sum of squared difference
   */

  type accType = Tuple2[Int,List[Int]]

  def seqOp(acc: accType , featureTuple: FeatureTuple) = {
    val newCount = acc._1 + 1
    val oldSums  = acc._2
    val newSums  = List(
      oldSums(0)+ featureTuple._2,  //remember featureTuple._1 is X0
      oldSums(1)+ featureTuple._3,
      oldSums(2)+ featureTuple._4,
      oldSums(3)+ featureTuple._5,
      oldSums(4)+ featureTuple._6)
    (newCount,newSums)
  }
  def binOp(accA: accType, accB: accType) = {
    val newCount = accA._1 + accB._1
    val newSums =  accA._2.zip(accB._2).map(p => p._1 + p._2)
    (newCount,newSums)
  }

  def scaleFeatures (featureRDD: RDD[FeatureTuple]): RDD[FeatureTuple] = {
    val (count, sums)  = featureRDD.aggregate(0,List(0,0,0,0,0))(seqOp, binOp)
    val means = sums.map(_/count)

    // (x_j^i - u_j)^2
    val diffWithMeanSquared = featureRDD.map {
      ftr =>
        (
          ftr._1,
          Math.pow(ftr._2 - means(0), 2),
          Math.pow(ftr._3 - means(1), 2),
          Math.pow(ftr._4 - means(2), 2),
          Math.pow(ftr._5 - means(3), 2),
          Math.pow(ftr._6 - means(4), 2))
    }


    def seqop(acc: List[Double], tuple: Tuple6[X0, Double,Double,Double,Double,Double]) =
    {
      List(
        acc(0) + tuple._2,
        acc(1) + tuple._3,
        acc(2) + tuple._4,
        acc(3) + tuple._5,
        acc(4) + tuple._6)
    }
    def binop = { (accA: List[Double], accB: List[Double]) => {
      accA.zip(accB).map(p => p._1 + p._2)
    }
    }

    //sum((x_j^i - u_j)^2)
    val sumsOfSquaredDiffs = diffWithMeanSquared.aggregate(List(0.0,0.0,0.0,0.0,0.0))(seqop,binop)

    val stdevs = sumsOfSquaredDiffs.map(sumsqrddiff =>  Math.sqrt(sumsqrddiff / count))


    def calculateZvalues(featureTuple: FeatureTuple):List[Double] = {
      List(
      (featureTuple._2 - means(0)) / stdevs(0),
      (featureTuple._3 - means(1)) / stdevs(1),
      (featureTuple._4 - means(2)) / stdevs(2),
      (featureTuple._5 - means(3)) / stdevs(3),
      (featureTuple._6 - means(4)) / stdevs(4))

    }
     featureRDD.map(calculateZvalues)
  }

  val scaledFeatureRDD = scaleFeatures(featureRDD)




}
