import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.{File, IOException, PrintWriter}






object Main extends App{


  val filePath = "fkezi.txt"
  //source chatgpt: https://chat.openai.com/share/a3a00db4-062f-416a-903f-13cb5599f403
  val writer = new PrintWriter(new File(filePath))
  def printToFile(content: String): Unit = {
    try {
      writer.write(content)
    } catch {
      case e: IOException =>
        println(s"An error occurred while writing to the file: ${e.getMessage}")
    } finally {
      writer.close()
    }
  }

  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]") //!!!!! TO REMOVE WHEN TESTING ON ISABELLE !!!!!!!
  val sc = new SparkContext(conf)
  val x = sc.parallelize(List(1,2,3))
  val y  = x.map(_*2);
  //println(y.collect().mkString("Array(", ", ", ")"))

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
    val retweet_count = (jsonTweet \ "retweet_count").asOpt[Int].getOrElse(0)
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
  //tweets.collect().foreach(println)   //calling collect is not necessary locally

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
 // featureRDD.foreach(println)


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
      oldSums(4)+ featureTuple._6)  //todo dependent variable stays same !!
    (newCount,newSums)
  }
  def binOp(accA: accType, accB: accType) = {
    val newCount = accA._1 + accB._1
    val newSums =  accA._2.zip(accB._2).map(p => p._1 + p._2)
    (newCount,newSums)
  }

  def scaleFeatures (featureRDD: RDD[FeatureTuple]): RDD[Array[Double]] = {
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


    def calculateZvalues(featureTuple: FeatureTuple):Array[Double] = {
      Array(
        featureTuple._1,
        (featureTuple._2 - means(0)) / stdevs(0),
        (featureTuple._3 - means(1)) / stdevs(1),
        (featureTuple._4 - means(2)) / stdevs(2),
        (featureTuple._5 - means(3)) / stdevs(3),
        (featureTuple._6 - means(4)) / stdevs(4))
    }
    featureRDD.map(calculateZvalues)
  }

  val scaledFeatureRDD = scaleFeatures(featureRDD)
  scaledFeatureRDD.foreach(arr =>  println(arr.mkString("(",",",")")))


  type Theta = Array[Float]

  def H(theta:Theta,  X: Array[Double]): Double = {
    if(theta.length != X.length){
      throw new Exception(s"THETA LENGTH ${theta.length} AND X LENGTH ${X.length} MUST BE THE SAME!")
    }
    else{
      theta.zip(X).map(tpl => tpl._1 * tpl._2).sum
    }
  }

  def J(scaledFeatureRDD: RDD[Array[Double]], theta: Theta, m:Long): Double = {
    (1.0 / 2 * m) *
    scaledFeatureRDD.map{featureTuple =>
      Math.pow( H(theta,featureTuple.dropRight(1)) - featureTuple.last , 2)
    }.sum()
  }


  def JV2(RDDofCalculatedTerms : RDD[Double], theta: Theta, m:Long): Double = {
    val teller = RDDofCalculatedTerms.map(x => x*x).sum()
    val noemer =  (2.0 * m)
    teller/noemer
  }

  /*the term (hθ (X(i) − y(i)) is common in both the calculation of the mean squared error and calculation of thetas
  the function will calculate this commont term once, the other formulas will both use the result of the calculated persisted result
  */
  def CommonCalculation(scaledFeatureRDD: RDD[Array[Double]], theta: Theta): RDD[Double] = {
    val res = scaledFeatureRDD.map(featureTuple => H(theta, featureTuple.dropRight(1)) - featureTuple.last)
    val x = res.collect()
    res
  }

  def CalcNewThetas(thetas:Theta, RDDofCalculatedTerms : RDD[Double], alfa:Double, m: Int): Theta = {
    //hθ (X(i) ) − y(i) ) x(i)
    ???
  }



    def gradientDescent(scaledFeatureRDD: RDD[Array[Double]],initialTheta: Theta, alpha: Float, sigma: Float, m: Long): Theta = {
      var theta:Theta = initialTheta
      val commonMap = CommonCalculation(scaledFeatureRDD,theta)  //TODO change variable name
      val error = JV2(commonMap, theta, m)
      var delta:Float = 0
      do{
        val newTheta = Array.fill(theta.length)(0.0f)
        val commonMap = CommonCalculation(scaledFeatureRDD,theta).persist()
        val X = -alpha*(1.0/m)*commonMap.sum()
        for(j <- 1 to theta.length){
          val indexTheta = j - 1  //theta is array starting from 0 while tuples start index from 1 so difference is 1 for the same position
          val Y = scaledFeatureRDD.map(ftrTuple => ftrTuple(j)).sum()
          newTheta(indexTheta) = (theta(indexTheta) - X * Y  ).toFloat
        }
        theta = newTheta
        delta = (error - JV2(commonMap,theta,m)).toFloat
      }
      while (delta > sigma)
      theta
  }


  val n = scaledFeatureRDD.take(1)(0).length - 1

  println("----------------n: ", n)
  val initialTheta = Array.fill(n)(0.0f)
  val alpha = 0.001f
  val sigma = 0.001f
  val m = featureRDD.count()
  val thetas = gradientDescent(scaledFeatureRDD, initialTheta, alpha, sigma, m)

  println(thetas.mkString("Array(",",", ")"))
}
