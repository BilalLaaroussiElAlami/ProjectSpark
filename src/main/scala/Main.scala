import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.{File, IOException, PrintWriter}


object Main extends App {

  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]") //!!!!! TO REMOVE WHEN TESTING ON ISABELLE !!!!!!!
  val sc = new SparkContext(conf)

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
                   retweet_count: RetweetCount,
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
  def getEmbeddedTweet(tweet: JsValue): Option[JsValue] = {
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
    Tweet(text, text.length, hashtags, followers_count, reply_count, retweet_count, likes)
  }

  def parseTweet(tweet: String): Option[Tweet] = {
    val json = stringToJson(tweet)
    if (json.isEmpty) return None

    val embeddedTweetJSON = getEmbeddedTweet(json.get)
    if (embeddedTweetJSON.isEmpty) return None

    val finalTweetObject = JsonToTweet(embeddedTweetJSON.get)
    Option(finalTweetObject)
  }

  val pathData = "./data/tweets"

  val tweets = sc.textFile(pathData).map(parseTweet).filter(_.isDefined).map(_.get)

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

  //todo change to array of floats or doubles, but int works for now
  def extractFeatures(tweets: RDD[Tweet]): RDD[Array[Int]] = tweets.map(twt => Array(1, twt.textLength, twt.followers_count, twt.reply_count, twt.retweet_count, twt.likes))

  val featureRDD = extractFeatures(tweets) //index 0 = X0, index i -> n-1 dependenrt variables, index n dependent variable
  featureRDD.foreach(arr => println(arr.mkString("(",",",")")))
  val NindependentVars = 4

  println("===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔===🔔")


  /*
  calculating sum and count --> average
  take sum of squared difference
   */
  type accType = Tuple2[Int, Array[Int]] //(count, sums)

  //takes input accumulator and an element of the featureRDD, and updates the accumulator
  def seqOpScale(acc: accType, featureArray: Array[Int]): accType = {
    val newCount = acc._1 + 1
    val oldSums = acc._2
    val newSums = oldSums.zip(featureArray).map(p => p._1 + p._2) //!! keep in mind that this will also sum up X0 and dependent variable !!
    (newCount, newSums)
  }

  def binOpSqale(accA: accType, accB: accType): accType = {
    val newCount = accA._1 + accB._1
    val newSums = accA._2.zip(accB._2).map(p => p._1 + p._2)
    (newCount, newSums)
  }

  def scaleFeatures(featureRDD: RDD[Array[Int]]): RDD[Array[Double]] = {
    val (count, sums) = featureRDD.aggregate(0, Array(0, 0, 0, 0, 0, 0))(seqOpScale, binOpSqale) //iterates once over the RDD
    val means = sums.map(_ / count)

    // RDD of (x_j^i - u_j)^2
    val diffWithMeanSquaredRDD = featureRDD.map {
      ftr: Array[Int] =>
        Array( ftr(0),//not necessary for X0
          Math.pow(ftr(1) - means(1), 2),
          Math.pow(ftr(2) - means(2), 2),
          Math.pow(ftr(3) - means(3), 2),
          Math.pow(ftr(4) - means(4), 2),
          ftr(5)) //not necessary for dependent var
    }


    // RDD [Array ( sum((x_j^0 - u_j)^2), sum((x_j^1 - u_j)^2), ..., sum((x_j^n - u_j)^2)
    val sumsOfSquaredDiffsRDD = diffWithMeanSquaredRDD.aggregate(Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))(
      (acc, meandiffSquareds) => acc.zip(meandiffSquareds).map(p => p._1 + p._2),
      (accA, accB) => accA.zip(accB).map(p => p._1 + p._2))

    val stdevs = sumsOfSquaredDiffsRDD.map(sumsqrddiff => Math.sqrt(sumsqrddiff / (count-1)))


    def calculateZvalues(features: Array[Int]): Array[Double] = {
      Array(
        features(0),
        (features(1) - means(1)) / stdevs(1),
        (features(2) - means(2)) / stdevs(2),
        (features(3) - means(3)) / stdevs(3),
        (features(4) - means(4)) / stdevs(4),
        features(5)) //dependent variable should not be replaced by z value  (was: (featureTuple._6 - means(4)) / stdevs(4)))
    }

    featureRDD.map(calculateZvalues)
  }

  val scaledFeatureRDD = scaleFeatures(featureRDD)
  //scaledFeatureRDD.foreach(arr =>  println(arr.mkString("(",",",")")))


  type Theta = Array[Float]

  def H(theta: Theta, X: Array[Double]): Double = {
    if(X(0).toInt !=  1){
      throw new Exception(s"X0 must be equal to 1, X0 = ${X(0)}")
    }
    if (theta.length != X.length) {
      throw new Exception(s"THETA LENGTH ${theta.length} AND X LENGTH ${X.length} MUST BE THE SAME!")
    }
    else {
      theta.zip(X).map(tpl => tpl._1 * tpl._2).sum
    }
  }

  def J(scaledFeatureRDD: RDD[Array[Double]], theta: Theta, m: Long): Double = {
    (1.0 / 2 * m) *
      scaledFeatureRDD.map { featureTuple =>
        Math.pow(H(theta, featureTuple.dropRight(1)) - featureTuple.last, 2)
      }.sum()
  }

  //RDDodCalculatedTerms : RDD of (hθ (X(i)) − y(i))
  def JV2(RDDofCalculatedTerms: RDD[Double], m: Long): Double = {
    val teller = RDDofCalculatedTerms.map(x => x * x).sum()
    val noemer = (2.0 * m)
    teller / noemer
  }

  /*the term (hθ (X(i) − y(i)) is common in both the calculation of the mean squared error and calculation of thetas
  the function will calculate this common term once, the other formulas will both use the result of the calculated persisted result
  */
  def CommonCalculation(scaledFeatureRDD: RDD[Array[Double]], theta: Theta): RDD[Double] = {
    val res = scaledFeatureRDD.map(featureArr => H(theta, featureArr.dropRight(1)) - featureArr.last)
    //val x = res.collect()  //Debug
    res
  }

  def gradientDescent(scaledFeatureRDD: RDD[Array[Double]], initialTheta: Theta, alpha: Float, sigma: Float, m: Long) = {
    if(initialTheta.length != NindependentVars + 1) throw new Exception("THETA LENGTH NOT CORRECT")
    var theta: Theta = initialTheta
    var commonMap = CommonCalculation(scaledFeatureRDD, theta) //TODO change variable name
    val error = JV2(commonMap, m)
    var delta: Float = 0
    do {
      val newTheta = Array.fill(theta.length)(0.0f)
      val X = -alpha * (1.0 / m) * commonMap.sum() //commonMap usage 1
      val YS = scaledFeatureRDD.aggregate(Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))((acc, arr) => acc.zip(arr).map(p => p._1 + p._2), (arrA, arrB) => arrA.zip(arrB).map(p => p._1 + p._2))
      for (j <- 0 until theta.length) {
        //val Y = scaledFeatureRDD.map(ftr => ftr(j)).sum()
        newTheta(j) = (theta(j) - X * YS(j)).toFloat
      }
      theta = newTheta
      delta = (error - JV2(commonMap, m)).toFloat //commonMap usage 2
      commonMap = CommonCalculation(scaledFeatureRDD, theta).persist()
    }
    while (delta > sigma)
    (theta, error)
  }


  val n = scaledFeatureRDD.take(1)(0).length - 1 //minus the dependent variable
  println("----------------n: ", n)
  val initialTheta = Array.fill(n)(0.0f)
  val alpha = 0.001f
  val sigma = 0.001f
  val m = featureRDD.count()
  val res = gradientDescent(scaledFeatureRDD, initialTheta, alpha, sigma, m)
  val thetas = res._1
  val err = res._2
  println(thetas.mkString("Array(", ",", ")"))
  println("☢️ error: ", err)


}

