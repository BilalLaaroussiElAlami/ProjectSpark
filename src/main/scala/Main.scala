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

  def parseTweet(tweet: String): Tweet = {
    val json: JsValue = Json.parse(tweet)
      ???
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

  def preProcess(tweet:String):String = {
    val postfix = "')#('"
    if (tweet.endsWith(postfix)) {
      tweet.dropRight(postfix.length)
    } else {
      tweet
    }
  }

  val pathData =  "./data/tweets"
  val tweetsRDD = sc.textFile(pathData).map(preProcess).persist()    //persisting because we'll use this source multiple times so persisting avoids the data being read each time

  /*
  /*
  {"created_at":"Mon Sep 04 12:34:09 +0000 2017","id":904684005001269248,"id_str":"904684005001269248","text":"\u63a8\u3057\u304c\u53f3\u624b\u3067\u30dd\u30fc\u30ba\u3092\u53d6\u308a\u304c\u3061\u3002 https:\/\/t.co\/bNmQSC2Xog","display_text_range":[0,15],"source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":2930557759,"id_str":"2930557759","name":"\ud83d\ude08\u3042\u3086\u2693\ufe0f","screen_name":"CR7_AYU","location":"\u6e05\u6d41","url":"http:\/\/ameblo.jp\/shuka-saito\/","description":"\u9022\u7530\u68a8\u9999\u5b50\u3055\u3093\u3001\u6589\u85e4\u6731\u590f\u3055\u3093\u3001\u5c0f\u6797\u611b\u9999\u3055\u3093\u3092\u5fdc\u63f4\u3057\u3066\u3044\u307e\u3059\u3002","translator_type":"none","protected":false,"verified":false,"followers_count":564,"friends_count":644,"listed_count":54,"favourites_count":142433,"statuses_count":138352,"created_at":"Mon Dec 15 05:22:02 +0000 2014","utc_offset":32400,"time_zone":"Tokyo","geo_enabled":false,"lang":"ja","contributors_enabled":false,"is_translator":false,"profile_background_color":"00BFFF","profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/605381334823890944\/qdEfh3qD.jpg","profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/605381334823890944\/qdEfh3qD.jpg","profile_background_tile":true,"profile_link_color":"4D5AAF","profile_sidebar_border_color":"000000","profile_sidebar_fill_color":"000000","profile_text_color":"000000","profile_use_background_image":false,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/858259320038768640\/7tqZv7WS_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/858259320038768640\/7tqZv7WS_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/2930557759\/1503961622","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[],"media":[{"id":904683986949070848,"id_str":"904683986949070848","indices":[16,39],"media_url":"http:\/\/pbs.twimg.com\/media\/DI4VSvwVYAAQxsd.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DI4VSvwVYAAQxsd.jpg","url":"https:\/\/t.co\/bNmQSC2Xog","display_url":"pic.twitter.com\/bNmQSC2Xog","expanded_url":"https:\/\/twitter.com\/CR7_AYU\/status\/904684005001269248\/photo\/1","type":"photo","sizes":{"medium":{"w":1200,"h":1200,"resize":"fit"},"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":680,"h":680,"resize":"fit"},"large":{"w":2048,"h":2048,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":904683986949070848,"id_str":"904683986949070848","indices":[16,39],"media_url":"http:\/\/pbs.twimg.com\/media\/DI4VSvwVYAAQxsd.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DI4VSvwVYAAQxsd.jpg","url":"https:\/\/t.co\/bNmQSC2Xog","display_url":"pic.twitter.com\/bNmQSC2Xog","expanded_url":"https:\/\/twitter.com\/CR7_AYU\/status\/904684005001269248\/photo\/1","type":"photo","sizes":{"medium":{"w":1200,"h":1200,"resize":"fit"},"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":680,"h":680,"resize":"fit"},"large":{"w":2048,"h":2048,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"ja","timestamp_ms":"1504528449665"}
  */
  val secondTweet = tweetsRDD.take(2).tail.head
  println("second tweet : ", secondTweet)
  println("second tweet is embedded?: ", isEmbedded(secondTweet))

  /*
    {"created_at":"Mon Sep 04 12:34:10 +0000 2017","id":904684009187356673,"id_str":"904684009187356673","text":"RT @tylerthecreator: ah jus did dat","source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":733859871016267776,"id_str":"733859871016267776","name":"\ud83c\udf10","screen_name":"melodie_mxx","location":null,"url":null,"description":"\u2728\u2728\u2728\u2728\u2728\u2728\u2601\ufe0f\ud83c\udf19\u2601\ufe0f\u2728\u2728\u2728\u2728\u2728\u2728","translator_type":"none","protected":false,"verified":false,"followers_count":270,"friends_count":624,"listed_count":1,"favourites_count":4798,"statuses_count":5942,"created_at":"Sat May 21 03:20:02 +0000 2016","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"000000","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":false,"profile_link_color":"981CEB","profile_sidebar_border_color":"000000","profile_sidebar_fill_color":"000000","profile_text_color":"000000","profile_use_background_image":false,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/903399053567942656\/SK8p5CkX_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/903399053567942656\/SK8p5CkX_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/733859871016267776\/1504222094","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"retweeted_status":{"created_at":"Mon Sep 04 07:35:11 +0000 2017","id":904608766770823168,"id_str":"904608766770823168","text":"ah jus did dat","source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":166747718,"id_str":"166747718","name":"Tyler, The Creator","screen_name":"tylerthecreator","location":"november","url":"http:\/\/campfloggnaw.com","description":"2017","translator_type":"none","protected":false,"verified":true,"followers_count":4806266,"friends_count":176,"listed_count":8081,"favourites_count":263,"statuses_count":39874,"created_at":"Wed Jul 14 22:32:25 +0000 2010","utc_offset":-25200,"time_zone":"Arizona","geo_enabled":false,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"75D1FF","profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/685727329729970176\/bNxMsXKn.jpg","profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/685727329729970176\/bNxMsXKn.jpg","profile_background_tile":true,"profile_link_color":"F58EA8","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"FFFFFF","profile_text_color":"00CCFF","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/877956152834875392\/Mz3B8nGw_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/877956152834875392\/Mz3B8nGw_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/166747718\/1499624230","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":52,"reply_count":91,"retweet_count":1532,"favorite_count":5207,"entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"en"},"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[{"screen_name":"tylerthecreator","name":"Tyler, The Creator","id":166747718,"id_str":"166747718","indices":[3,19]}],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"en","timestamp_ms":"1504528450663"}')#('
    this tweet is a retweet ("retweeted_status": xyz)
   */

  val fourthTweet =  tweetsRDD.take(4).tail.tail.tail.head
  println("fourth tweet: ", fourthTweet)
  println("fourth tweet is embedded?: ", isEmbedded(fourthTweet))
  */

  val first10Tweets = tweetsRDD.take(10)
  first10Tweets.zipWithIndex.foreach{ case (tweetstr, index) =>
    print(s"${index+1}'th tweet embedded?", isEmbedded(tweetstr))
    if(isEmbedded(tweetstr)) print(s" original: ${getOriginal(tweetstr)}")
    println()

  }

}
