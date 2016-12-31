package edu.gmu.stc.climatespark.application.twitter

import org.apache.commons.cli.{Options, ParseException, PosixParser}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by Fei Hu on 12/24/16.
  */
object Utils {

  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  val CONSUMER_KEY = "consumerKey"
  val CONSUMER_SECRET = "consumerSecret"
  val ACCESS_TOKEN = "accessToken"
  val ACCESS_TOKEN_SECRET = "accessTokenSecret"

  val apiKey = "HKPkAYw8vRVoRAw2In2BpxLEU"
  val apiSecret = "8UkAOsotiz0aEmQ9t8QpTe87pmWlGWTpUtUinAg6NsZV5fxwl6"
  val accessToken = "2234110302-E54jHBGVTDXtxu9MptRk1WZR1nm7N25Wz5JxUTf"
  val accessTokenSecret = "9jX5jo4v4onTPDMjAWWIv6JAgjTm2Coejk5ltsLr2rqPW"

  val THE_OPTIONS = {
    val options = new Options()
    options.addOption(CONSUMER_KEY, true, "Twitter OAuth Consumer Key")
    options.addOption(CONSUMER_SECRET, true, "Twitter OAuth Consumer Secret")
    options.addOption(ACCESS_TOKEN, true, "Twitter OAuth Access Token")
    options.addOption(ACCESS_TOKEN_SECRET, true, "Twitter OAuth Access Token Secret")
    options
  }

  def parseCommandLineWithTwitterCredentials(args: Array[String]) = {
    val parser = new PosixParser
    try {
      val cl = parser.parse(THE_OPTIONS, args)
      System.setProperty("twitter4j.oauth.consumerKey", apiKey)
      System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
      cl.getArgList.toArray
    } catch {
      case e: ParseException =>
        System.err.println("Parsing failed.  Reason: " + e.getMessage)
        System.exit(1)
    }
  }

  def getAuth = {
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }

  /**
    * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
    * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
    * This is a common way to decrease the number of features in a model while still
    * getting excellent accuracy (otherwise every pair of Unicode characters would
    * potentially be a feature).
    */
  def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }

  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }

}
