import java.io.File

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials, AWSCredentialsProvider}

/**
  * Created by keerathj on 18/7/16.
  */
object S3Test extends App{

  val s3Util = new S3Util(new BasicAWSCredentials("foo", "bar"))
  val start = System.currentTimeMillis()
  s3Util.putObjectAsMultiPart("rdrawlogdata", new File("/home/keerathj/train1.csv"))
  println(s"Time taken ${System.currentTimeMillis() - start}")
}
