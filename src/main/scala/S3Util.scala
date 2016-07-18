import java.io.File

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{AbortMultipartUploadRequest, CompleteMultipartUploadRequest, InitiateMultipartUploadRequest, PartETag, UploadPartRequest}
import com.amazonaws.services.s3.transfer.TransferManager

import scala.collection.mutable.ListBuffer

class S3Util(aWSCredentials: AWSCredentials) {

  val PART_SIZE = 5 * 1024 * 1024L
  private val clientConfig: ClientConfiguration = new ClientConfiguration()
  clientConfig.setConnectionTimeout(0)
  clientConfig.setSocketTimeout(0)
  val s3Client = new AmazonS3Client(aWSCredentials, clientConfig)
  val transferManager = new TransferManager(aWSCredentials)

  def putObjectAsMultiPart(bucketName: String, file: File): Unit = putObjectAsMultiPart(bucketName, file, PART_SIZE)

  private def putObjectAsMultiPart(bucketName: String, file: File, pPartSize: Long): Unit = {
    val eTags = ListBuffer[PartETag]()
    val uploaders = ListBuffer[MultiPartFileUploader]()

    val initRequest = new InitiateMultipartUploadRequest(bucketName, file.getName)
    val initResponse = s3Client.initiateMultipartUpload(initRequest)
    val contentLength = file.length()

    println(contentLength)

    try {
      var filePosition: Long = 0L
      var i: Int = 1
      while (filePosition < contentLength) {
        val partSize = Math.min(pPartSize, contentLength - filePosition)

        val uploadRequest = new UploadPartRequest().withBucketName(bucketName)
          .withKey(file.getName).withUploadId(initResponse.getUploadId)
          .withPartNumber(i).withFileOffset(filePosition).withFile(file)
          .withPartSize(partSize)
        /*uploadRequest.setGeneralProgressListener(new UploadProgressListener(file, i, partSize))*/

        s3Client.uploadPart(uploadRequest)
        eTags += s3Client.uploadPart(uploadRequest).getPartETag


        filePosition += partSize
        i += 1
      }

        /*val uploader = new MultiPartFileUploader(uploadRequest)
        uploaders += uploader
        uploader.upload()
      }

      uploaders foreach { uploader =>
        uploader.join()
        eTags += uploader.getPartETag
      }*/

      import scala.collection.JavaConverters._
      val completeRequest = new CompleteMultipartUploadRequest(bucketName, file.getName, initResponse.getUploadId, eTags.toList.asJava)
      s3Client.completeMultipartUpload(completeRequest)
    } catch {
      case ex: Exception => ex.printStackTrace()
        s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, file.getName, initResponse.getUploadId))
    }
  }

  class MultiPartFileUploader(uploadRequest: UploadPartRequest) extends Thread {

    private var partETag: PartETag = null

    override def run(): Unit = {
      partETag = s3Client.uploadPart(uploadRequest).getPartETag
    }

    def getPartETag: PartETag = {
      partETag
    }

    def upload(): Unit = {
      start()
    }
  }
}

/*class UploadProgressListener(file: File, partNo: Int, partLength: Long) extends ProgressListener {

  def progressChanged(progressEvent: ProgressEvent) {
    progressEvent.getEventCode match {
      case ProgressEvent.STARTED_EVENT_CODE =>
        println("Upload started for file " + "\"" + file.getName + "\"")

      case ProgressEvent.COMPLETED_EVENT_CODE =>
        println("Upload completed for file " + "\"" + file.getName + "\"" + ", " + file.length + " bytes data has been transferred")

      case ProgressEvent.FAILED_EVENT_CODE =>
        println("Upload failed for file " + "\"" + file.getName + "\"" + ", " + progressEvent.getBytesTransferred + " bytes data has been transferred")

      case ProgressEvent.CANCELED_EVENT_CODE =>
        println("Upload cancelled for file " + "\"" + file.getName + "\"" + ", " + progressEvent.getBytesTransferred + " bytes data has been transferred")

      case ProgressEvent.PART_STARTED_EVENT_CODE =>
        println("Upload started at " + partNo + ". part for file " + "\"" + file.getName + "\"")

      case ProgressEvent.PART_COMPLETED_EVENT_CODE =>
        println("Upload completed at " + partNo + ". part for file " + "\"" + file.getName + "\"" + ", " + (if (partLength > 0) partLength else progressEvent.getBytesTransferred) + " bytes data has been transferred")

      case ProgressEvent.PART_FAILED_EVENT_CODE =>
        println("Upload failed at " + partNo + ". part for file " + "\"" + file.getName + "\"" + ", " + progressEvent.getBytesTransferred + " bytes data has been transferred")

    }
  }*/
