package de.ktmeindl.yelp

object Constants {

  val PROP_FILE               = "yelp.properties"

  //// some constants
  // naming of instances
  val CHECKIN                 = "checkin"
  val REVIEW                  = "review"
  val BUSINESS                = "business"
  val TIP                     = "tip"
  val USER                    = "user"

  // file names for analysis
  val FILE_CHECKIN            = "yelp_academic_dataset_checkin.json"
  val FILE_BUSINESS           = "yelp_academic_dataset_business.json"
  val FILE_REVIEW             = "yelp_academic_dataset_review.json"
  val FILE_TIP                = "yelp_academic_dataset_tip.json"
  val FILE_USER               = "yelp_academic_dataset_user.json"


  // column names in yelp data
  val COL_BUSINESS_ID         = "business_id"
  val COL_REVIEW_ID           = "review_id"
  val COL_USER_ID             = "user_id"
  val COL_TIME                = "time"
  val COL_UUID : String       = "key_uuid"


  // possible storage types
  val TYPE_CASSANDRA          = "cassandra"
  val TYPE_FILE               = "file"
  val TYPE_HDFS               = "hdfs"


  //// properties in PROP_FILE
  val SHOULD_UNTAR            = "should.untar.files"
  val DATA_DIR                = "untarred.files.dir"
  val STORAGE_TYPE            = "storage.type"
  val CASSANDRA_HOSTS         = "cassandra.hosts"

  // cassandra keyspace and name
  val C_KEYSPACE              = "cassandra.keyspace"
  val C_CHECKIN               = "cassandra.table.checkin"
  val C_REVIEW                = "cassandra.table.review"
  val C_BUSINESS              = "cassandra.table.business"
  val C_TIP                   = "cassandra.table.tip"
  val C_USER                  = "cassandra.table.user"

  // in case of s3 data storage
  val S3_ENDPOINT             = "s3.aws.endpoint"
  val S3_ID                   = "s3.aws.access.id"
  val S3_KEY                  = "s3.aws.access.key"

  //// default values
  // default cassandra keyspace and table names
  val C_KEYSPACE_DEFAULT      = "yelp_dataset"
  val C_CHECKIN_DEFAULT       = CHECKIN
  val C_REVIEW_DEFAULT        = REVIEW
  val C_BUSINESS_DEFAULT      = BUSINESS
  val C_TIP_DEFAULT           = TIP
  val C_USER_DEFAULT          = USER



}
