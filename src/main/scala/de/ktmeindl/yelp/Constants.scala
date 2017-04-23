package de.ktmeindl.yelp

object Constants {

  val PROP_FILE               = "yelp.properties"

  //// properties in PROP_FILE
  val CASSANDRA_HOSTS         = "cassandra.hosts"
  val SHOULD_UNTAR            = "should.untar.files"
  val DATA_DIR                = "untared.files.dir"

  // cassandra keyspace and name
  val C_KEYSPACE              = "cassandra.keyspace"
  val C_CHECKIN               = "cassandra.table.checkin"
  val C_REVIEW                = "cassandra.table.review"
  val C_BUSINESS              = "cassandra.table.business"
  val C_TIP                   = "cassandra.table.tip"
  val C_USER                  = "cassandra.table.user"

  // default cassandra keyspace and table names
  val C_KEYSPACE_DEFAULT      = "yelp_dataset"
  val C_CHECKIN_DEFAULT       = "checkin"
  val C_REVIEW_DEFAULT        = "review"
  val C_BUSINESS_DEFAULT      = "business"
  val C_TIP_DEFAULT           = "tip"
  val C_USER_DEFAULT          = "user"

  // file names for analysis
  val CHECKIN_FILE            = "yelp_academic_dataset_checkin.json"
  val BUSINESS_FILE           = "yelp_academic_dataset_business.json"
  val REVIEW_FILE             = "yelp_academic_dataset_review.json"
  val TIP_FILE                = "yelp_academic_dataset_tip.json"
  val USER_FILE               = "yelp_academic_dataset_user.json"


  val COL_BUSINESS_ID         = "business_id"
  val COL_REVIEW_ID           = "review_id"
  val COL_USER_ID             = "user_id"
  val COL_TIME                = "time"
  val COL_UUID : String       = "key_uuid"

}
