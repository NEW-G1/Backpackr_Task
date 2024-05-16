import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object UserActivityLog {
  def main(args: Array[String]): Unit = {
    
    // Spark 세션 생성
    val spark = SparkSession.builder()
      .appName("UserActivityLog")
      .enableHiveSupport()
      .getOrCreate()

    // 입력 경로, 출력 경로, 체크포인트 경로, 트랜잭션 로그 경로 설정
    val inputPath = "hdfs:///user/hive/warehouse/temp/input"
    val outputPath = "hdfs:///user/hive/warehouse/temp/output"
    val checkpointPath = "hdfs:///user/hive/warehouse/temp/checkpoint"
    val transactionLogPath = "hdfs:///user/hive/warehouse/temp/transaction_log"

    // 체크포인트 디렉토리 설정
    spark.sparkContext.setCheckpointDir(checkpointPath)

    // 입력 데이터의 스키마 정의
    val schema = StructType(Array(
      StructField("event_time", StringType),
      StructField("event_type", StringType),
      StructField("product_id", LongType),
      StructField("category_id", LongType),
      StructField("category_code", StringType),
      StructField("brand", StringType),
      StructField("price", DoubleType),
      StructField("user_id", LongType),
      StructField("user_session", StringType)
    ))

    // CSV 파일 읽어서 DataFrame 생성
    val userActLogDF = spark.read
      .schema(schema)
      .option("header", "true")
      .csv(inputPath)

    // DataFrame의 첫 10개 레코드 확인
    userActLogDF.show(10)

    // event_time 컬럼을 UTC에서 KST로 변환
    val utcTokstDF = userActLogDF
      .withColumn("event_time", date_format(from_utc_timestamp(col("event_time"), "Asia/Seoul"), "yyyy-MM-dd HH:mm:ss"))

    // KST 기준 날짜 컬럼 추가
    val addKstDF = utcTokstDF.withColumn("KST_date", to_date(col("event_time")))

    // 변환된 DataFrame의 첫 10개 레코드 확인
    addKstDF.show(10)

    // 필요한 컬럼만 선택하여 새로운 DataFrame 생성
    val resultDF = addKstDF
      .select(
        col("KST_date"),
        col("event_time"),
        col("event_type"),
        col("product_id"),
        col("category_id"),
        col("category_code"),
        col("brand"),
        col("price"),
        col("user_id"),
        col("user_session")
      )

    // 중복 데이터 제거
    val resultDropDuplicatesDF = resultDF.dropDuplicates()

    // 체크포인트 설정을 통해 장애 발생 시 복구 지점 설정
    resultDropDuplicatesDF.checkpoint()

    // 장애 발생 여부에 따른 처리 분기
    val isFailureOccurred = true

    if (isFailureOccurred) {
      // 장애 발생 시 처리 로직
      println("장애 발생 시 복구 프로세스 실행")

      // Case 1: 트랜잭션 로그를 활용한 장애 복구
      // 트랜잭션 로그에 데이터 추가 (append mode)
      resultDropDuplicatesDF.write
        .format("csv")
        .option("header", "true")
        .mode(SaveMode.Append)
        .save(transactionLogPath)

      // Case 2: 재처리 로직을 통한 장애 복구
      // 특정 날짜 이후의 데이터 필터링하여 재처리
      val reprocessFromDate = "2019-10-01"
      val reprocessDF = resultDropDuplicatesDF.filter(col("KST_date") >= lit(reprocessFromDate))

      reprocessDF.write
        .partitionBy("KST_date")
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet(outputPath)
    } else {
      // 장애 미발생 시 처리 로직

      // KST_date 컬럼으로 파티셔닝하여 parquet 포맷으로 저장, snappy 압축 적용
      resultDropDuplicatesDF.write
        .partitionBy("KST_date")
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet(outputPath)
    }

    // Hive 외부 테이블 생성
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS user_activity_log (
        event_time STRING,
        event_type STRING,
        product_id LONG,
        category_id LONG,
        category_code STRING,
        brand STRING,
        price DOUBLE,
        user_id LONG,
        user_session STRING
      )
      PARTITIONED BY (KST_date STRING)
      STORED AS PARQUET
      LOCATION '$outputPath'
    """)

    // Hive 메타스토어에 파티션 정보 업데이트
    spark.sql("MSCK REPAIR TABLE user_activity_log")

    // 생성된 외부 테이블의 상세 정보 확인
    spark.sql("DESCRIBE FORMATTED user_activity_log").show()

    // 외부 테이블에서 데이터 조회
    spark.sql("SELECT * FROM user_activity_log").show(10)

    spark.stop()
  }
}