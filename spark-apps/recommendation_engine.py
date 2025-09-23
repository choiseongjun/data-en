from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
import redis
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("EcommerceRecommendationEngine") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Redis 연결
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

        # 스키마 정의
        self.event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("device", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("view_duration", IntegerType(), True)
        ])

    def create_kafka_stream(self):
        """Kafka 스트림 생성"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "user-events-all") \
            .option("startingOffsets", "latest") \
            .load()

    def parse_kafka_data(self, kafka_df):
        """Kafka 메시지 파싱"""
        return kafka_df.select(
            from_json(col("value").cast("string"), self.event_schema).alias("data")
        ).select("data.*")

    def calculate_user_item_ratings(self, events_df):
        """사용자-상품 평점 계산"""
        # 이벤트 타입별 가중치
        rating_weights = {
            'view': 1.0,
            'cart': 3.0,
            'like': 4.0,
            'purchase': 5.0
        }

        # 이벤트 타입에 따른 가중치 적용
        weighted_events = events_df.withColumn(
            "rating",
            when(col("event_type") == "view", lit(1.0))
            .when(col("event_type") == "cart", lit(3.0))
            .when(col("event_type") == "like", lit(4.0))
            .when(col("event_type") == "purchase", lit(5.0))
            .otherwise(lit(1.0))
        )

        # 사용자-상품별 평점 집계
        user_item_ratings = weighted_events.groupBy("user_id", "product_id") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("*").alias("interaction_count")
            ) \
            .withColumn("final_rating",
                       col("avg_rating") * log(col("interaction_count") + 1)
            )

        return user_item_ratings

    def train_als_model(self, ratings_df):
        """ALS 모델 훈련"""
        # String Indexer for user_id and product_id
        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
        product_indexer = StringIndexer(inputCol="product_id", outputCol="product_index")

        # Apply indexers
        indexed_df = user_indexer.fit(ratings_df).transform(ratings_df)
        indexed_df = product_indexer.fit(indexed_df).transform(indexed_df)

        # ALS 모델 설정
        als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol="user_index",
            itemCol="product_index",
            ratingCol="final_rating",
            coldStartStrategy="drop"
        )

        # 모델 훈련
        model = als.fit(indexed_df)

        return model, user_indexer.fit(ratings_df), product_indexer.fit(indexed_df)

    def generate_recommendations(self, model, user_indexer, product_indexer, num_recommendations=10):
        """추천 생성"""
        # 모든 사용자에 대한 추천 생성
        user_recs = model.recommendForAllUsers(num_recommendations)

        # 인덱스를 원래 ID로 변환
        user_index_to_id = user_indexer.labels
        product_index_to_id = product_indexer.labels

        def convert_recommendations(row):
            user_id = user_index_to_id[row.user_index]
            recommendations = []

            for rec in row.recommendations:
                product_id = product_index_to_id[rec.product_index]
                score = float(rec.rating)
                recommendations.append({
                    "product_id": product_id,
                    "score": score
                })

            return user_id, recommendations

        return user_recs.rdd.map(convert_recommendations)

    def save_recommendations_to_redis(self, recommendations_rdd):
        """추천 결과를 Redis에 저장"""
        def save_to_redis(partition):
            redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

            for user_id, recommendations in partition:
                # 사용자별 추천 저장
                key = f"recommendations:{user_id}"
                redis_client.setex(
                    key,
                    3600,  # 1시간 TTL
                    json.dumps(recommendations)
                )

                # 추천 생성 시간 저장
                redis_client.setex(
                    f"recommendations_updated:{user_id}",
                    3600,
                    str(int(time.time()))
                )

        recommendations_rdd.foreachPartition(save_to_redis)

    def calculate_trending_products(self, events_df):
        """실시간 트렌딩 상품 계산"""
        # 최근 시간별 상품 상호작용 계산
        current_time = current_timestamp()

        recent_events = events_df.filter(
            col("timestamp") > date_sub(current_time, 1)  # 최근 1일
        )

        trending = recent_events.groupBy("product_id") \
            .agg(
                count("*").alias("total_interactions"),
                countDistinct("user_id").alias("unique_users"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
            ) \
            .withColumn("trending_score",
                       col("total_interactions") * 0.3 +
                       col("unique_users") * 0.5 +
                       col("purchases") * 0.2
            ) \
            .orderBy(desc("trending_score")) \
            .limit(50)

        return trending

    def save_trending_to_redis(self, trending_df):
        """트렌딩 상품을 Redis에 저장"""
        trending_list = trending_df.collect()

        trending_products = []
        for row in trending_list:
            trending_products.append({
                "product_id": row.product_id,
                "score": float(row.trending_score),
                "interactions": row.total_interactions,
                "unique_users": row.unique_users,
                "purchases": row.purchases
            })

        self.redis_client.setex(
            "trending_products",
            300,  # 5분 TTL
            json.dumps(trending_products)
        )

    def process_stream(self):
        """스트림 처리 메인 함수"""
        logger.info("Starting recommendation engine stream processing...")

        # Kafka 스트림 생성
        kafka_stream = self.create_kafka_stream()
        parsed_stream = self.parse_kafka_data(kafka_stream)

        # 윈도우 기반 처리 (10분 윈도우, 5분 슬라이딩)
        windowed_events = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes", "5 minutes"),
                "user_id", "product_id", "event_type"
            ) \
            .count() \
            .select("window.*", "user_id", "product_id", "event_type", "count")

        def process_batch(batch_df, batch_id):
            try:
                logger.info(f"Processing batch {batch_id}")

                if batch_df.count() > 0:
                    # 사용자-상품 평점 계산
                    ratings_df = self.calculate_user_item_ratings(batch_df)

                    # 충분한 데이터가 있을 때만 모델 훈련
                    if ratings_df.count() > 100:
                        logger.info("Training ALS model...")
                        model, user_indexer, product_indexer = self.train_als_model(ratings_df)

                        # 추천 생성
                        recommendations = self.generate_recommendations(
                            model, user_indexer, product_indexer
                        )

                        # Redis에 저장
                        self.save_recommendations_to_redis(recommendations)
                        logger.info("Recommendations saved to Redis")

                    # 트렌딩 상품 계산
                    trending = self.calculate_trending_products(batch_df)
                    self.save_trending_to_redis(trending)
                    logger.info("Trending products updated")

            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {e}")

        # 스트림 쿼리 시작
        query = windowed_events.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .trigger(processingTime='5 minutes') \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        return query

    def run(self):
        """추천 엔진 실행"""
        try:
            query = self.process_stream()
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping recommendation engine...")
        except Exception as e:
            logger.error(f"Error in recommendation engine: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    import time
    engine = RecommendationEngine()
    engine.run()