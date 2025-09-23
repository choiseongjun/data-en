#!/usr/bin/env python3
"""
간단한 Spark 추천 엔진 실행 스크립트
Kafka 스트림에서 사용자 이벤트를 읽어 실시간 추천을 생성합니다.
"""

import subprocess
import sys
import time
import logging
import json
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_spark_master():
    """Spark 마스터 상태 확인"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'spark-master',
            'curl', '-s', 'http://localhost:8080'
        ], capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except:
        return False

def submit_spark_job():
    """Spark 작업 제출"""
    try:
        logger.info("Submitting Spark recommendation job...")

        # Spark submit 명령
        cmd = [
            'docker', 'exec', 'spark-master',
            'spark-submit',
            '--master', 'spark://spark-master:7077',
            '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0',
            '--driver-memory', '1g',
            '--executor-memory', '1g',
            '/opt/bitnami/spark/jobs/recommendation_engine.py'
        ]

        # 백그라운드에서 실행
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Spark job submitted with PID: {process.pid}")

        return process

    except Exception as e:
        logger.error(f"Failed to submit Spark job: {e}")
        return None

def create_simple_recommendations():
    """간단한 추천 데이터 생성 (Spark 대안)"""
    logger.info("Creating simple recommendations as fallback...")

    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # 샘플 사용자들에 대한 추천 생성
    users = [f"user_{str(i).zfill(6)}" for i in range(1, 101)]
    products = [f"prod_{str(i).zfill(6)}" for i in range(1, 1001)]

    import random

    for user_id in users:
        # 각 사용자에게 5-10개의 추천 상품 생성
        num_recs = random.randint(5, 10)
        recommendations = []

        selected_products = random.sample(products, num_recs)

        for product_id in selected_products:
            score = round(random.uniform(2.0, 5.0), 2)
            recommendations.append({
                "product_id": product_id,
                "score": score
            })

        # 점수 순으로 정렬
        recommendations.sort(key=lambda x: x['score'], reverse=True)

        # Redis에 저장 (1시간 TTL)
        key = f"recommendations:{user_id}"
        redis_client.setex(key, 3600, json.dumps(recommendations))

    # 트렌딩 상품 생성
    trending_products = []
    for i in range(20):
        product_id = random.choice(products)
        trending_products.append({
            "product_id": product_id,
            "score": round(random.uniform(70.0, 100.0), 1),
            "interactions": random.randint(50, 200),
            "unique_users": random.randint(20, 80),
            "purchases": random.randint(5, 25)
        })

    trending_products.sort(key=lambda x: x['score'], reverse=True)
    redis_client.setex("trending_products", 300, json.dumps(trending_products))

    logger.info(f"Created recommendations for {len(users)} users and {len(trending_products)} trending products")

def main():
    logger.info("Starting recommendation engine setup...")

    # Spark 마스터 상태 확인
    if not check_spark_master():
        logger.warning("Spark master not accessible, using simple recommendation fallback")
        create_simple_recommendations()
        return

    # Spark 작업 제출 시도
    process = submit_spark_job()

    if process:
        logger.info("Spark job running. Waiting for initialization...")
        time.sleep(30)  # 초기화 대기

        # 프로세스 상태 확인
        if process.poll() is None:
            logger.info("Spark job is running successfully!")
            logger.info("The recommendation engine will continuously process Kafka events.")
            logger.info("Check Redis for real-time recommendations.")
        else:
            logger.warning("Spark job failed, using simple recommendation fallback")
            create_simple_recommendations()
    else:
        logger.warning("Could not start Spark job, using simple recommendation fallback")
        create_simple_recommendations()

if __name__ == "__main__":
    main()