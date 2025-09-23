#!/usr/bin/env python3
"""
간단한 추천 데이터 생성기
실제 Kafka 이벤트를 읽어서 Redis에 추천 데이터 생성
"""

import json
import redis
import random
import time
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleRecommendationGenerator:
    def __init__(self):
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        self.consumer = KafkaConsumer(
            'user-events-all',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='recommendation-generator',
            auto_offset_reset='latest'
        )

        # 사용자별 상호작용 추적
        self.user_interactions = {}
        self.product_stats = {}

    def process_event(self, event):
        """이벤트 처리"""
        user_id = event['user_id']
        product_id = event['product_id']
        event_type = event['event_type']

        # 사용자 상호작용 기록
        if user_id not in self.user_interactions:
            self.user_interactions[user_id] = {}

        if product_id not in self.user_interactions[user_id]:
            self.user_interactions[user_id][product_id] = 0

        # 이벤트 타입별 가중치
        weights = {'view': 1, 'cart': 3, 'like': 4, 'purchase': 5}
        weight = weights.get(event_type, 1)

        self.user_interactions[user_id][product_id] += weight

        # 상품 통계 업데이트
        if product_id not in self.product_stats:
            self.product_stats[product_id] = {'interactions': 0, 'users': set()}

        self.product_stats[product_id]['interactions'] += 1
        self.product_stats[product_id]['users'].add(user_id)

    def generate_recommendations(self, user_id, num_recs=10):
        """사용자별 추천 생성"""
        user_prefs = self.user_interactions.get(user_id, {})

        if not user_prefs:
            # 새 사용자면 인기 상품 추천
            popular_products = sorted(
                self.product_stats.items(),
                key=lambda x: x[1]['interactions'],
                reverse=True
            )[:num_recs]

            recommendations = []
            for product_id, stats in popular_products:
                score = min(5.0, stats['interactions'] / 10)  # 정규화
                recommendations.append({
                    'product_id': product_id,
                    'score': round(score, 2)
                })
            return recommendations

        # 기존 사용자면 유사도 기반 추천
        recommendations = []
        for product_id, score in sorted(user_prefs.items(), key=lambda x: x[1], reverse=True):
            if len(recommendations) < num_recs:
                normalized_score = min(5.0, score / 2)  # 정규화
                recommendations.append({
                    'product_id': product_id,
                    'score': round(normalized_score, 2)
                })

        # 부족하면 인기 상품으로 채움
        if len(recommendations) < num_recs:
            popular_products = sorted(
                self.product_stats.items(),
                key=lambda x: x[1]['interactions'],
                reverse=True
            )

            for product_id, stats in popular_products:
                if product_id not in user_prefs and len(recommendations) < num_recs:
                    score = min(4.0, stats['interactions'] / 15)
                    recommendations.append({
                        'product_id': product_id,
                        'score': round(score, 2)
                    })

        return recommendations

    def generate_trending(self):
        """트렌딩 상품 생성"""
        trending = []
        for product_id, stats in sorted(
            self.product_stats.items(),
            key=lambda x: x[1]['interactions'],
            reverse=True
        )[:20]:
            trending.append({
                'product_id': product_id,
                'score': round(stats['interactions'] * 2.5, 1),
                'interactions': stats['interactions'],
                'unique_users': len(stats['users']),
                'purchases': max(1, stats['interactions'] // 5)
            })

        return trending

    def save_to_redis(self):
        """Redis에 추천 데이터 저장"""
        # 각 사용자별 추천 저장
        for user_id in self.user_interactions:
            recommendations = self.generate_recommendations(user_id)
            if recommendations:
                key = f"recommendations:{user_id}"
                self.redis_client.setex(key, 3600, json.dumps(recommendations))

        # 트렌딩 상품 저장
        trending = self.generate_trending()
        if trending:
            self.redis_client.setex("trending_products", 300, json.dumps(trending))

        logger.info(f"Updated recommendations for {len(self.user_interactions)} users")
        logger.info(f"Updated trending with {len(trending)} products")

    def run(self):
        """메인 실행 루프"""
        logger.info("Starting simple recommendation generator...")

        batch_count = 0

        try:
            for message in self.consumer:
                event = message.value
                self.process_event(event)
                batch_count += 1

                # 100개 이벤트마다 Redis 업데이트
                if batch_count % 100 == 0:
                    self.save_to_redis()
                    logger.info(f"Processed {batch_count} events")

        except KeyboardInterrupt:
            logger.info("Stopping recommendation generator...")
        except Exception as e:
            logger.error(f"Error in recommendation generator: {e}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    generator = SimpleRecommendationGenerator()
    generator.run()