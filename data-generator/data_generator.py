import json
import time
import random
import math
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class EcommerceDataGenerator:
    def __init__(self):
        # Kafka Producer 설정
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )

        # Elasticsearch 연결
        self.es = Elasticsearch(['http://elasticsearch:9200'])

        # Redis 연결
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

        # PostgreSQL 연결
        self.pg_conn = psycopg2.connect(
            host='postgres',
            database='ecommerce',
            user='postgres',
            password='postgres'
        )
        self.pg_conn.autocommit = True

        # 상품 카테고리
        self.categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden',
            'Sports', 'Beauty', 'Toys', 'Automotive', 'Food', 'Health'
        ]

        # 브랜드
        self.brands = [
            'Samsung', 'Apple', 'Nike', 'Adidas', 'Sony', 'LG',
            'Zara', 'H&M', 'Canon', 'Dell', 'HP', 'Lenovo'
        ]

        # 사용자 행동 타입
        self.event_types = ['view', 'cart', 'purchase', 'like', 'search']

        # 가중치 (주문 생성 빠르게 - purchase 30%로 증가)
        self.event_weights = [0.3, 0.2, 0.3, 0.1, 0.1]

        self.products = []
        self.users = []

        # 프로모션 이벤트 상태
        self.promotion_active = False
        self.promotion_end_time = None

    def generate_products(self, num_products=1000):
        """상품 데이터 생성 및 PostgreSQL과 Elasticsearch에 저장"""
        logger.info(f"Generating {num_products} products...")

        with self.pg_conn.cursor() as cursor:
            for i in range(num_products):
                category = random.choice(self.categories)
                brand = random.choice(self.brands)

                product = {
                    'product_id': f'prod_{i+1:06d}',
                    'name': fake.catch_phrase(),
                    'category': category,
                    'brand': brand,
                    'price': round(random.uniform(10, 1000), 2),
                    'rating': round(random.uniform(1, 5), 1),
                    'description': fake.text(max_nb_chars=200),
                    'tags': [fake.word() for _ in range(random.randint(2, 5))],
                    'created_at': fake.date_time_between(start_date='-1y', end_date='now').isoformat()
                }
                self.products.append(product)

                # PostgreSQL에 저장
                try:
                    # 카테고리 ID 조회/생성
                    cursor.execute("SELECT category_id FROM categories WHERE name = %s", (category,))
                    category_result = cursor.fetchone()
                    if category_result:
                        category_id = category_result[0]
                    else:
                        cursor.execute("INSERT INTO categories (name) VALUES (%s) RETURNING category_id", (category,))
                        category_id = cursor.fetchone()[0]

                    # 브랜드 ID 조회/생성
                    cursor.execute("SELECT brand_id FROM brands WHERE name = %s", (brand,))
                    brand_result = cursor.fetchone()
                    if brand_result:
                        brand_id = brand_result[0]
                    else:
                        cursor.execute("INSERT INTO brands (name) VALUES (%s) RETURNING brand_id", (brand,))
                        brand_id = cursor.fetchone()[0]

                    # 상품 데이터 삽입
                    cursor.execute("""
                        INSERT INTO products (product_id, name, description, category_id, brand_id,
                                            price, rating, stock_quantity, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        product['product_id'], product['name'], product['description'],
                        category_id, brand_id, product['price'], product['rating'],
                        random.randint(10, 1000), product['created_at']
                    ))
                except Exception as e:
                    logger.warning(f"Failed to insert product {product['product_id']} to PostgreSQL: {e}")

                # Elasticsearch에 저장
                try:
                    self.es.index(index='products', id=product['product_id'], body=product)
                except Exception as e:
                    logger.warning(f"Failed to index product {product['product_id']}: {e}")

        logger.info(f"Generated and stored {len(self.products)} products")

    def generate_users(self, num_users=5000):
        """사용자 데이터 생성 및 PostgreSQL에 저장"""
        logger.info(f"Generating {num_users} users...")

        with self.pg_conn.cursor() as cursor:
            for i in range(num_users):
                user = {
                    'user_id': f'user_{i+1:06d}',
                    'name': fake.name(),
                    'email': fake.email(),
                    'age': random.randint(18, 70),
                    'gender': random.choice(['M', 'F']),
                    'location': fake.city(),
                    'signup_date': fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
                    'preferred_categories': random.sample(self.categories, k=random.randint(1, 3))
                }
                self.users.append(user)

                # PostgreSQL에 저장
                try:
                    cursor.execute("""
                        INSERT INTO users (user_id, name, email, age, gender, location, signup_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user['user_id'], user['name'], user['email'],
                        user['age'], user['gender'], user['location'], user['signup_date']
                    ))
                except Exception as e:
                    logger.warning(f"Failed to insert user {user['user_id']} to PostgreSQL: {e}")

        logger.info(f"Generated and stored {len(self.users)} users")

    def generate_user_behavior_event(self):
        """사용자 행동 이벤트 생성"""
        if not self.users or not self.products:
            return None

        user = random.choice(self.users)
        product = random.choice(self.products)
        event_type = np.random.choice(self.event_types, p=self.event_weights)

        event = {
            'event_id': fake.uuid4(),
            'user_id': user['user_id'],
            'product_id': product['product_id'],
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'session_id': fake.uuid4()[:8],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
        }

        # 이벤트 타입별 추가 정보
        if event_type == 'purchase':
            event['quantity'] = random.randint(1, 5)
            event['total_amount'] = round(product['price'] * event['quantity'], 2)
        elif event_type == 'search':
            event['search_query'] = fake.word()
            event['search_results_count'] = random.randint(0, 100)
        elif event_type == 'view':
            event['view_duration'] = random.randint(1, 300)  # seconds

        return event

    def generate_dynamic_user_behavior_event(self, dynamic_weights):
        """동적 가중치를 사용한 사용자 행동 이벤트 생성"""
        if not self.users or not self.products:
            return None

        user = random.choice(self.users)
        product = random.choice(self.products)
        event_type = np.random.choice(self.event_types, p=dynamic_weights)

        event = {
            'event_id': fake.uuid4(),
            'user_id': user['user_id'],
            'product_id': product['product_id'],
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'session_id': fake.uuid4()[:8],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
        }

        # 이벤트 타입별 추가 정보
        if event_type == 'purchase':
            quantity = random.randint(1, 3)  # 프로모션 시에는 더 많이 구매
            if self.promotion_active:
                quantity = random.randint(1, 5)  # 프로모션 중엔 더 많이!

            event['quantity'] = quantity
            event['total_amount'] = round(product['price'] * quantity, 2)
        elif event_type == 'search':
            event['search_query'] = fake.word()
            event['search_results_count'] = random.randint(0, 100)
        elif event_type == 'view':
            # 프로모션 중에는 더 오래 봄
            max_duration = 600 if self.promotion_active else 300
            event['view_duration'] = random.randint(1, max_duration)

        return event

    def send_to_kafka(self, event):
        """Kafka로 이벤트 전송"""
        try:
            # 토픽별로 이벤트 분산
            topic = f"user-events-{event['event_type']}"

            self.kafka_producer.send(
                topic=topic,
                key=event['user_id'],
                value=event
            )

            # 통합 토픽에도 전송
            self.kafka_producer.send(
                topic='user-events-all',
                key=event['user_id'],
                value=event
            )

        except Exception as e:
            logger.error(f"Failed to send event to Kafka: {e}")

    def update_user_stats(self, event):
        """Redis에 사용자 통계 업데이트"""
        try:
            user_id = event['user_id']

            # 사용자별 이벤트 카운트
            self.redis_client.hincrby(f"user_stats:{user_id}", "total_events", 1)
            self.redis_client.hincrby(f"user_stats:{user_id}", f"{event['event_type']}_count", 1)

            # 최근 활동 시간 업데이트
            self.redis_client.hset(f"user_stats:{user_id}", "last_activity", event['timestamp'])

            # 상품별 인기도 점수
            if event['event_type'] == 'view':
                self.redis_client.zincrby("popular_products", 1, event['product_id'])
            elif event['event_type'] == 'purchase':
                self.redis_client.zincrby("popular_products", 5, event['product_id'])
            elif event['event_type'] == 'like':
                self.redis_client.zincrby("popular_products", 3, event['product_id'])

        except Exception as e:
            logger.error(f"Failed to update Redis stats: {e}")

    def run(self):
        """데이터 생성 및 스트리밍 실행"""
        logger.info("Starting E-commerce Data Generator...")

        # 초기 데이터 생성
        self.generate_products(1000)
        self.generate_users(5000)

        # Kafka 토픽 생성 대기
        time.sleep(10)

        logger.info("Starting real-time event generation...")

        event_count = 0
        while True:
            try:
                # 동적 활동률 계산
                purchase_rate, sleep_interval = self.calculate_dynamic_activity_rate()
                dynamic_weights = self.get_dynamic_event_weights(purchase_rate)

                # 동적 가중치로 이벤트 생성
                event = self.generate_dynamic_user_behavior_event(dynamic_weights)
                if event:
                    # Kafka로 전송
                    self.send_to_kafka(event)

                    # Redis 통계 업데이트
                    self.update_user_stats(event)

                    # PostgreSQL에 행동 로그 저장
                    self.log_user_behavior(event)

                    # 이벤트 타입별 추가 처리
                    if event['event_type'] == 'purchase':
                        self.create_order_from_purchase_event(event)
                    elif event['event_type'] == 'cart':
                        self.add_to_cart(event)

                    event_count += 1
                    if event_count % 100 == 0:
                        logger.info(f"Generated {event_count} events")

                # 동적 간격으로 대기
                time.sleep(sleep_interval)

            except KeyboardInterrupt:
                logger.info("Stopping data generator...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)

        # 정리
        self.kafka_producer.close()
        self.pg_conn.close()

    def create_order_from_purchase_event(self, event):
        """구매 이벤트로부터 주문 생성"""
        try:
            with self.pg_conn.cursor() as cursor:
                # 주문 생성
                cursor.execute("""
                    INSERT INTO orders (user_id, total_amount, status, shipping_address, payment_method)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING order_id
                """, (
                    event['user_id'],
                    event.get('total_amount', 0),
                    random.choice(['pending', 'processing', 'shipped']),
                    fake.address(),
                    random.choice(['credit_card', 'debit_card', 'paypal'])
                ))
                order_id = cursor.fetchone()[0]

                # 주문 상품 항목 생성
                cursor.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    order_id,
                    event['product_id'],
                    event.get('quantity', 1),
                    event.get('total_amount', 0) / event.get('quantity', 1),
                    event.get('total_amount', 0)
                ))

                logger.info(f"Created order {order_id} for user {event['user_id']}")

        except Exception as e:
            logger.error(f"Failed to create order: {e}")

    def add_to_cart(self, event):
        """장바구니에 상품 추가"""
        try:
            with self.pg_conn.cursor() as cursor:
                # 기존 장바구니 아이템 확인
                cursor.execute("""
                    SELECT quantity FROM cart_items
                    WHERE user_id = %s AND product_id = %s
                """, (event['user_id'], event['product_id']))

                existing_item = cursor.fetchone()

                if existing_item:
                    # 기존 아이템이 있으면 수량 증가
                    new_quantity = existing_item[0] + 1
                    cursor.execute("""
                        UPDATE cart_items
                        SET quantity = %s, added_at = NOW()
                        WHERE user_id = %s AND product_id = %s
                    """, (new_quantity, event['user_id'], event['product_id']))
                    logger.info(f"Updated cart item for user {event['user_id']}, product {event['product_id']}, new quantity: {new_quantity}")
                else:
                    # 새로운 아이템 추가
                    cursor.execute("""
                        INSERT INTO cart_items (user_id, product_id, quantity)
                        VALUES (%s, %s, %s)
                    """, (event['user_id'], event['product_id'], 1))
                    logger.info(f"Added new item to cart for user {event['user_id']}, product {event['product_id']}")

        except Exception as e:
            logger.error(f"Failed to add to cart: {e}")

    def log_user_behavior(self, event):
        """사용자 행동 로그를 PostgreSQL에 저장"""
        try:
            with self.pg_conn.cursor() as cursor:
                action_mapping = {
                    'view': 'view',
                    'cart': 'cart_add',
                    'purchase': 'purchase',
                    'like': 'review',
                    'search': 'search'
                }

                cursor.execute("""
                    INSERT INTO user_behavior_log
                    (user_id, product_id, action_type, session_id, device_type,
                     ip_address, user_agent, search_query)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    event['user_id'],
                    event.get('product_id'),
                    action_mapping.get(event['event_type'], event['event_type']),
                    event.get('session_id'),
                    event.get('device'),
                    event.get('ip_address'),
                    event.get('user_agent'),
                    event.get('search_query')
                ))

        except Exception as e:
            logger.error(f"Failed to log user behavior: {e}")

    def get_hourly_activity_multiplier(self):
        """시간대별 활동 배율 계산"""
        current_hour = datetime.now().hour

        # 시간대별 활동 패턴 (0.1 = 10%, 10.0 = 1000%)
        hourly_patterns = {
            0: 0.5,  # 자정
            1: 0.2,  # 새벽 1시
            2: 0.1,  # 새벽 2시
            3: 0.1,  # 새벽 3시
            4: 0.1,  # 새벽 4시
            5: 0.3,  # 새벽 5시
            6: 0.8,  # 아침 6시
            7: 1.5,  # 아침 7시
            8: 2.0,  # 아침 8시
            9: 2.5,  # 오전 9시
            10: 3.0, # 오전 10시
            11: 4.0, # 오전 11시
            12: 5.0, # 점심 12시 (점심시간 쇼핑)
            13: 4.0, # 오후 1시
            14: 3.0, # 오후 2시
            15: 2.5, # 오후 3시
            16: 3.0, # 오후 4시
            17: 4.0, # 오후 5시
            18: 6.0, # 저녁 6시
            19: 8.0, # 저녁 7시 (퇴근 후 쇼핑)
            20: 10.0, # 저녁 8시 (최대 피크 타임)
            21: 8.0, # 저녁 9시
            22: 4.0, # 저녁 10시
            23: 2.0  # 밤 11시
        }

        return hourly_patterns.get(current_hour, 1.0)

    def get_weekly_activity_multiplier(self):
        """요일별 활동 배율 계산"""
        weekday = datetime.now().weekday()  # 0=월요일, 6=일요일

        weekly_patterns = {
            0: 1.0,  # 월요일
            1: 1.1,  # 화요일
            2: 1.2,  # 수요일
            3: 1.3,  # 목요일
            4: 1.8,  # 금요일 (주말 준비 쇼핑)
            5: 2.2,  # 토요일 (주말 쇼핑)
            6: 1.6   # 일요일
        }

        return weekly_patterns.get(weekday, 1.0)

    def check_promotion_event(self):
        """프로모션 이벤트 확인 및 시작"""
        now = datetime.now()

        # 기존 프로모션이 끝났는지 확인
        if self.promotion_active and now > self.promotion_end_time:
            self.promotion_active = False
            logger.info("🎉 Promotion event ended")

        # 새로운 프로모션 시작 (10% 확률로 5분간 프로모션)
        if not self.promotion_active and random.random() < 0.10:
            self.promotion_active = True
            self.promotion_end_time = now + timedelta(minutes=5)
            logger.info("🔥 MEGA FLASH SALE STARTED! 2000% activity boost for 5 minutes!")

        return 20.0 if self.promotion_active else 1.0

    def get_seasonal_multiplier(self):
        """계절별/월별 활동 배율"""
        month = datetime.now().month

        seasonal_patterns = {
            1: 0.8,   # 1월 (신정 후 조용)
            2: 0.9,   # 2월
            3: 1.1,   # 3월 (봄 시즌)
            4: 1.2,   # 4월
            5: 1.3,   # 5월 (어린이날, 어버이날)
            6: 1.1,   # 6월
            7: 1.2,   # 7월 (여름 휴가)
            8: 1.3,   # 8월
            9: 1.1,   # 9월
            10: 1.2,  # 10월
            11: 1.8,  # 11월 (블랙프라이데이, 쇼핑 시즌)
            12: 2.5   # 12월 (크리스마스, 연말)
        }

        return seasonal_patterns.get(month, 1.0)

    def calculate_dynamic_activity_rate(self):
        """동적 활동률 계산"""
        hourly = self.get_hourly_activity_multiplier()
        weekly = self.get_weekly_activity_multiplier()
        promotion = self.check_promotion_event()
        seasonal = self.get_seasonal_multiplier()

        # 전체 배율 계산
        total_multiplier = hourly * weekly * promotion * seasonal

        # 기본 확률 조정
        base_purchase_rate = 0.30  # 기본 30%
        adjusted_purchase_rate = min(0.9, base_purchase_rate * total_multiplier)

        # 이벤트 생성 간격 조정 (활동이 높을수록 빠르게)
        base_interval = 0.01  # 기본 0.01초 (100개/초)
        adjusted_interval = max(0.0005, base_interval / total_multiplier)  # 최소 0.0005초 (2000개/초)

        # 로그 출력 (10% 확률로)
        if random.random() < 0.1:
            current_hour = datetime.now().hour
            weekday_names = ['월', '화', '수', '목', '금', '토', '일']
            weekday = weekday_names[datetime.now().weekday()]

            logger.info(f"⚡ Activity: {total_multiplier:.1f}x | "
                       f"시간: {current_hour}시({hourly:.1f}x) | "
                       f"요일: {weekday}({weekly:.1f}x) | "
                       f"프로모션: {'🔥ON' if promotion > 1 else 'OFF'} | "
                       f"구매율: {adjusted_purchase_rate:.1%}")

        return adjusted_purchase_rate, adjusted_interval

    def get_dynamic_event_weights(self, purchase_rate):
        """동적 이벤트 가중치 계산"""
        # 구매율에 따라 다른 행동 패턴 조정
        view_rate = max(0.2, 0.6 - purchase_rate)
        cart_rate = min(0.3, purchase_rate * 0.8)
        like_rate = 0.1
        search_rate = 0.1

        # 정규화
        total = view_rate + cart_rate + purchase_rate + like_rate + search_rate
        normalized_weights = [
            view_rate / total,
            cart_rate / total,
            purchase_rate / total,
            like_rate / total,
            search_rate / total
        ]

        return normalized_weights

if __name__ == "__main__":
    generator = EcommerceDataGenerator()
    generator.run()