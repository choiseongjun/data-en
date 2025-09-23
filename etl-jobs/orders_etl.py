import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch
import json
import logging
from datetime import datetime
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrdersETL:
    def __init__(self):
        # PostgreSQL 연결 설정 (재사용 가능하도록)
        self.pg_config = {
            'host': 'postgres',
            'database': 'ecommerce',
            'user': 'postgres',
            'password': 'postgres',
            'cursor_factory': RealDictCursor
        }
        self.pg_conn = None

        # Elasticsearch 연결
        self.es = Elasticsearch(['http://elasticsearch:9200'])

        # 마지막 ETL 실행 시간 추적
        self.last_etl_time = None

    def get_connection(self):
        """PostgreSQL 연결 관리 (재연결 지원)"""
        try:
            if self.pg_conn is None or self.pg_conn.closed:
                logger.info("Creating new PostgreSQL connection")
                self.pg_conn = psycopg2.connect(**self.pg_config)
            return self.pg_conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.pg_conn = None
            raise

    def extract_orders(self):
        """PostgreSQL에서 주문 데이터 추출 (증분 업데이트 지원)"""
        conn = self.get_connection()

        with conn.cursor() as cursor:
            # 증분 업데이트: 마지막 ETL 이후 생성/수정된 주문만 가져오기
            where_clause = ""
            params = []

            if self.last_etl_time:
                where_clause = "WHERE o.created_at > %s OR o.updated_at > %s"
                params = [self.last_etl_time, self.last_etl_time]
                logger.info(f"Extracting orders modified after {self.last_etl_time}")
            else:
                logger.info("Extracting all orders (initial run)")

            query = f"""
                SELECT
                    o.order_id,
                    o.user_id,
                    u.name as user_name,
                    u.email as user_email,
                    o.order_date,
                    o.status,
                    o.total_amount,
                    o.shipping_address,
                    o.payment_method,
                    o.created_at,
                    o.updated_at,
                    -- 주문 아이템 정보를 JSON으로 집계
                    json_agg(
                        json_build_object(
                            'product_id', oi.product_id,
                            'product_name', p.name,
                            'category', c.name,
                            'brand', b.name,
                            'quantity', oi.quantity,
                            'unit_price', oi.unit_price,
                            'total_price', oi.total_price
                        )
                    ) as items
                FROM orders o
                JOIN users u ON o.user_id = u.user_id
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                JOIN categories c ON p.category_id = c.category_id
                JOIN brands b ON p.brand_id = b.brand_id
                {where_clause}
                GROUP BY o.order_id, u.name, u.email
                ORDER BY o.order_date DESC
                LIMIT 10000
            """

            cursor.execute(query, params)
            orders = cursor.fetchall()
            logger.info(f"Extracted {len(orders)} orders from PostgreSQL")
            return orders

    def transform_orders(self, orders):
        """주문 데이터 변환"""
        transformed_orders = []

        for order in orders:
            # 날짜 형식 변환
            order_doc = {
                'order_id': order['order_id'],
                'user_id': order['user_id'],
                'user_name': order['user_name'],
                'user_email': order['user_email'],
                'order_date': order['order_date'].isoformat() if order['order_date'] else None,
                'status': order['status'],
                'total_amount': float(order['total_amount']),
                'shipping_address': order['shipping_address'],
                'payment_method': order['payment_method'],
                'created_at': order['created_at'].isoformat() if order['created_at'] else None,
                'updated_at': order['updated_at'].isoformat() if order['updated_at'] else None,
                'items': order['items'],
                'items_count': len(order['items']),
                'total_quantity': sum(item['quantity'] for item in order['items']),
                # 카테고리별 통계
                'categories': list(set(item['category'] for item in order['items'])),
                'brands': list(set(item['brand'] for item in order['items'])),
                # ETL 메타데이터
                'etl_timestamp': datetime.now().isoformat(),
                'etl_source': 'postgresql'
            }

            # 숫자 형식 변환
            for item in order_doc['items']:
                item['unit_price'] = float(item['unit_price'])
                item['total_price'] = float(item['total_price'])

            transformed_orders.append(order_doc)

        logger.info(f"Transformed {len(transformed_orders)} orders")
        return transformed_orders

    def load_to_elasticsearch(self, orders):
        """Elasticsearch에 주문 데이터 로드"""
        # 인덱스 매핑 설정
        mapping = {
            "mappings": {
                "properties": {
                    "order_id": {"type": "integer"},
                    "user_id": {"type": "keyword"},
                    "user_name": {"type": "text"},
                    "user_email": {"type": "keyword"},
                    "order_date": {"type": "date"},
                    "status": {"type": "keyword"},
                    "total_amount": {"type": "float"},
                    "shipping_address": {"type": "text"},
                    "payment_method": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "items_count": {"type": "integer"},
                    "total_quantity": {"type": "integer"},
                    "categories": {"type": "keyword"},
                    "brands": {"type": "keyword"},
                    "etl_timestamp": {"type": "date"},
                    "etl_source": {"type": "keyword"},
                    "items": {
                        "type": "nested",
                        "properties": {
                            "product_id": {"type": "keyword"},
                            "product_name": {"type": "text"},
                            "category": {"type": "keyword"},
                            "brand": {"type": "keyword"},
                            "quantity": {"type": "integer"},
                            "unit_price": {"type": "float"},
                            "total_price": {"type": "float"}
                        }
                    }
                }
            }
        }

        # 인덱스 생성 (이미 있으면 무시)
        index_name = "orders"
        try:
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"Created index: {index_name}")
        except Exception as e:
            if "already_exists" in str(e):
                logger.info(f"Index {index_name} already exists")
            else:
                logger.error(f"Error creating index: {e}")

        # 벌크 인덱싱 (배치 처리)
        batch_size = 1000  # 1000개씩 처리
        total_indexed = 0

        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]
            bulk_data = []

            for order in batch:
                bulk_data.append({
                    "index": {
                        "_index": index_name,
                        "_id": order['order_id']
                    }
                })
                bulk_data.append(order)

            if bulk_data:
                try:
                    response = self.es.bulk(operations=bulk_data)

                    # 에러 체크
                    if response['errors']:
                        logger.error(f"Some documents failed to index in batch {i//batch_size + 1}")
                        for item in response['items']:
                            if 'index' in item and 'error' in item['index']:
                                logger.error(f"Error: {item['index']['error']}")
                    else:
                        total_indexed += len(batch)
                        logger.info(f"Successfully indexed batch {i//batch_size + 1}: {len(batch)} orders")

                except Exception as e:
                    logger.error(f"Bulk indexing failed for batch {i//batch_size + 1}: {e}")

        logger.info(f"Total indexed: {total_indexed} out of {len(orders)} orders")

    def run_etl(self):
        """전체 ETL 프로세스 실행"""
        logger.info("Starting Orders ETL process...")

        try:
            # Extract
            orders = self.extract_orders()

            if not orders:
                logger.info("No orders to process")
                return

            # Transform
            transformed_orders = self.transform_orders(orders)

            # Load
            self.load_to_elasticsearch(transformed_orders)

            # 성공 시 마지막 ETL 시간 업데이트
            self.last_etl_time = datetime.now()
            logger.info(f"Orders ETL process completed successfully. Updated last_etl_time to {self.last_etl_time}")

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            # 연결 문제 시 재연결을 위해 None으로 설정
            if "connection" in str(e).lower():
                logger.info("Resetting connection for next attempt")
                self.pg_conn = None

if __name__ == "__main__":
    # 주기적으로 ETL 실행 (2분마다)
    etl = OrdersETL()

    while True:
        try:
            etl.run_etl()
            logger.info("Waiting 2 minutes before next ETL run...")
            time.sleep(120)  # 2분 대기 (더 자주 동기화)
        except KeyboardInterrupt:
            logger.info("ETL process stopped")
            # 연결 정리
            if etl.pg_conn and not etl.pg_conn.closed:
                etl.pg_conn.close()
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(30)  # 에러 시 30초 대기