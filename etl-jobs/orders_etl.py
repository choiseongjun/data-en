import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch
import json
import logging
from datetime import datetime
import time
import pandas as pd
import os

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

        # S3-like 로컬 스토리지 경로 설정
        self.storage_path = '/data/s3-storage/orders'
        os.makedirs(self.storage_path, exist_ok=True)

    def close_connection(self):
        """PostgreSQL 연결 정리"""
        if self.pg_conn is not None:
            try:
                if self.pg_conn.closed == 0:
                    self.pg_conn.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.pg_conn = None

    def get_connection(self):
        """PostgreSQL 연결 관리 (재연결 지원)"""
        try:
            # 연결이 없거나 닫혔다면 새로 생성
            if self.pg_conn is None or self.pg_conn.closed != 0:
                if self.pg_conn is not None:
                    try:
                        self.pg_conn.close()
                    except:
                        pass
                logger.info("Creating new PostgreSQL connection")
                self.pg_conn = psycopg2.connect(**self.pg_config)
                self.pg_conn.autocommit = True  # 자동 커밋 설정

            # 연결 상태 확인
            try:
                with self.pg_conn.cursor() as cur:
                    cur.execute('SELECT 1')
                return self.pg_conn
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                logger.warning("Connection test failed, creating new connection")
                self.pg_conn = None
                return self.get_connection()  # 재귀 호출로 새 연결 생성

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.pg_conn = None
            raise

    def extract_orders(self):
        """PostgreSQL에서 주문 데이터 추출 (증분 업데이트 지원)"""
        conn = self.get_connection()

        try:
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
                    GROUP BY o.order_id, o.user_id, u.name, u.email, o.order_date, o.status, o.total_amount, o.shipping_address, o.payment_method, o.created_at, o.updated_at
                    ORDER BY o.order_date DESC
                    LIMIT 10000
                """

                cursor.execute(query, params)
                orders = cursor.fetchall()
                logger.info(f"Extracted {len(orders)} orders from PostgreSQL")
                return orders

        except Exception as e:
            logger.error(f"Error extracting orders: {e}")
            # 연결 에러 시 다음 시도를 위해 연결 재설정
            if any(keyword in str(e).lower() for keyword in ['connection', 'server', 'network']):
                logger.info("Resetting connection due to connection error")
                self.pg_conn = None
            raise

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

    def load_to_file_storage(self, orders):
        """S3-like 로컬 스토리지에 Parquet 파일로 저장 (시간 단위 병합)"""
        try:
            if not orders:
                logger.info("No orders to save to file storage")
                return None

            # 현재 날짜/시간 기반 파티셔닝 (year=YYYY/month=MM/day=DD)
            now = datetime.now()
            partition_path = os.path.join(
                self.storage_path,
                f"year={now.year}",
                f"month={now.month:02d}",
                f"day={now.day:02d}"
            )
            os.makedirs(partition_path, exist_ok=True)

            # 시간 단위 파일명 (같은 시간대 데이터는 하나의 파일로 병합)
            hour_timestamp = now.strftime("%Y%m%d_%H")  # 시간 단위
            file_path = os.path.join(partition_path, f"orders_{hour_timestamp}.parquet")
            json_file_path = os.path.join(partition_path, f"orders_{hour_timestamp}.json")

            # 새로운 데이터를 DataFrame으로 변환
            new_df = pd.DataFrame(orders)

            # items 컬럼은 JSON 문자열로 변환
            new_df['items'] = new_df['items'].apply(json.dumps)
            new_df['categories'] = new_df['categories'].apply(json.dumps)
            new_df['brands'] = new_df['brands'].apply(json.dumps)

            # 기존 파일이 있으면 읽어서 병합
            if os.path.exists(file_path):
                try:
                    existing_df = pd.read_parquet(file_path, engine='pyarrow')

                    # 중복 제거: order_id 기준으로 최신 데이터 유지
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['order_id'], keep='last')

                    logger.info(f"Merged {len(new_df)} new orders with {len(existing_df)} existing orders")
                    df_to_save = combined_df
                except Exception as e:
                    logger.warning(f"Could not read existing file, creating new: {e}")
                    df_to_save = new_df
            else:
                logger.info(f"Creating new file for hour {hour_timestamp}")
                df_to_save = new_df

            # Parquet 파일로 저장 (압축 사용)
            df_to_save.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)

            file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
            logger.info(f"Saved {len(df_to_save)} total orders to {file_path} ({file_size:.2f} MB)")

            # JSON 파일도 병합하여 저장
            if os.path.exists(json_file_path):
                try:
                    with open(json_file_path, 'r', encoding='utf-8') as f:
                        existing_orders = json.load(f)

                    # order_id 기준으로 중복 제거하며 병합
                    order_dict = {order['order_id']: order for order in existing_orders}
                    for order in orders:
                        order_dict[order['order_id']] = order

                    merged_orders = list(order_dict.values())
                except Exception as e:
                    logger.warning(f"Could not merge JSON, using new data: {e}")
                    merged_orders = orders
            else:
                merged_orders = orders

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(merged_orders, f, ensure_ascii=False, indent=2)

            json_file_size = os.path.getsize(json_file_path) / 1024 / 1024  # MB
            logger.info(f"Saved {len(merged_orders)} total orders to JSON backup ({json_file_size:.2f} MB)")

            return file_path

        except Exception as e:
            logger.error(f"Failed to save to file storage: {e}")
            raise

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

            # Load to Elasticsearch
            self.load_to_elasticsearch(transformed_orders)

            # Load to File Storage (S3-like)
            self.load_to_file_storage(transformed_orders)

            # 성공 시 마지막 ETL 시간 업데이트
            self.last_etl_time = datetime.now()
            logger.info(f"Orders ETL process completed successfully. Updated last_etl_time to {self.last_etl_time}")

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            # 연결 문제 시 재연결을 위해 연결 정리
            if any(keyword in str(e).lower() for keyword in ['connection', 'server', 'network']):
                logger.info("Closing connection due to connection error")
                self.close_connection()

if __name__ == "__main__":
    # 주기적으로 ETL 실행 (30초마다)
    etl = OrdersETL()

    while True:
        try:
            etl.run_etl()
            logger.info("Waiting 30 seconds before next ETL run...")
            time.sleep(30)  # 30초 대기 (파일 저장 빈도 증가)
        except KeyboardInterrupt:
            logger.info("ETL process stopped")
            # 연결 정리
            etl.close_connection()
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(10)  # 에러 시 10초 대기