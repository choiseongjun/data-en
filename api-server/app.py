from flask import Flask, jsonify, request
import redis
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch
from datetime import datetime
import functools
import requests

# DB 튜닝 기능을 직접 추가
import time

app = Flask(__name__)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis 연결
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Elasticsearch 연결
es = Elasticsearch(['http://elasticsearch:9200'])

# PostgreSQL 연결
# Logstash로 로그 전송 함수
def send_to_logstash(message, log_data=None):
    try:
        payload = {
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "service": "api-server"
        }
        if log_data:
            payload.update(log_data)

        requests.post("http://logstash:5044",
                     json=payload,
                     timeout=1)
    except:
        pass  # 로그 전송 실패해도 메인 로직에 영향 없도록

# 쿼리 로깅을 위한 커서 래퍼 클래스
class LoggingCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        start_time = time.time()
        try:
            # 쿼리와 파라미터 로깅 (콘솔 + Logstash)
            if params:
                query_log = f"[SQL Query] {query}"
                params_log = f"[SQL Params] {params}"

                logger.info(query_log)
                logger.info(params_log)
                print(query_log, flush=True)
                print(params_log, flush=True)

                # Logstash로 전송
                send_to_logstash(query_log, {"sql_query": query})
                send_to_logstash(params_log, {"sql_params": str(params)})
            else:
                query_log = f"[SQL Query] {query}"
                logger.info(query_log)
                print(query_log, flush=True)
                send_to_logstash(query_log, {"sql_query": query})

            result = self._cursor.execute(query, params)

            # 실행 시간 로깅 (콘솔 + Logstash)
            execution_time = (time.time() - start_time) * 1000
            time_log = f"[SQL Execution Time] {execution_time:.2f}ms"

            logger.info(time_log)
            print(time_log, flush=True)
            send_to_logstash(time_log, {
                "execution_time_ms": execution_time,
                "sql_query": query[:100] + "..." if len(query) > 100 else query
            })

            return result
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_log = f"[SQL Error] {str(e)} (took {execution_time:.2f}ms)"

            logger.error(error_log)
            print(error_log, flush=True)
            send_to_logstash(error_log, {
                "error_message": str(e),
                "execution_time_ms": execution_time,
                "sql_query": query[:100] + "..." if len(query) > 100 else query
            })
            raise

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self._cursor, '__exit__'):
            return self._cursor.__exit__(exc_type, exc_val, exc_tb)
        return False

    def __getattr__(self, name):
        return getattr(self._cursor, name)

# 로깅이 적용된 연결 클래스
class LoggingConnection:
    def __init__(self, connection):
        self._connection = connection

    def cursor(self):
        return LoggingCursor(self._connection.cursor())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self._connection, '__exit__'):
            return self._connection.__exit__(exc_type, exc_val, exc_tb)
        return False

    def __getattr__(self, name):
        return getattr(self._connection, name)

def get_db_connection():
    conn = psycopg2.connect(
        host='postgres',
        database='ecommerce',
        user='postgres',
        password='postgres',
        cursor_factory=RealDictCursor
    )
    return LoggingConnection(conn)

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/test-sql-logging', methods=['GET'])
def test_sql_logging():
    """SQL 로깅 테스트"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 간단한 테스트 쿼리
                cursor.execute("SELECT COUNT(*) as total_orders FROM orders")
                result = cursor.fetchone()

                cursor.execute("SELECT status, COUNT(*) as count FROM orders GROUP BY status LIMIT 5")
                status_counts = cursor.fetchall()

                return jsonify({
                    "message": "SQL logging test completed",
                    "total_orders": result['total_orders'],
                    "status_breakdown": [
                        {"status": row['status'], "count": row['count']}
                        for row in status_counts
                    ]
                })
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error in SQL logging test: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/recommendations/<user_id>', methods=['GET'])
def get_user_recommendations(user_id):
    """사용자별 추천 조회"""
    try:
        # Redis에서 추천 조회
        recommendations_key = f"recommendations:{user_id}"
        recommendations_data = redis_client.get(recommendations_key)

        if not recommendations_data:
            return jsonify({
                "user_id": user_id,
                "recommendations": [],
                "message": "No recommendations found"
            }), 404

        recommendations = json.loads(recommendations_data)

        # 추천 상품의 상세 정보를 Elasticsearch에서 조회
        product_ids = [rec['product_id'] for rec in recommendations[:10]]

        products_detail = []
        for product_id in product_ids:
            try:
                result = es.get(index='products', id=product_id)
                product_info = result['_source']

                # 추천 점수 추가
                score = next((rec['score'] for rec in recommendations if rec['product_id'] == product_id), 0)
                product_info['recommendation_score'] = score

                products_detail.append(product_info)
            except:
                logger.warning(f"Product {product_id} not found in Elasticsearch")

        return jsonify({
            "user_id": user_id,
            "recommendations": products_detail,
            "total_count": len(products_detail)
        })

    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/trending', methods=['GET'])
def get_trending_products():
    """트렌딩 상품 조회"""
    try:
        limit = request.args.get('limit', 20, type=int)

        trending_data = redis_client.get("trending_products")
        if not trending_data:
            return jsonify({
                "trending_products": [],
                "message": "No trending data available"
            }), 404

        trending_products = json.loads(trending_data)[:limit]

        # 상품 상세 정보 조회
        detailed_trending = []
        for item in trending_products:
            try:
                result = es.get(index='products', id=item['product_id'])
                product_info = result['_source']
                product_info.update(item)  # 트렌딩 정보 추가
                detailed_trending.append(product_info)
            except:
                logger.warning(f"Product {item['product_id']} not found")

        return jsonify({
            "trending_products": detailed_trending,
            "total_count": len(detailed_trending)
        })

    except Exception as e:
        logger.error(f"Error getting trending products: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/search', methods=['GET'])
def search_products():
    """상품 검색"""
    try:
        query = request.args.get('q', '')
        category = request.args.get('category', '')
        brand = request.args.get('brand', '')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)
        page = request.args.get('page', 1, type=int)
        size = request.args.get('size', 20, type=int)

        # Elasticsearch 쿼리 구성
        search_body = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": []
                }
            },
            "from": (page - 1) * size,
            "size": size,
            "sort": [{"_score": {"order": "desc"}}]
        }

        # 텍스트 검색
        if query:
            search_body["query"]["bool"]["must"].append({
                "multi_match": {
                    "query": query,
                    "fields": ["name^2", "description", "tags"],
                    "type": "best_fields"
                }
            })
        else:
            search_body["query"]["bool"]["must"].append({"match_all": {}})

        # 필터 추가
        if category:
            search_body["query"]["bool"]["filter"].append({
                "term": {"category.keyword": category}
            })

        if brand:
            search_body["query"]["bool"]["filter"].append({
                "term": {"brand.keyword": brand}
            })

        if min_price is not None or max_price is not None:
            price_range = {}
            if min_price is not None:
                price_range["gte"] = min_price
            if max_price is not None:
                price_range["lte"] = max_price

            search_body["query"]["bool"]["filter"].append({
                "range": {"price": price_range}
            })

        # 검색 실행
        result = es.search(index='products', body=search_body)

        products = []
        for hit in result['hits']['hits']:
            product = hit['_source']
            product['relevance_score'] = hit['_score']
            products.append(product)

        return jsonify({
            "products": products,
            "total_count": result['hits']['total']['value'],
            "page": page,
            "size": size,
            "query": query
        })

    except Exception as e:
        logger.error(f"Error searching products: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/user-stats/<user_id>', methods=['GET'])
def get_user_stats(user_id):
    """사용자 통계 조회"""
    try:
        stats_key = f"user_stats:{user_id}"
        stats = redis_client.hgetall(stats_key)

        if not stats:
            return jsonify({
                "user_id": user_id,
                "message": "No stats found"
            }), 404

        # 숫자 값 변환
        numeric_fields = ['total_events', 'view_count', 'cart_count', 'purchase_count', 'like_count', 'search_count']
        for field in numeric_fields:
            if field in stats:
                stats[field] = int(stats[field])

        return jsonify({
            "user_id": user_id,
            "stats": stats
        })

    except Exception as e:
        logger.error(f"Error getting user stats for {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/popular-products', methods=['GET'])
def get_popular_products():
    """인기 상품 조회 (Redis에서)"""
    try:
        limit = request.args.get('limit', 20, type=int)

        # Redis에서 인기 상품 조회
        popular_products = redis_client.zrevrange("popular_products", 0, limit-1, withscores=True)

        result = []
        for product_id, score in popular_products:
            try:
                # Elasticsearch에서 상품 정보 조회
                es_result = es.get(index='products', id=product_id)
                product_info = es_result['_source']
                product_info['popularity_score'] = score
                result.append(product_info)
            except:
                logger.warning(f"Product {product_id} not found")

        return jsonify({
            "popular_products": result,
            "total_count": len(result)
        })

    except Exception as e:
        logger.error(f"Error getting popular products: {e}")
        return jsonify({"error": "Internal server error"}), 500

# PostgreSQL 기반 엔드포인트들

@app.route('/orders/<user_id>', methods=['GET'])
def get_user_orders(user_id):
    """사용자의 주문 내역 조회 - 성능 최적화된 JOIN 쿼리"""
    try:
        page = request.args.get('page', 1, type=int)
        size = request.args.get('size', 20, type=int)
        status = request.args.get('status', '')

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 최적화된 쿼리: 필요한 데이터만 조회하고 복합 인덱스 활용
                base_query = """
                    WITH user_orders AS (
                        SELECT o.order_id, o.order_date, o.status, o.total_amount,
                               o.shipping_address, o.payment_method
                        FROM orders o
                        WHERE o.user_id = %s
                """
                params = [user_id]

                if status:
                    base_query += " AND o.status = %s"
                    params.append(status)

                # 페이징은 CTE 내에서 처리하여 성능 향상
                base_query += """
                        ORDER BY o.order_date DESC
                        LIMIT %s OFFSET %s
                    )
                    SELECT uo.order_id, uo.order_date, uo.status, uo.total_amount,
                           uo.shipping_address, uo.payment_method,
                           COALESCE(
                               json_agg(
                                   json_build_object(
                                       'product_id', oi.product_id,
                                       'product_name', p.name,
                                       'quantity', oi.quantity,
                                       'unit_price', oi.unit_price,
                                       'total_price', oi.total_price
                                   ) ORDER BY oi.order_item_id
                               ) FILTER (WHERE oi.product_id IS NOT NULL),
                               '[]'::json
                           ) as items
                    FROM user_orders uo
                    LEFT JOIN order_items oi ON uo.order_id = oi.order_id
                    LEFT JOIN products p ON oi.product_id = p.product_id
                    GROUP BY uo.order_id, uo.order_date, uo.status, uo.total_amount,
                             uo.shipping_address, uo.payment_method
                    ORDER BY uo.order_date DESC
                """
                params.extend([size, (page - 1) * size])

                cursor.execute(base_query, params)
                orders_data = cursor.fetchall()

                # JSON 형태로 이미 그룹화되어 있으므로 간단한 처리
                orders = []
                for row in orders_data:
                    order_dict = {
                        'order_id': row['order_id'],
                        'order_date': row['order_date'].isoformat(),
                        'status': row['status'],
                        'total_amount': float(row['total_amount']),
                        'shipping_address': row['shipping_address'],
                        'payment_method': row['payment_method'],
                        'items': row['items'] if row['items'] else []
                    }
                    orders.append(order_dict)

                return jsonify({
                    "user_id": user_id,
                    "orders": orders,
                    "page": page,
                    "size": size,
                    "optimization": "Using CTE and JSON aggregation for better performance"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting orders for user {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/products-db', methods=['GET'])
def get_products_from_db():
    """PostgreSQL에서 상품 조회 - 성능 최적화된 복합 인덱스 활용"""
    try:
        page = request.args.get('page', 1, type=int)
        size = request.args.get('size', 20, type=int)
        category = request.args.get('category', '')
        brand = request.args.get('brand', '')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 최적화된 쿼리: WHERE 조건을 먼저 적용하고 JOIN 수행
                # 복합 인덱스 활용을 위해 필터링을 우선 처리
                base_conditions = ["p.is_active = true"]
                params = []

                # 인덱스 활용을 위한 조건 순서 최적화
                if category:
                    base_conditions.append("c.name = %s")
                    params.append(category)

                if brand:
                    base_conditions.append("b.name = %s")
                    params.append(brand)

                if min_price is not None:
                    base_conditions.append("p.price >= %s")
                    params.append(min_price)

                if max_price is not None:
                    base_conditions.append("p.price <= %s")
                    params.append(max_price)

                # 서브쿼리로 필터링 먼저 수행하여 JOIN 비용 감소
                query = f"""
                    WITH filtered_products AS (
                        SELECT p.product_id, p.name, p.description, p.price, p.rating,
                               p.stock_quantity, p.category_id, p.brand_id, p.created_at
                        FROM products p
                        WHERE {' AND '.join(base_conditions[:1])}  -- is_active 조건
                        {'AND p.price >= %s' if min_price is not None else ''}
                        {'AND p.price <= %s' if max_price is not None else ''}
                        ORDER BY p.created_at DESC
                        LIMIT %s OFFSET %s
                    )
                    SELECT fp.product_id, fp.name, fp.description, fp.price, fp.rating,
                           fp.stock_quantity, c.name as category, b.name as brand,
                           fp.created_at,
                           COUNT(*) OVER() as total_count
                    FROM filtered_products fp
                    JOIN categories c ON fp.category_id = c.category_id
                    JOIN brands b ON fp.brand_id = b.brand_id
                """

                # 파라미터 정리
                query_params = []

                if min_price is not None:
                    query_params.append(min_price)
                if max_price is not None:
                    query_params.append(max_price)

                query_params.extend([size, (page - 1) * size])

                # 카테고리/브랜드 필터가 있는 경우 WHERE 절 추가
                if category or brand:
                    where_conditions = []
                    if category:
                        where_conditions.append("c.name = %s")
                        query_params.append(category)
                    if brand:
                        where_conditions.append("b.name = %s")
                        query_params.append(brand)
                    query += f" WHERE {' AND '.join(where_conditions)}"

                query += " ORDER BY fp.created_at DESC"

                cursor.execute(query, query_params)
                products = cursor.fetchall()

                # 데이터 형식 변환
                product_list = []
                total_count = 0
                for product in products:
                    if total_count == 0 and products:
                        total_count = product.get('total_count', len(products))

                    product_dict = {
                        'product_id': product['product_id'],
                        'name': product['name'],
                        'description': product['description'],
                        'price': float(product['price']),
                        'rating': float(product['rating']) if product['rating'] else 0,
                        'stock_quantity': product['stock_quantity'],
                        'category': product['category'],
                        'brand': product['brand'],
                        'created_at': product['created_at'].isoformat()
                    }
                    product_list.append(product_dict)

                return jsonify({
                    "products": product_list,
                    "total_count": total_count,
                    "page": page,
                    "size": size,
                    "optimization": "Using filtered CTE and strategic JOIN order for better performance"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting products from database: {e}")
        return jsonify({"error": "Internal server error"}), 500

# 복잡한 분석 쿼리들 - 시간이 오래 걸리는 쿼리들

@app.route('/analytics/complex-order-analysis')
def complex_order_analysis():
    """복잡한 주문 분석 - 여러 테이블 조인 및 집계"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 매우 복잡한 주문 분석 쿼리 (시간이 오래 걸림)
                query = """
                    WITH monthly_stats AS (
                        SELECT
                            DATE_TRUNC('month', o.created_at) as month,
                            c.name as category,
                            COUNT(DISTINCT o.order_id) as order_count,
                            COUNT(DISTINCT o.user_id) as unique_customers,
                            SUM(oi.quantity * oi.unit_price) as revenue,
                            AVG(oi.quantity * oi.unit_price) as avg_order_value,
                            MIN(oi.quantity * oi.unit_price) as min_order_value,
                            MAX(oi.quantity * oi.unit_price) as max_order_value,
                            STDDEV(oi.quantity * oi.unit_price) as revenue_stddev
                        FROM orders o
                        JOIN order_items oi ON o.order_id = oi.order_id
                        JOIN products p ON oi.product_id = p.product_id
                        JOIN categories c ON p.category_id = c.category_id
                        WHERE o.created_at >= CURRENT_DATE - INTERVAL '12 months'
                        GROUP BY DATE_TRUNC('month', o.created_at), c.name
                    ),
                    growth_rates AS (
                        SELECT
                            month, category, revenue,
                            LAG(revenue) OVER (PARTITION BY category ORDER BY month) as prev_revenue,
                            CASE
                                WHEN LAG(revenue) OVER (PARTITION BY category ORDER BY month) > 0
                                THEN ((revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY month)) /
                                      LAG(revenue) OVER (PARTITION BY category ORDER BY month)) * 100
                                ELSE 0
                            END as growth_rate
                        FROM monthly_stats
                    )
                    SELECT
                        gr.month, gr.category, gr.revenue, gr.growth_rate,
                        ms.order_count, ms.unique_customers, ms.avg_order_value,
                        ms.revenue_stddev,
                        RANK() OVER (PARTITION BY gr.month ORDER BY gr.revenue DESC) as revenue_rank,
                        ROW_NUMBER() OVER (ORDER BY gr.growth_rate DESC NULLS LAST) as growth_rank
                    FROM growth_rates gr
                    JOIN monthly_stats ms ON gr.month = ms.month AND gr.category = ms.category
                    ORDER BY gr.month DESC, gr.revenue DESC
                    LIMIT 100
                """

                cursor.execute(query)
                results = cursor.fetchall()

                analysis_data = []
                for row in results:
                    analysis_data.append({
                        'month': row['month'].isoformat() if row['month'] else None,
                        'category': row['category'],
                        'revenue': float(row['revenue']) if row['revenue'] else 0,
                        'growth_rate': float(row['growth_rate']) if row['growth_rate'] else 0,
                        'order_count': row['order_count'],
                        'unique_customers': row['unique_customers'],
                        'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0,
                        'revenue_stddev': float(row['revenue_stddev']) if row['revenue_stddev'] else 0,
                        'revenue_rank': row['revenue_rank'],
                        'growth_rank': row['growth_rank']
                    })

                return jsonify({
                    "complex_analysis": analysis_data,
                    "query_type": "Complex Multi-table Analysis with Window Functions"
                })

    except Exception as e:
        logger.error(f"Error in complex order analysis: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/analytics/heavy-aggregation')
def heavy_aggregation():
    """무거운 집계 쿼리 - 대용량 데이터 GROUP BY"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 대용량 집계 쿼리 (인덱스 스캔이 많이 발생)
                query = """
                    SELECT
                        p.name as product_name,
                        c.name as category,
                        b.name as brand,
                        COUNT(DISTINCT o.order_id) as total_orders,
                        SUM(oi.quantity) as total_quantity_sold,
                        SUM(oi.quantity * oi.unit_price) as total_revenue,
                        AVG(oi.quantity * oi.unit_price) as avg_order_item_value,
                        MIN(o.created_at) as first_order_date,
                        MAX(o.created_at) as last_order_date,
                        COUNT(DISTINCT o.user_id) as unique_customers,
                        COUNT(DISTINCT DATE_TRUNC('month', o.created_at)) as months_active,
                        COALESCE(AVG(pr.rating), 0) as avg_product_rating,
                        COUNT(pr.review_id) as review_count,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY oi.quantity * oi.unit_price) as median_order_value,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oi.quantity * oi.unit_price) as p95_order_value
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    JOIN brands b ON p.brand_id = b.brand_id
                    LEFT JOIN order_items oi ON p.product_id = oi.product_id
                    LEFT JOIN orders o ON oi.order_id = o.order_id
                    LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
                    WHERE o.created_at >= CURRENT_DATE - INTERVAL '6 months'
                    GROUP BY p.product_id, p.name, c.name, b.name
                    HAVING COUNT(DISTINCT o.order_id) > 0
                    ORDER BY total_revenue DESC NULLS LAST, total_orders DESC
                    LIMIT 50
                """

                cursor.execute(query)
                results = cursor.fetchall()

                aggregation_data = []
                for row in results:
                    aggregation_data.append({
                        'product_name': row['product_name'],
                        'category': row['category'],
                        'brand': row['brand'],
                        'total_orders': row['total_orders'],
                        'total_quantity_sold': row['total_quantity_sold'],
                        'total_revenue': float(row['total_revenue']) if row['total_revenue'] else 0,
                        'avg_order_item_value': float(row['avg_order_item_value']) if row['avg_order_item_value'] else 0,
                        'first_order_date': row['first_order_date'].isoformat() if row['first_order_date'] else None,
                        'last_order_date': row['last_order_date'].isoformat() if row['last_order_date'] else None,
                        'unique_customers': row['unique_customers'],
                        'months_active': row['months_active'],
                        'avg_product_rating': float(row['avg_product_rating']) if row['avg_product_rating'] else 0,
                        'review_count': row['review_count'],
                        'median_order_value': float(row['median_order_value']) if row['median_order_value'] else 0,
                        'p95_order_value': float(row['p95_order_value']) if row['p95_order_value'] else 0
                    })

                return jsonify({
                    "heavy_aggregation": aggregation_data,
                    "query_type": "Heavy Aggregation with Multiple JOINs and Statistical Functions"
                })

    except Exception as e:
        logger.error(f"Error in heavy aggregation: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/analytics/recursive-category-tree')
def recursive_category_tree():
    """재귀 쿼리 - 카테고리 트리 구조 분석"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 재귀 CTE를 사용한 복잡한 쿼리
                query = """
                    WITH RECURSIVE category_hierarchy AS (
                        -- Base case: root categories
                        SELECT
                            category_id,
                            name,
                            parent_category_id,
                            1 as level,
                            name as path,
                            ARRAY[category_id] as id_path
                        FROM categories
                        WHERE parent_category_id IS NULL

                        UNION ALL

                        -- Recursive case: child categories
                        SELECT
                            c.category_id,
                            c.name,
                            c.parent_category_id,
                            ch.level + 1,
                            ch.path || ' > ' || c.name,
                            ch.id_path || c.category_id
                        FROM categories c
                        JOIN category_hierarchy ch ON c.parent_category_id = ch.category_id
                        WHERE ch.level < 10  -- Prevent infinite recursion
                    ),
                    category_stats AS (
                        SELECT
                            ch.category_id,
                            ch.name,
                            ch.level,
                            ch.path,
                            COUNT(DISTINCT p.product_id) as product_count,
                            COUNT(DISTINCT o.order_id) as order_count,
                            COALESCE(SUM(oi.quantity * oi.unit_price), 0) as total_revenue,
                            COUNT(DISTINCT o.user_id) as unique_customers,
                            COALESCE(AVG(p.rating), 0) as avg_rating,
                            MIN(p.price) as min_price,
                            MAX(p.price) as max_price,
                            COALESCE(AVG(p.price), 0) as avg_price
                        FROM category_hierarchy ch
                        LEFT JOIN products p ON ch.category_id = p.category_id
                        LEFT JOIN order_items oi ON p.product_id = oi.product_id
                        LEFT JOIN orders o ON oi.order_id = o.order_id
                        WHERE o.created_at IS NULL OR o.created_at >= CURRENT_DATE - INTERVAL '3 months'
                        GROUP BY ch.category_id, ch.name, ch.level, ch.path
                    )
                    SELECT
                        cs.*,
                        RANK() OVER (PARTITION BY cs.level ORDER BY cs.total_revenue DESC) as revenue_rank_in_level,
                        LAG(cs.total_revenue) OVER (PARTITION BY cs.level ORDER BY cs.total_revenue DESC) as prev_revenue_in_level,
                        CASE
                            WHEN cs.product_count > 0
                            THEN cs.total_revenue / cs.product_count
                            ELSE 0
                        END as revenue_per_product
                    FROM category_stats cs
                    ORDER BY cs.level, cs.total_revenue DESC
                """

                cursor.execute(query)
                results = cursor.fetchall()

                tree_data = []
                for row in results:
                    tree_data.append({
                        'category_id': row['category_id'],
                        'name': row['name'],
                        'level': row['level'],
                        'path': row['path'],
                        'product_count': row['product_count'],
                        'order_count': row['order_count'],
                        'total_revenue': float(row['total_revenue']) if row['total_revenue'] else 0,
                        'unique_customers': row['unique_customers'],
                        'avg_rating': float(row['avg_rating']) if row['avg_rating'] else 0,
                        'min_price': float(row['min_price']) if row['min_price'] else 0,
                        'max_price': float(row['max_price']) if row['max_price'] else 0,
                        'avg_price': float(row['avg_price']) if row['avg_price'] else 0,
                        'revenue_rank_in_level': row['revenue_rank_in_level'],
                        'revenue_per_product': float(row['revenue_per_product']) if row['revenue_per_product'] else 0
                    })

                return jsonify({
                    "category_tree": tree_data,
                    "query_type": "Recursive CTE with Complex Hierarchy Analysis"
                })

    except Exception as e:
        logger.error(f"Error in recursive category tree: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/analytics/customer-cohort-analysis')
def customer_cohort_analysis():
    """고객 코호트 분석 - 매우 복잡한 시계열 분석"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 코호트 분석 쿼리 (매우 복잡하고 시간이 오래 걸림)
                query = """
                    WITH customer_first_orders AS (
                        SELECT
                            user_id,
                            MIN(created_at) as first_order_date,
                            DATE_TRUNC('month', MIN(created_at)) as cohort_month
                        FROM orders
                        GROUP BY user_id
                    ),
                    customer_orders AS (
                        SELECT
                            o.user_id,
                            o.order_id,
                            o.created_at,
                            cfo.cohort_month,
                            DATE_TRUNC('month', o.created_at) as order_month,
                            EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', o.created_at), cfo.cohort_month)) * 12 +
                            EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', o.created_at), cfo.cohort_month)) as period_number,
                            SUM(oi.quantity * oi.unit_price) as order_value
                        FROM orders o
                        JOIN customer_first_orders cfo ON o.user_id = cfo.user_id
                        JOIN order_items oi ON o.order_id = oi.order_id
                        WHERE o.created_at >= CURRENT_DATE - INTERVAL '18 months'
                        GROUP BY o.user_id, o.order_id, o.created_at, cfo.cohort_month
                    ),
                    cohort_data AS (
                        SELECT
                            cohort_month,
                            period_number,
                            COUNT(DISTINCT user_id) as customers,
                            COUNT(DISTINCT order_id) as orders,
                            SUM(order_value) as revenue,
                            AVG(order_value) as avg_order_value,
                            MIN(order_value) as min_order_value,
                            MAX(order_value) as max_order_value
                        FROM customer_orders
                        WHERE period_number >= 0 AND period_number <= 12
                        GROUP BY cohort_month, period_number
                    ),
                    cohort_sizes AS (
                        SELECT
                            cohort_month,
                            COUNT(DISTINCT user_id) as cohort_size
                        FROM customer_first_orders
                        GROUP BY cohort_month
                    )
                    SELECT
                        cd.cohort_month,
                        cd.period_number,
                        cd.customers,
                        cs.cohort_size,
                        ROUND((cd.customers::DECIMAL / cs.cohort_size) * 100, 2) as retention_rate,
                        cd.orders,
                        cd.revenue,
                        cd.avg_order_value,
                        cd.min_order_value,
                        cd.max_order_value,
                        ROUND(cd.revenue / cd.customers, 2) as revenue_per_customer,
                        ROUND(cd.orders::DECIMAL / cd.customers, 2) as orders_per_customer,
                        LAG(cd.customers) OVER (PARTITION BY cd.cohort_month ORDER BY cd.period_number) as prev_period_customers,
                        CASE
                            WHEN LAG(cd.customers) OVER (PARTITION BY cd.cohort_month ORDER BY cd.period_number) > 0
                            THEN ROUND(((cd.customers - LAG(cd.customers) OVER (PARTITION BY cd.cohort_month ORDER BY cd.period_number))::DECIMAL /
                                      LAG(cd.customers) OVER (PARTITION BY cd.cohort_month ORDER BY cd.period_number)) * 100, 2)
                            ELSE 0
                        END as period_growth_rate
                    FROM cohort_data cd
                    JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
                    ORDER BY cd.cohort_month DESC, cd.period_number
                """

                cursor.execute(query)
                results = cursor.fetchall()

                cohort_data = []
                for row in results:
                    cohort_data.append({
                        'cohort_month': row['cohort_month'].isoformat() if row['cohort_month'] else None,
                        'period_number': row['period_number'],
                        'customers': row['customers'],
                        'cohort_size': row['cohort_size'],
                        'retention_rate': float(row['retention_rate']) if row['retention_rate'] else 0,
                        'orders': row['orders'],
                        'revenue': float(row['revenue']) if row['revenue'] else 0,
                        'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0,
                        'min_order_value': float(row['min_order_value']) if row['min_order_value'] else 0,
                        'max_order_value': float(row['max_order_value']) if row['max_order_value'] else 0,
                        'revenue_per_customer': float(row['revenue_per_customer']) if row['revenue_per_customer'] else 0,
                        'orders_per_customer': float(row['orders_per_customer']) if row['orders_per_customer'] else 0,
                        'period_growth_rate': float(row['period_growth_rate']) if row['period_growth_rate'] else 0
                    })

                return jsonify({
                    "cohort_analysis": cohort_data,
                    "query_type": "Complex Customer Cohort Analysis with Retention Metrics"
                })

    except Exception as e:
        logger.error(f"Error in customer cohort analysis: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/analytics/full-table-scan-test')
def full_table_scan_test():
    """의도적인 Full Table Scan 테스트 (매우 느린 쿼리)"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 의도적으로 인덱스를 사용하지 않는 쿼리 (매우 느림)
                query = """
                    SELECT
                        COUNT(*) as total_records,
                        AVG(CAST(SUBSTRING(o.order_id::text, 1, 3) AS INTEGER)) as avg_id_prefix,
                        SUM(CASE WHEN o.created_at::text LIKE '%2024%' THEN 1 ELSE 0 END) as year_2024_count,
                        MAX(LENGTH(u.first_name || u.last_name)) as max_name_length,
                        MIN(EXTRACT(EPOCH FROM o.created_at)) as min_timestamp,
                        COUNT(DISTINCT UPPER(LEFT(p.name, 5))) as unique_product_prefixes,
                        SUM(CASE WHEN oi.unit_price::text ~ '^[0-9]+\\.[0-9]{2}$' THEN 1 ELSE 0 END) as exact_price_count,
                        AVG(CASE WHEN c.name ~ '[aeiou]' THEN LENGTH(c.name) ELSE 0 END) as avg_vowel_category_length
                    FROM orders o
                    JOIN users u ON CAST(o.user_id AS TEXT) = CAST(u.user_id AS TEXT)
                    JOIN order_items oi ON CAST(o.order_id AS TEXT) = CAST(oi.order_id AS TEXT)
                    JOIN products p ON CAST(oi.product_id AS TEXT) = CAST(p.product_id AS TEXT)
                    JOIN categories c ON CAST(p.category_id AS TEXT) = CAST(c.category_id AS TEXT)
                    WHERE
                        o.created_at::text NOT LIKE '%impossible_date%'
                        AND u.email::text NOT LIKE '%@nonexistent.com'
                        AND p.description IS NOT NULL
                        AND RANDOM() < 1.0  -- Force full scan
                """

                cursor.execute(query)
                result = cursor.fetchone()

                scan_data = {
                    'total_records': result['total_records'],
                    'avg_id_prefix': float(result['avg_id_prefix']) if result['avg_id_prefix'] else 0,
                    'year_2024_count': result['year_2024_count'],
                    'max_name_length': result['max_name_length'],
                    'min_timestamp': float(result['min_timestamp']) if result['min_timestamp'] else 0,
                    'unique_product_prefixes': result['unique_product_prefixes'],
                    'exact_price_count': result['exact_price_count'],
                    'avg_vowel_category_length': float(result['avg_vowel_category_length']) if result['avg_vowel_category_length'] else 0
                }

                return jsonify({
                    "full_scan_test": scan_data,
                    "query_type": "Intentional Full Table Scan with String Operations",
                    "warning": "This query is intentionally slow for testing purposes"
                })

    except Exception as e:
        logger.error(f"Error in full table scan test: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/user-behavior/<user_id>', methods=['GET'])
def get_user_behavior(user_id):
    """사용자 행동 분석 - 고성능 JOIN과 집계 쿼리 최적화"""
    try:
        days = request.args.get('days', 7, type=int)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 단일 쿼리로 모든 정보를 조회하여 성능 향상
                optimized_query = """
                WITH user_behavior_stats AS (
                    SELECT
                        action_type,
                        COUNT(*) as count,
                        COUNT(DISTINCT product_id) as unique_products
                    FROM user_behavior_log
                    WHERE user_id = %s
                    AND created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY action_type
                ),
                recent_product_interactions AS (
                    SELECT
                        ubl.product_id,
                        p.name as product_name,
                        c.name as category,
                        COUNT(*) as interaction_count,
                        MAX(ubl.created_at) as last_interaction,
                        array_agg(DISTINCT ubl.action_type) as action_types
                    FROM user_behavior_log ubl
                    JOIN products p ON ubl.product_id = p.product_id
                    JOIN categories c ON p.category_id = c.category_id
                    WHERE ubl.user_id = %s
                    AND ubl.created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY ubl.product_id, p.name, c.name
                    ORDER BY interaction_count DESC, last_interaction DESC
                    LIMIT 10
                )
                SELECT
                    'stats' as data_type,
                    json_agg(
                        json_build_object(
                            'action_type', action_type,
                            'count', count,
                            'unique_products', unique_products
                        ) ORDER BY count DESC
                    ) as data
                FROM user_behavior_stats

                UNION ALL

                SELECT
                    'products' as data_type,
                    json_agg(
                        json_build_object(
                            'product_id', product_id,
                            'product_name', product_name,
                            'category', category,
                            'interaction_count', interaction_count,
                            'last_interaction', last_interaction,
                            'action_types', action_types
                        ) ORDER BY interaction_count DESC
                    ) as data
                FROM recent_product_interactions
                """

                cursor.execute(optimized_query, (user_id, days, user_id, days))
                results = cursor.fetchall()

                # 결과 파싱
                behavior_stats = []
                recent_products = []

                for row in results:
                    if row['data_type'] == 'stats' and row['data']:
                        behavior_stats = row['data']
                    elif row['data_type'] == 'products' and row['data']:
                        for product in row['data']:
                            product['last_interaction'] = product['last_interaction'].isoformat() if product['last_interaction'] else None
                        recent_products = row['data']

                # 추가 분석: 행동 패턴 분석
                pattern_analysis = analyze_user_behavior_patterns(behavior_stats)

                return jsonify({
                    "user_id": user_id,
                    "period_days": days,
                    "behavior_stats": behavior_stats,
                    "recent_products": recent_products,
                    "pattern_analysis": pattern_analysis,
                    "optimization": "Using CTE with JSON aggregation for single-query performance"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting user behavior for {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

def analyze_user_behavior_patterns(behavior_stats):
    """사용자 행동 패턴 분석"""
    if not behavior_stats:
        return {"engagement_level": "no_data", "primary_actions": []}

    # 행동별 가중치 (참여도 계산)
    action_weights = {
        'view': 1,
        'like': 2,
        'cart': 3,
        'purchase': 5,
        'search': 1
    }

    total_weighted_actions = 0
    total_actions = 0
    primary_actions = []

    for stat in behavior_stats:
        action_type = stat['action_type']
        count = stat['count']
        weight = action_weights.get(action_type, 1)

        total_weighted_actions += count * weight
        total_actions += count

        if count > 0:
            primary_actions.append({
                'action': action_type,
                'count': count,
                'percentage': 0  # 나중에 계산
            })

    # 비율 계산
    for action in primary_actions:
        action['percentage'] = round((action['count'] / total_actions) * 100, 1) if total_actions > 0 else 0

    # 정렬
    primary_actions.sort(key=lambda x: x['count'], reverse=True)

    # 참여도 레벨 결정
    engagement_score = total_weighted_actions / max(total_actions, 1)
    if engagement_score >= 3:
        engagement_level = "high"
    elif engagement_score >= 2:
        engagement_level = "medium"
    else:
        engagement_level = "low"

    return {
        "engagement_level": engagement_level,
        "engagement_score": round(engagement_score, 2),
        "primary_actions": primary_actions[:3],  # 상위 3개만
        "total_actions": total_actions
    }

@app.route('/analytics/dashboard', methods=['GET'])
def get_analytics_dashboard():
    """분석 대시보드 - 대량 데이터 집계 최적화된 쿼리"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 단일 최적화된 쿼리로 모든 대시보드 데이터 조회
                dashboard_query = """
                WITH overview_stats AS (
                    SELECT
                        (SELECT COUNT(*) FROM users) as total_users,
                        (SELECT COUNT(*) FROM products WHERE is_active = true) as total_products,
                        (SELECT COUNT(*) FROM orders) as total_orders,
                        (SELECT COALESCE(SUM(total_amount), 0)
                         FROM orders
                         WHERE status IN ('shipped', 'delivered', 'completed')) as total_revenue
                ),
                category_performance AS (
                    SELECT
                        c.name as category,
                        COUNT(oi.order_item_id) as items_sold,
                        SUM(oi.total_price) as revenue,
                        AVG(oi.unit_price) as avg_unit_price,
                        COUNT(DISTINCT o.user_id) as unique_customers
                    FROM categories c
                    JOIN products p ON c.category_id = p.category_id
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.status IN ('shipped', 'delivered', 'completed')
                    AND o.order_date >= NOW() - INTERVAL '90 days'  -- 최근 3개월로 제한하여 성능 향상
                    GROUP BY c.category_id, c.name
                    ORDER BY revenue DESC
                    LIMIT 10
                ),
                daily_trends AS (
                    SELECT
                        DATE(order_date) as order_date,
                        COUNT(*) as order_count,
                        SUM(total_amount) as daily_revenue,
                        AVG(total_amount) as avg_order_value,
                        COUNT(DISTINCT user_id) as unique_customers
                    FROM orders
                    WHERE order_date >= NOW() - INTERVAL '30 days'
                    AND status NOT IN ('cancelled', 'failed')
                    GROUP BY DATE(order_date)
                    ORDER BY order_date DESC
                    LIMIT 30
                ),
                top_products AS (
                    SELECT
                        p.product_id,
                        p.name as product_name,
                        c.name as category,
                        SUM(oi.quantity) as total_sold,
                        SUM(oi.total_price) as product_revenue,
                        COUNT(DISTINCT oi.order_id) as order_frequency
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.status IN ('shipped', 'delivered', 'completed')
                    AND o.order_date >= NOW() - INTERVAL '30 days'
                    GROUP BY p.product_id, p.name, c.name
                    ORDER BY total_sold DESC
                    LIMIT 5
                )
                SELECT
                    'overview' as section,
                    json_build_object(
                        'total_users', os.total_users,
                        'total_products', os.total_products,
                        'total_orders', os.total_orders,
                        'total_revenue', os.total_revenue
                    ) as data
                FROM overview_stats os

                UNION ALL

                SELECT
                    'categories' as section,
                    json_agg(
                        json_build_object(
                            'category', category,
                            'items_sold', items_sold,
                            'revenue', revenue,
                            'avg_unit_price', avg_unit_price,
                            'unique_customers', unique_customers
                        ) ORDER BY revenue DESC
                    ) as data
                FROM category_performance

                UNION ALL

                SELECT
                    'daily_trends' as section,
                    json_agg(
                        json_build_object(
                            'order_date', order_date,
                            'order_count', order_count,
                            'daily_revenue', daily_revenue,
                            'avg_order_value', avg_order_value,
                            'unique_customers', unique_customers
                        ) ORDER BY order_date DESC
                    ) as data
                FROM daily_trends

                UNION ALL

                SELECT
                    'top_products' as section,
                    json_agg(
                        json_build_object(
                            'product_id', product_id,
                            'product_name', product_name,
                            'category', category,
                            'total_sold', total_sold,
                            'product_revenue', product_revenue,
                            'order_frequency', order_frequency
                        ) ORDER BY total_sold DESC
                    ) as data
                FROM top_products
                """

                cursor.execute(dashboard_query)
                results = cursor.fetchall()

                # 결과 구조화
                dashboard_data = {
                    "overview": {},
                    "category_stats": [],
                    "daily_orders": [],
                    "top_products": []
                }

                for row in results:
                    section = row['section']
                    data = row['data']

                    if section == 'overview':
                        dashboard_data['overview'] = data
                    elif section == 'categories':
                        dashboard_data['category_stats'] = data or []
                    elif section == 'daily_trends':
                        # 날짜 형식 변환
                        if data:
                            for item in data:
                                if item['order_date']:
                                    item['order_date'] = item['order_date'].isoformat() if hasattr(item['order_date'], 'isoformat') else str(item['order_date'])
                                item['daily_revenue'] = float(item['daily_revenue']) if item['daily_revenue'] else 0
                                item['avg_order_value'] = float(item['avg_order_value']) if item['avg_order_value'] else 0
                        dashboard_data['daily_orders'] = data or []
                    elif section == 'top_products':
                        dashboard_data['top_products'] = data or []

                # 숫자 형식 변환
                if dashboard_data['overview']:
                    dashboard_data['overview']['total_revenue'] = float(dashboard_data['overview']['total_revenue'])

                for stat in dashboard_data['category_stats']:
                    stat['revenue'] = float(stat['revenue']) if stat['revenue'] else 0
                    stat['avg_unit_price'] = float(stat['avg_unit_price']) if stat['avg_unit_price'] else 0

                # 성능 메트릭 추가
                dashboard_data['performance_info'] = {
                    "optimization": "Single CTE query with JSON aggregation",
                    "data_period": "Last 30-90 days for performance",
                    "query_strategy": "Filtered joins with time-based indexing"
                }

                return jsonify(dashboard_data)
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting analytics dashboard: {e}")
        return jsonify({"error": "Internal server error"}), 500

# 새로운 성능 최적화 API 엔드포인트들

@app.route('/optimized/user-purchase-history/<user_id>', methods=['GET'])
def get_optimized_user_purchase_history(user_id):
    """최적화된 사용자 구매 히스토리 - 복합 인덱스와 CTE 활용"""
    try:
        months = request.args.get('months', 6, type=int)
        include_details = request.args.get('details', 'false').lower() == 'true'

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                if include_details:
                    # 상세 정보 포함 쿼리
                    query = """
                    WITH user_orders AS (
                        SELECT o.order_id, o.order_date, o.status, o.total_amount,
                               COUNT(oi.order_item_id) as item_count
                        FROM orders o
                        LEFT JOIN order_items oi ON o.order_id = oi.order_id
                        WHERE o.user_id = %s
                        AND o.order_date >= NOW() - INTERVAL '%s months'
                        AND o.status IN ('shipped', 'delivered', 'completed')
                        GROUP BY o.order_id, o.order_date, o.status, o.total_amount
                        ORDER BY o.order_date DESC
                        LIMIT 50
                    )
                    SELECT
                        uo.*,
                        json_agg(
                            json_build_object(
                                'product_name', p.name,
                                'category', c.name,
                                'quantity', oi.quantity,
                                'unit_price', oi.unit_price
                            ) ORDER BY oi.order_item_id
                        ) as items
                    FROM user_orders uo
                    JOIN order_items oi ON uo.order_id = oi.order_id
                    JOIN products p ON oi.product_id = p.product_id
                    JOIN categories c ON p.category_id = c.category_id
                    GROUP BY uo.order_id, uo.order_date, uo.status, uo.total_amount, uo.item_count
                    ORDER BY uo.order_date DESC
                    """
                    cursor.execute(query, [user_id, months])
                else:
                    # 요약 정보만 쿼리
                    query = """
                    SELECT
                        order_id, order_date, status, total_amount,
                        COUNT(oi.order_item_id) as item_count
                    FROM orders o
                    LEFT JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.user_id = %s
                    AND o.order_date >= NOW() - INTERVAL '%s months'
                    AND o.status IN ('shipped', 'delivered', 'completed')
                    GROUP BY o.order_id, o.order_date, o.status, o.total_amount
                    ORDER BY o.order_date DESC
                    LIMIT 50
                    """
                    cursor.execute(query, [user_id, months])

                results = cursor.fetchall()

                # 데이터 변환
                purchase_history = []
                for row in results:
                    item = {
                        'order_id': row['order_id'],
                        'order_date': row['order_date'].isoformat(),
                        'status': row['status'],
                        'total_amount': float(row['total_amount']),
                        'item_count': row['item_count']
                    }
                    if include_details and 'items' in row:
                        item['items'] = row['items']
                    purchase_history.append(item)

                return jsonify({
                    "user_id": user_id,
                    "months_period": months,
                    "purchase_history": purchase_history,
                    "total_orders": len(purchase_history),
                    "optimization": "CTE with conditional JOIN for flexible detail level"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting optimized user purchase history: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/optimized/category-sales-report', methods=['GET'])
def get_optimized_category_sales_report():
    """최적화된 카테고리 판매 리포트 - 윈도우 함수와 집계 최적화"""
    try:
        period_days = request.args.get('days', 30, type=int)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                WITH category_sales AS (
                    SELECT
                        c.name as category,
                        COUNT(DISTINCT o.order_id) as total_orders,
                        SUM(oi.quantity) as total_items_sold,
                        SUM(oi.total_price) as total_revenue,
                        COUNT(DISTINCT o.user_id) as unique_customers,
                        AVG(oi.unit_price) as avg_unit_price,
                        MIN(oi.unit_price) as min_price,
                        MAX(oi.unit_price) as max_price
                    FROM categories c
                    JOIN products p ON c.category_id = p.category_id
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.order_date >= NOW() - INTERVAL '%s days'
                    AND o.status IN ('shipped', 'delivered', 'completed')
                    GROUP BY c.category_id, c.name
                )
                SELECT *,
                       ROUND((total_revenue * 100.0 / SUM(total_revenue) OVER()), 2) as revenue_percentage,
                       ROUND((total_items_sold * 100.0 / SUM(total_items_sold) OVER()), 2) as volume_percentage,
                       ROUND(total_revenue / NULLIF(total_orders, 0), 2) as avg_order_value,
                       ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                       ROW_NUMBER() OVER (ORDER BY total_items_sold DESC) as volume_rank
                FROM category_sales
                ORDER BY total_revenue DESC
                """

                cursor.execute(query, [period_days])
                results = cursor.fetchall()

                # 데이터 변환
                category_report = []
                for row in results:
                    category_report.append({
                        'category': row['category'],
                        'metrics': {
                            'total_orders': row['total_orders'],
                            'total_items_sold': row['total_items_sold'],
                            'total_revenue': float(row['total_revenue']),
                            'unique_customers': row['unique_customers'],
                            'avg_unit_price': float(row['avg_unit_price']),
                            'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0,
                            'price_range': {
                                'min': float(row['min_price']),
                                'max': float(row['max_price'])
                            }
                        },
                        'performance': {
                            'revenue_percentage': float(row['revenue_percentage']),
                            'volume_percentage': float(row['volume_percentage']),
                            'revenue_rank': row['revenue_rank'],
                            'volume_rank': row['volume_rank']
                        }
                    })

                return jsonify({
                    "period_days": period_days,
                    "category_sales_report": category_report,
                    "total_categories": len(category_report),
                    "optimization": "Window functions for rankings and percentages in single query"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting category sales report: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/optimized/top-customers', methods=['GET'])
def get_optimized_top_customers():
    """최적화된 우수 고객 분석 - 복합 집계와 고급 분석"""
    try:
        limit = request.args.get('limit', 20, type=int)
        period_days = request.args.get('days', 90, type=int)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                WITH customer_metrics AS (
                    SELECT
                        u.user_id,
                        u.name,
                        u.email,
                        u.created_at as join_date,
                        COUNT(DISTINCT o.order_id) as total_orders,
                        SUM(o.total_amount) as total_spent,
                        AVG(o.total_amount) as avg_order_value,
                        MAX(o.order_date) as last_order_date,
                        MIN(o.order_date) as first_order_date,
                        COUNT(DISTINCT oi.product_id) as unique_products_purchased,
                        COUNT(DISTINCT EXTRACT(MONTH FROM o.order_date)) as active_months,
                        SUM(oi.quantity) as total_items_purchased
                    FROM users u
                    JOIN orders o ON u.user_id = o.user_id
                    JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.order_date >= NOW() - INTERVAL '%s days'
                    AND o.status IN ('shipped', 'delivered', 'completed')
                    GROUP BY u.user_id, u.name, u.email, u.created_at
                ),
                customer_scores AS (
                    SELECT *,
                           -- 고객 가치 점수 계산 (여러 요소 고려)
                           (
                               (total_spent / 1000) * 0.4 +  -- 총 구매액 (40%)
                               (total_orders * 2) * 0.3 +     -- 주문 빈도 (30%)
                               (unique_products_purchased) * 0.2 + -- 제품 다양성 (20%)
                               (active_months * 3) * 0.1      -- 활동 기간 (10%)
                           ) as customer_value_score,
                           ROUND(total_spent / NULLIF(EXTRACT(EPOCH FROM (NOW() - first_order_date))/86400, 0), 2) as daily_avg_spend,
                           CASE
                               WHEN last_order_date >= NOW() - INTERVAL '7 days' THEN 'highly_active'
                               WHEN last_order_date >= NOW() - INTERVAL '30 days' THEN 'active'
                               WHEN last_order_date >= NOW() - INTERVAL '60 days' THEN 'moderate'
                               ELSE 'inactive'
                           END as activity_status
                    FROM customer_metrics
                )
                SELECT *,
                       ROW_NUMBER() OVER (ORDER BY customer_value_score DESC) as value_rank,
                       ROW_NUMBER() OVER (ORDER BY total_spent DESC) as spending_rank,
                       ROUND((customer_value_score * 100.0 / MAX(customer_value_score) OVER()), 1) as score_percentile
                FROM customer_scores
                ORDER BY customer_value_score DESC
                LIMIT %s
                """

                cursor.execute(query, [period_days, limit])
                results = cursor.fetchall()

                # 데이터 변환
                top_customers = []
                for row in results:
                    customer = {
                        'user_id': row['user_id'],
                        'name': row['name'],
                        'email': row['email'],
                        'metrics': {
                            'total_orders': row['total_orders'],
                            'total_spent': float(row['total_spent']),
                            'avg_order_value': float(row['avg_order_value']),
                            'unique_products_purchased': row['unique_products_purchased'],
                            'total_items_purchased': row['total_items_purchased'],
                            'active_months': row['active_months'],
                            'daily_avg_spend': float(row['daily_avg_spend']) if row['daily_avg_spend'] else 0
                        },
                        'timeline': {
                            'join_date': row['join_date'].isoformat(),
                            'first_order_date': row['first_order_date'].isoformat(),
                            'last_order_date': row['last_order_date'].isoformat()
                        },
                        'analysis': {
                            'customer_value_score': float(row['customer_value_score']),
                            'value_rank': row['value_rank'],
                            'spending_rank': row['spending_rank'],
                            'score_percentile': float(row['score_percentile']),
                            'activity_status': row['activity_status']
                        }
                    }
                    top_customers.append(customer)

                return jsonify({
                    "period_days": period_days,
                    "top_customers": top_customers,
                    "total_analyzed": len(top_customers),
                    "analysis_method": "Multi-factor customer value scoring with activity tracking",
                    "optimization": "Single CTE query with complex calculations and window functions"
                })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting top customers: {e}")
        return jsonify({"error": "Internal server error"}), 500

# 커버링 인덱스 최적화 API 엔드포인트들

@app.route('/db-tuning/covering-index-demo', methods=['GET'])
def covering_index_demo():
    """커버링 인덱스 성능 비교 - Index-Only Scan vs Regular Index Scan"""
    try:
        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 일반적인 쿼리 (커버링 인덱스 없음)
                logger.info("Testing query WITHOUT covering index...")
                start_time = time.time()
                cursor.execute("""
                    SELECT user_id, order_date, status, total_amount
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2023-07-01'
                    AND status IN ('shipped', 'delivered')
                    ORDER BY order_date DESC
                    LIMIT 1000
                """)
                without_covering = cursor.fetchall()
                without_covering_time = time.time() - start_time

                # 실행 계획 확인
                cursor.execute("""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT user_id, order_date, status, total_amount
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2023-07-01'
                    AND status IN ('shipped', 'delivered')
                    ORDER BY order_date DESC
                    LIMIT 1000
                """)
                plan_without = cursor.fetchone()[0][0]

                results['without_covering_index'] = {
                    'execution_time_ms': round(without_covering_time * 1000, 2),
                    'rows_returned': len(without_covering),
                    'execution_plan': plan_without
                }

                # 2. 커버링 인덱스 생성
                logger.info("Creating covering index...")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_orders_covering_demo
                    ON orders(order_date DESC, status)
                    INCLUDE (user_id, total_amount)
                """)

                # 3. 커버링 인덱스를 사용하는 동일한 쿼리
                logger.info("Testing query WITH covering index...")
                start_time = time.time()
                cursor.execute("""
                    SELECT user_id, order_date, status, total_amount
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2023-07-01'
                    AND status IN ('shipped', 'delivered')
                    ORDER BY order_date DESC
                    LIMIT 1000
                """)
                with_covering = cursor.fetchall()
                with_covering_time = time.time() - start_time

                # 실행 계획 확인
                cursor.execute("""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT user_id, order_date, status, total_amount
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2023-07-01'
                    AND status IN ('shipped', 'delivered')
                    ORDER BY order_date DESC
                    LIMIT 1000
                """)
                plan_with = cursor.fetchone()[0][0]

                results['with_covering_index'] = {
                    'execution_time_ms': round(with_covering_time * 1000, 2),
                    'rows_returned': len(with_covering),
                    'execution_plan': plan_with
                }

                return jsonify({
                    'scenario': 'Covering Index Performance Test',
                    'query_description': 'SELECT user_id, order_date, status, total_amount with date range filter',
                    'covering_index': 'CREATE INDEX idx_orders_covering_demo ON orders(order_date DESC, status) INCLUDE (user_id, total_amount)',
                    'results': results,
                    'speedup': f"{round(without_covering_time / with_covering_time, 1)}x faster" if with_covering_time > 0 else 'N/A',
                    'benefit': 'Index-Only Scan eliminates table access completely'
                })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in covering index demo: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/user-summary-covering', methods=['GET'])
def user_summary_covering_index():
    """사용자 요약 정보 - 커버링 인덱스 최적화 예시"""
    try:
        user_limit = request.args.get('limit', 500, type=int)

        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 커버링 인덱스 없이 (일반적인 방식)
                start_time = time.time()
                cursor.execute("""
                    SELECT u.user_id, u.name, u.email,
                           COUNT(o.order_id) as order_count,
                           COALESCE(SUM(o.total_amount), 0) as total_spent,
                           MAX(o.order_date) as last_order_date
                    FROM users u
                    LEFT JOIN orders o ON u.user_id = o.user_id
                    WHERE o.status IN ('shipped', 'delivered', 'completed')
                    OR o.status IS NULL
                    GROUP BY u.user_id, u.name, u.email
                    ORDER BY total_spent DESC
                    LIMIT %s
                """, [user_limit])
                without_results = cursor.fetchall()
                without_time = time.time() - start_time

                results['without_covering_index'] = {
                    'execution_time_ms': round(without_time * 1000, 2),
                    'rows_returned': len(without_results),
                    'method': 'Regular index with table lookups'
                }

                # 2. 커버링 인덱스 생성 (orders 테이블용)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_orders_user_covering
                    ON orders(user_id, status)
                    INCLUDE (total_amount, order_date)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_users_covering
                    ON users(user_id)
                    INCLUDE (name, email)
                """)

                # 3. 커버링 인덱스를 활용한 최적화된 쿼리
                start_time = time.time()
                cursor.execute("""
                    SELECT u.user_id, u.name, u.email,
                           COUNT(o.order_id) as order_count,
                           COALESCE(SUM(o.total_amount), 0) as total_spent,
                           MAX(o.order_date) as last_order_date
                    FROM users u
                    LEFT JOIN orders o ON u.user_id = o.user_id
                    WHERE o.status IN ('shipped', 'delivered', 'completed')
                    OR o.status IS NULL
                    GROUP BY u.user_id, u.name, u.email
                    ORDER BY total_spent DESC
                    LIMIT %s
                """, [user_limit])
                with_results = cursor.fetchall()
                with_time = time.time() - start_time

                results['with_covering_index'] = {
                    'execution_time_ms': round(with_time * 1000, 2),
                    'rows_returned': len(with_results),
                    'method': 'Index-Only Scan with covering indexes'
                }

                # 샘플 데이터 (처음 5개)
                sample_users = []
                for row in with_results[:5]:
                    sample_users.append({
                        'user_id': row['user_id'],
                        'name': row['name'],
                        'email': row['email'],
                        'order_count': row['order_count'],
                        'total_spent': float(row['total_spent']),
                        'last_order_date': row['last_order_date'].isoformat() if row['last_order_date'] else None
                    })

                return jsonify({
                    'scenario': 'User Summary with Covering Index',
                    'covering_indexes_created': [
                        'CREATE INDEX idx_orders_user_covering ON orders(user_id, status) INCLUDE (total_amount, order_date)',
                        'CREATE INDEX idx_users_covering ON users(user_id) INCLUDE (name, email)'
                    ],
                    'performance_comparison': results,
                    'speedup': f"{round(without_time / with_time, 1)}x faster" if with_time > 0 else 'N/A',
                    'sample_users': sample_users,
                    'total_users_analyzed': len(with_results)
                })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in user summary covering index: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/product-stats-covering', methods=['GET'])
def product_stats_covering_index():
    """상품 통계 - 커버링 인덱스로 빠른 집계"""
    try:
        category_filter = request.args.get('category', '')

        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                base_filter = ""
                params = []
                if category_filter:
                    base_filter = "AND c.name = %s"
                    params.append(category_filter)

                # 1. 일반적인 방식 (테이블 스캔 포함)
                start_time = time.time()
                query1 = f"""
                    SELECT p.product_id, p.name, c.name as category,
                           COUNT(oi.order_item_id) as times_ordered,
                           SUM(oi.quantity) as total_quantity_sold,
                           SUM(oi.total_price) as total_revenue,
                           AVG(oi.unit_price) as avg_selling_price,
                           p.rating, p.stock_quantity
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    LEFT JOIN order_items oi ON p.product_id = oi.product_id
                    LEFT JOIN orders o ON oi.order_id = o.order_id
                    WHERE p.is_active = true
                    AND (o.status IN ('shipped', 'delivered', 'completed') OR o.status IS NULL)
                    {base_filter}
                    GROUP BY p.product_id, p.name, c.name, p.rating, p.stock_quantity
                    ORDER BY total_revenue DESC NULLS LAST
                    LIMIT 100
                """
                cursor.execute(query1, params)
                without_results = cursor.fetchall()
                without_time = time.time() - start_time

                results['without_covering_index'] = {
                    'execution_time_ms': round(without_time * 1000, 2),
                    'rows_returned': len(without_results)
                }

                # 2. 커버링 인덱스 생성
                index1_sql = """
                    CREATE INDEX IF NOT EXISTS idx_order_items_covering
                    ON order_items(product_id)
                    INCLUDE (order_id, quantity, unit_price, total_price)
                """
                cursor.execute(index1_sql)

                index2_sql = """
                    CREATE INDEX IF NOT EXISTS idx_products_covering
                    ON products(product_id, is_active, category_id)
                    INCLUDE (name, rating, stock_quantity)
                """
                cursor.execute(index2_sql)

                index3_sql = """
                    CREATE INDEX IF NOT EXISTS idx_orders_covering_status
                    ON orders(order_id, status)
                """
                cursor.execute(index3_sql)

                # 3. 커버링 인덱스를 활용한 쿼리
                start_time = time.time()
                query2 = f"""
                    SELECT p.product_id, p.name, c.name as category,
                           COUNT(oi.order_item_id) as times_ordered,
                           SUM(oi.quantity) as total_quantity_sold,
                           SUM(oi.total_price) as total_revenue,
                           AVG(oi.unit_price) as avg_selling_price,
                           p.rating, p.stock_quantity
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    LEFT JOIN order_items oi ON p.product_id = oi.product_id
                    LEFT JOIN orders o ON oi.order_id = o.order_id
                    WHERE p.is_active = true
                    AND (o.status IN ('shipped', 'delivered', 'completed') OR o.status IS NULL)
                    {base_filter}
                    GROUP BY p.product_id, p.name, c.name, p.rating, p.stock_quantity
                    ORDER BY total_revenue DESC NULLS LAST
                    LIMIT 100
                """
                cursor.execute(query2, params)
                with_results = cursor.fetchall()
                with_time = time.time() - start_time

                results['with_covering_index'] = {
                    'execution_time_ms': round(with_time * 1000, 2),
                    'rows_returned': len(with_results)
                }

                # 상위 5개 상품 샘플 데이터
                top_products = []
                for row in with_results[:5]:
                    top_products.append({
                        'product_id': row['product_id'],
                        'name': row['name'],
                        'category': row['category'],
                        'times_ordered': row['times_ordered'] or 0,
                        'total_quantity_sold': row['total_quantity_sold'] or 0,
                        'total_revenue': float(row['total_revenue']) if row['total_revenue'] else 0,
                        'avg_selling_price': float(row['avg_selling_price']) if row['avg_selling_price'] else 0,
                        'rating': float(row['rating']) if row['rating'] else 0,
                        'stock_quantity': row['stock_quantity']
                    })

                return jsonify({
                    'scenario': 'Product Sales Statistics with Covering Index',
                    'category_filter': category_filter if category_filter else 'All categories',
                    'covering_indexes_created': [
                        'CREATE INDEX idx_order_items_covering ON order_items(product_id) INCLUDE (order_id, quantity, unit_price, total_price)',
                        'CREATE INDEX idx_products_covering ON products(product_id, is_active, category_id) INCLUDE (name, rating, stock_quantity)',
                        'CREATE INDEX idx_orders_covering_status ON orders(order_id, status)'
                    ],
                    'performance_comparison': results,
                    'speedup': f"{round(without_time / with_time, 1)}x faster" if with_time > 0 else 'N/A',
                    'top_products': top_products,
                    'benefit': 'Covering indexes eliminate table access for aggregated queries'
                })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in product stats covering index: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/partition-performance', methods=['GET'])
def partition_performance_comparison():
    """파티션 vs 일반 테이블 성능 비교"""
    try:
        status_filter = request.args.get('status', 'shipped')
        limit = int(request.args.get('limit', 1000))

        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 일반 테이블에서 조회 (120만건 풀스캔)
                start_time = time.time()
                cursor.execute("""
                    SELECT order_id, user_id, status, total_amount, created_at
                    FROM orders
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status_filter, limit))
                normal_results = cursor.fetchall()
                normal_time = time.time() - start_time

                # 2. 원본 테이블 백업에서 조회 (비교용)
                start_time = time.time()
                cursor.execute("""
                    SELECT order_id, user_id, status, total_amount, created_at
                    FROM orders_old_original
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status_filter, limit))
                partition_results = cursor.fetchall()
                partition_time = time.time() - start_time

                # 3. 원본 테이블에서 날짜 범위 조회
                start_time = time.time()
                cursor.execute("""
                    SELECT order_id, user_id, status, total_amount, created_at
                    FROM orders_old_original
                    WHERE status = %s
                    AND created_at >= '2025-09-24'
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status_filter, limit))
                normal_date_results = cursor.fetchall()
                normal_date_time = time.time() - start_time

                # 4. 파티션 테이블에서 날짜 범위 조회 (파티션 프루닝) - 현재는 orders가 파티션 테이블
                start_time = time.time()
                cursor.execute("""
                    SELECT order_id, user_id, status, total_amount, created_at
                    FROM orders
                    WHERE status = %s
                    AND created_at >= '2025-09-24'
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status_filter, limit))
                partition_date_results = cursor.fetchall()
                partition_date_time = time.time() - start_time

                # 성능 비교 결과
                results = {
                    'scenario': 'Partitioned vs Normal Table Performance',
                    'test_parameters': {
                        'status_filter': status_filter,
                        'limit': limit,
                        'total_records': 1205308
                    },
                    'performance_comparison': {
                        'status_only_query': {
                            'normal_table_ms': round(normal_time * 1000, 2),
                            'partition_table_ms': round(partition_time * 1000, 2),
                            'improvement': f"{round(normal_time / partition_time, 1)}x faster" if partition_time > 0 else 'N/A',
                            'records_returned': len(partition_results)
                        },
                        'date_range_query': {
                            'normal_table_ms': round(normal_date_time * 1000, 2),
                            'partition_table_ms': round(partition_date_time * 1000, 2),
                            'improvement': f"{round(normal_date_time / partition_date_time, 1)}x faster" if partition_date_time > 0 else 'N/A',
                            'records_returned': len(partition_date_results)
                        }
                    },
                    'partition_info': {
                        'partition_strategy': 'RANGE by created_at',
                        'partitions': ['orders_2025_09', 'orders_2025_10', 'orders_2025_11'],
                        'partition_pruning': 'Enabled - only scans relevant partitions',
                        'indexes_per_partition': ['status', 'user_id', 'order_date']
                    },
                    'sample_data': [
                        {
                            'order_id': row['order_id'],
                            'user_id': row['user_id'],
                            'status': row['status'],
                            'total_amount': float(row['total_amount']),
                            'created_at': row['created_at'].isoformat()
                        }
                        for row in partition_date_results[:3]
                    ]
                }

                return jsonify(results)

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in partition performance comparison: {e}")
        return jsonify({"error": str(e)}), 500

# DB 튜닝 API 엔드포인트들 - 대용량 데이터 최적화

@app.route('/db-tuning/heavy-queries', methods=['GET'])
def heavy_query_tuning():
    """대용량 orders 테이블 - 느린 쿼리 vs 최적화된 쿼리 비교"""
    try:
        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 나쁜 쿼리: WHERE 조건에 함수 사용 (인덱스 사용 불가)
                logger.info("Running slow query with function in WHERE clause...")
                start_time = time.time()
                cursor.execute("""
                    SELECT COUNT(*), AVG(total_amount)
                    FROM orders
                    WHERE EXTRACT(YEAR FROM order_date) = 2023
                    AND EXTRACT(MONTH FROM order_date) = 6
                """)
                slow_result = cursor.fetchone()
                slow_time = time.time() - start_time

                results['slow_query'] = {
                    'query': 'Using EXTRACT functions in WHERE clause',
                    'execution_time_ms': round(slow_time * 1000, 2),
                    'result': {'count': slow_result[0], 'avg_amount': float(slow_result[1]) if slow_result[1] else 0}
                }

                # 2. 최적화된 쿼리: 날짜 범위로 변경 (인덱스 사용 가능)
                logger.info("Running optimized query with date range...")
                start_time = time.time()
                cursor.execute("""
                    SELECT COUNT(*), AVG(total_amount)
                    FROM orders
                    WHERE order_date >= '2023-06-01'
                    AND order_date < '2023-07-01'
                """)
                fast_result = cursor.fetchone()
                fast_time = time.time() - start_time

                results['optimized_query'] = {
                    'query': 'Using date range with index',
                    'execution_time_ms': round(fast_time * 1000, 2),
                    'result': {'count': fast_result[0], 'avg_amount': float(fast_result[1]) if fast_result[1] else 0}
                }

            return jsonify({
                'total_orders': '1.2M+',
                'comparison': results,
                'speedup': f"{round(slow_time / fast_time, 1)}x faster" if fast_time > 0 else 'N/A',
                'recommendation': 'Use date ranges instead of date functions in WHERE clauses for better index usage'
            })

        finally:
            conn.close()

    except Exception as e:
        import traceback
        logger.error(f"Error in heavy query tuning: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

@app.route('/db-tuning/pagination-performance', methods=['GET'])
def pagination_performance():
    """대용량 테이블 페이징 - OFFSET vs Cursor 기반 페이징 비교"""
    try:
        page = request.args.get('page', 10000, type=int)  # 깊은 페이지로 테스트
        limit = request.args.get('limit', 20, type=int)

        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 나쁜 방법: OFFSET 사용 (깊은 페이지일수록 느려짐)
                offset = (page - 1) * limit
                logger.info(f"Testing OFFSET pagination at page {page}...")

                start_time = time.time()
                cursor.execute("""
                    SELECT order_id, user_id, order_date, total_amount, status
                    FROM orders
                    ORDER BY order_id
                    LIMIT %s OFFSET %s
                """, [limit, offset])
                offset_results = cursor.fetchall()
                offset_time = time.time() - start_time

                results['offset_pagination'] = {
                    'method': f'OFFSET {offset} LIMIT {limit}',
                    'execution_time_ms': round(offset_time * 1000, 2),
                    'page': page,
                    'rows_returned': len(offset_results)
                }

                # 2. 최적화된 방법: Cursor 기반 페이징 (WHERE > last_id 사용)
                if offset_results:
                    # 이전 페이지의 마지막 order_id를 기준으로 사용
                    cursor.execute("SELECT order_id FROM orders ORDER BY order_id LIMIT 1 OFFSET %s", [offset-1])
                    cursor_start = cursor.fetchone()
                    last_id = cursor_start[0] if cursor_start else 0

                    logger.info(f"Testing cursor-based pagination from order_id {last_id}...")
                    start_time = time.time()
                    cursor.execute("""
                        SELECT order_id, user_id, order_date, total_amount, status
                        FROM orders
                        WHERE order_id > %s
                        ORDER BY order_id
                        LIMIT %s
                    """, [last_id, limit])
                    cursor_results = cursor.fetchall()
                    cursor_time = time.time() - start_time

                    results['cursor_pagination'] = {
                        'method': f'WHERE order_id > {last_id} LIMIT {limit}',
                        'execution_time_ms': round(cursor_time * 1000, 2),
                        'cursor_position': last_id,
                        'rows_returned': len(cursor_results)
                    }

            return jsonify({
                'scenario': f'Deep pagination at page {page} of 1.2M+ orders',
                'comparison': results,
                'speedup': f"{round(offset_time / cursor_time, 1)}x faster" if 'cursor_pagination' in results and cursor_time > 0 else 'N/A',
                'recommendation': 'Use cursor-based pagination (WHERE id > last_id) instead of OFFSET for deep pagination'
            })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in pagination performance: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/aggregation-optimization', methods=['GET'])
def aggregation_optimization():
    """대용량 데이터 집계 쿼리 최적화"""
    try:
        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 비효율적인 집계: 전체 테이블 스캔
                logger.info("Running slow aggregation without proper indexing...")
                start_time = time.time()
                cursor.execute("""
                    SELECT
                        status,
                        COUNT(*) as order_count,
                        AVG(total_amount) as avg_amount,
                        SUM(total_amount) as total_revenue
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2024-01-01'
                    GROUP BY status
                    ORDER BY total_revenue DESC
                """)
                slow_results = cursor.fetchall()
                slow_time = time.time() - start_time

                results['without_optimization'] = {
                    'execution_time_ms': round(slow_time * 1000, 2),
                    'results': [dict(zip(['status', 'order_count', 'avg_amount', 'total_revenue'], row))
                               for row in slow_results]
                }

                # 2. 최적화된 집계: 복합 인덱스 활용 확인
                # 먼저 인덱스가 있는지 확인하고 없으면 생성
                cursor.execute("""
                    SELECT 1 FROM pg_indexes
                    WHERE tablename = 'orders'
                    AND indexname = 'idx_orders_date_status_amount'
                """)

                if not cursor.fetchone():
                    logger.info("Creating composite index for optimization...")
                    cursor.execute("""
                        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_date_status_amount
                        ON orders(order_date, status, total_amount)
                    """)

                logger.info("Running optimized aggregation with composite index...")
                start_time = time.time()
                cursor.execute("""
                    SELECT
                        status,
                        COUNT(*) as order_count,
                        AVG(total_amount) as avg_amount,
                        SUM(total_amount) as total_revenue
                    FROM orders
                    WHERE order_date >= '2023-01-01'
                    AND order_date < '2024-01-01'
                    GROUP BY status
                    ORDER BY total_revenue DESC
                """)
                fast_results = cursor.fetchall()
                fast_time = time.time() - start_time

                results['with_optimization'] = {
                    'execution_time_ms': round(fast_time * 1000, 2),
                    'optimization': 'Composite index on (order_date, status, total_amount)',
                    'results': [dict(zip(['status', 'order_count', 'avg_amount', 'total_revenue'], row))
                               for row in fast_results]
                }

            return jsonify({
                'scenario': 'Large scale aggregation on 1.2M+ orders',
                'year': '2023',
                'comparison': results,
                'speedup': f"{round(slow_time / fast_time, 1)}x faster" if fast_time > 0 else 'N/A',
                'recommendation': 'Create composite indexes covering WHERE, GROUP BY, and aggregate columns'
            })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in aggregation optimization: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/join-performance', methods=['GET'])
def join_performance():
    """대용량 테이블 JOIN 성능 최적화"""
    try:
        conn = get_db_connection()
        results = {}

        try:
            with conn.cursor() as cursor:
                # 1. 비효율적인 JOIN: WHERE 조건이 JOIN 후에 적용
                logger.info("Running inefficient JOIN query...")
                start_time = time.time()
                cursor.execute("""
                    SELECT
                        u.name as user_name,
                        COUNT(o.order_id) as order_count,
                        SUM(o.total_amount) as total_spent
                    FROM users u
                    JOIN orders o ON u.user_id = o.user_id
                    WHERE o.status IN ('shipped', 'delivered')
                    AND o.order_date >= '2023-06-01'
                    GROUP BY u.user_id, u.name
                    HAVING COUNT(o.order_id) >= 5
                    ORDER BY total_spent DESC
                    LIMIT 100
                """)
                slow_results = cursor.fetchall()
                slow_time = time.time() - start_time

                results['inefficient_join'] = {
                    'execution_time_ms': round(slow_time * 1000, 2),
                    'approach': 'Filter after JOIN',
                    'top_customers': len(slow_results)
                }

                # 2. 최적화된 JOIN: 서브쿼리로 먼저 필터링
                logger.info("Running optimized JOIN with pre-filtering...")
                start_time = time.time()
                cursor.execute("""
                    SELECT
                        u.name as user_name,
                        filtered_orders.order_count,
                        filtered_orders.total_spent
                    FROM users u
                    JOIN (
                        SELECT
                            user_id,
                            COUNT(*) as order_count,
                            SUM(total_amount) as total_spent
                        FROM orders
                        WHERE status IN ('shipped', 'delivered')
                        AND order_date >= '2023-06-01'
                        GROUP BY user_id
                        HAVING COUNT(*) >= 5
                    ) filtered_orders ON u.user_id = filtered_orders.user_id
                    ORDER BY filtered_orders.total_spent DESC
                    LIMIT 100
                """)
                fast_results = cursor.fetchall()
                fast_time = time.time() - start_time

                results['optimized_join'] = {
                    'execution_time_ms': round(fast_time * 1000, 2),
                    'approach': 'Pre-filter with subquery',
                    'top_customers': len(fast_results)
                }

                # 실행 계획 비교
                cursor.execute("""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT u.name, COUNT(o.order_id), SUM(o.total_amount)
                    FROM users u JOIN orders o ON u.user_id = o.user_id
                    WHERE o.status IN ('shipped', 'delivered')
                    AND o.order_date >= '2023-06-01'
                    GROUP BY u.user_id, u.name
                    HAVING COUNT(o.order_id) >= 5
                    LIMIT 5
                """)
                plan_slow = cursor.fetchone()[0][0]

                cursor.execute("""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT u.name, fo.order_count, fo.total_spent
                    FROM users u
                    JOIN (
                        SELECT user_id, COUNT(*) as order_count, SUM(total_amount) as total_spent
                        FROM orders
                        WHERE status IN ('shipped', 'delivered') AND order_date >= '2023-06-01'
                        GROUP BY user_id HAVING COUNT(*) >= 5
                    ) fo ON u.user_id = fo.user_id
                    LIMIT 5
                """)
                plan_fast = cursor.fetchone()[0][0]

            return jsonify({
                'scenario': 'Finding top customers from 1.2M+ orders',
                'filter_criteria': 'Recent orders, shipped/delivered status, 5+ orders',
                'comparison': results,
                'speedup': f"{round(slow_time / fast_time, 1)}x faster" if fast_time > 0 else 'N/A',
                'execution_plans': {
                    'inefficient': plan_slow,
                    'optimized': plan_fast
                },
                'recommendation': 'Filter large tables early with subqueries before JOINing'
            })

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in join performance: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/scan-comparison', methods=['GET'])
def scan_comparison():
    """Full Table Scan vs Index Scan 성능 비교"""
    try:
        table = request.args.get('table', 'orders')
        limit = request.args.get('limit', 1000, type=int)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 1. Full Table Scan (인덱스 사용 금지)
                start_time = time.time()
                cursor.execute("SET enable_indexscan = OFF")
                cursor.execute("SET enable_bitmapscan = OFF")
                cursor.execute(f"SELECT * FROM {table} LIMIT %s", [limit])
                full_scan_results = cursor.fetchall()
                full_scan_time = time.time() - start_time

                # 설정 리셋
                cursor.execute("RESET enable_indexscan")
                cursor.execute("RESET enable_bitmapscan")

                # 2. Index Scan (기본 설정)
                start_time = time.time()
                cursor.execute(f"SELECT * FROM {table} ORDER BY {table[:-1]}_id LIMIT %s", [limit])
                index_scan_results = cursor.fetchall()
                index_scan_time = time.time() - start_time

            return jsonify({
                'table': table,
                'limit': limit,
                'full_table_scan': {
                    'execution_time_ms': round(full_scan_time * 1000, 2),
                    'row_count': len(full_scan_results)
                },
                'index_scan': {
                    'execution_time_ms': round(index_scan_time * 1000, 2),
                    'row_count': len(index_scan_results)
                },
                'performance_ratio': round(full_scan_time / index_scan_time, 2) if index_scan_time > 0 else 'N/A'
            })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in scan comparison: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/index-analysis', methods=['GET'])
def index_analysis():
    """인덱스 사용률 및 효율성 분석"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 인덱스 사용 통계
                cursor.execute("""
                    SELECT
                        schemaname,
                        relname as tablename,
                        indexrelname as indexname,
                        idx_tup_read,
                        idx_tup_fetch,
                        CASE
                            WHEN idx_tup_read > 0
                            THEN round(100.0 * idx_tup_fetch / idx_tup_read, 2)
                            ELSE 0
                        END as efficiency_percent
                    FROM pg_stat_user_indexes
                    ORDER BY idx_tup_read DESC
                """)
                index_stats = cursor.fetchall()

                # 사용되지 않는 인덱스
                cursor.execute("""
                    SELECT
                        schemaname,
                        relname as tablename,
                        indexrelname as indexname,
                        pg_size_pretty(pg_relation_size(indexrelid)) as size
                    FROM pg_stat_user_indexes
                    WHERE idx_tup_read = 0
                    AND idx_tup_fetch = 0
                    AND indexrelname NOT LIKE '%_pkey'
                """)
                unused_indexes = cursor.fetchall()

            return jsonify({
                'index_statistics': [dict(row) for row in index_stats],
                'unused_indexes': [dict(row) for row in unused_indexes]
            })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in index analysis: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/table-stats', methods=['GET'])
def table_stats():
    """테이블 통계 및 성능 정보"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        schemaname,
                        relname as tablename,
                        n_tup_ins as inserts,
                        n_tup_upd as updates,
                        n_tup_del as deletes,
                        n_live_tup as live_tuples,
                        n_dead_tup as dead_tuples,
                        CASE
                            WHEN n_live_tup > 0
                            THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                            ELSE 0
                        END as dead_tuple_percent,
                        pg_size_pretty(pg_total_relation_size(relid)) as total_size
                    FROM pg_stat_user_tables
                    ORDER BY n_live_tup DESC
                """)
                table_stats = cursor.fetchall()

            return jsonify({
                'table_statistics': [dict(row) for row in table_stats]
            })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in table stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/query-plan', methods=['POST'])
def query_plan():
    """쿼리 실행 계획 분석"""
    try:
        data = request.get_json()
        query = data.get('query')

        if not query:
            return jsonify({"error": "Query is required"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 실행 시간 측정
                start_time = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                execution_time = time.time() - start_time

                # 실행 계획 분석
                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
                plan = cursor.fetchone()[0][0]

            return jsonify({
                'query': query,
                'execution_time_ms': round(execution_time * 1000, 2),
                'row_count': len(results),
                'execution_plan': plan,
                'sample_results': [dict(row) for row in results[:5]]
            })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in query plan: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/index-hints-simple', methods=['POST'])
def index_hints_simple():
    """간단한 인덱스 힌트 테스트"""
    try:
        data = request.get_json() or {}
        base_query = data.get('query', "SELECT order_id FROM orders LIMIT 10")

        conn = psycopg2.connect(
            host='postgres', database='ecommerce',
            user='postgres', password='postgres',
            cursor_factory=RealDictCursor
        )

        results = {}

        with conn.cursor() as cursor:
            # 1. 기본 실행
            start_time = time.time()
            cursor.execute(base_query)
            base_results = cursor.fetchall()
            base_time = time.time() - start_time

            results['default'] = {
                'execution_time_ms': round(base_time * 1000, 2),
                'row_count': len(base_results)
            }

            # 2. 인덱스 스캔 강제
            cursor.execute("SET enable_seqscan = OFF")
            start_time = time.time()
            cursor.execute(base_query)
            index_results = cursor.fetchall()
            index_time = time.time() - start_time

            results['force_index'] = {
                'execution_time_ms': round(index_time * 1000, 2),
                'row_count': len(index_results)
            }

            # 설정 리셋
            cursor.execute("RESET enable_seqscan")

        conn.close()

        return jsonify({
            'query': base_query,
            'experiments': results,
            'success': True
        })

    except Exception as e:
        import traceback
        logger.error(f"Index hints error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/index-hints', methods=['POST'])
def index_hints():
    """인덱스 힌트 실험"""
    try:
        data = request.get_json()
        base_query = data.get('query')
        table = data.get('table', 'orders')

        if not base_query:
            return jsonify({"error": "Query is required"}), 400

        results = {}

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 기본 쿼리 (옵티마이저 선택)
                start_time = time.time()
                cursor.execute(base_query)
                base_results = cursor.fetchall()
                base_time = time.time() - start_time

                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {base_query}")
                plan_result = cursor.fetchone()
                base_plan = plan_result[0][0] if plan_result and plan_result[0] else {}

                results['default'] = {
                    'execution_time_ms': round(base_time * 1000, 2),
                    'row_count': len(base_results),
                    'plan': base_plan
                }

                # 2. 인덱스 스캔 강제
                cursor.execute("SET enable_seqscan = OFF")
                start_time = time.time()
                cursor.execute(base_query)
                index_results = cursor.fetchall()
                index_time = time.time() - start_time

                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {base_query}")
                plan_result = cursor.fetchone()
                index_plan = plan_result[0][0] if plan_result and plan_result[0] else {}

                results['force_index'] = {
                    'execution_time_ms': round(index_time * 1000, 2),
                    'row_count': len(index_results),
                    'plan': index_plan
                }

                # 3. 시퀀셜 스캔 강제
                cursor.execute("SET enable_seqscan = ON")
                cursor.execute("SET enable_indexscan = OFF")
                cursor.execute("SET enable_bitmapscan = OFF")
                start_time = time.time()
                cursor.execute(base_query)
                seq_results = cursor.fetchall()
                seq_time = time.time() - start_time

                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {base_query}")
                plan_result = cursor.fetchone()
                seq_plan = plan_result[0][0] if plan_result and plan_result[0] else {}

                results['force_seqscan'] = {
                    'execution_time_ms': round(seq_time * 1000, 2),
                    'row_count': len(seq_results),
                    'plan': seq_plan
                }

                # 설정 리셋
                cursor.execute("RESET enable_seqscan")
                cursor.execute("RESET enable_indexscan")
                cursor.execute("RESET enable_bitmapscan")

            return jsonify({
                'query': base_query,
                'experiments': results,
                'analysis': {
                    'fastest': min(results, key=lambda x: results[x]['execution_time_ms']),
                    'slowest': max(results, key=lambda x: results[x]['execution_time_ms']),
                    'speedup_ratio': round(
                        max(results[x]['execution_time_ms'] for x in results) /
                        min(results[x]['execution_time_ms'] for x in results), 2
                    )
                }
            })
        finally:
            conn.close()

    except Exception as e:
        import traceback
        logger.error(f"Error in index hints: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

@app.route('/db-tuning/advanced-indexing', methods=['POST'])
def advanced_indexing():
    """고급 인덱스 실험 - 복합인덱스, 부분인덱스, 함수기반인덱스"""
    try:
        data = request.get_json() or {}
        experiment_type = data.get('type', 'composite_index')

        conn = psycopg2.connect(
            host='postgres', database='ecommerce',
            user='postgres', password='postgres',
            cursor_factory=RealDictCursor
        )

        results = {}

        with conn.cursor() as cursor:
            if experiment_type == 'composite_index':
                # 복합 인덱스 실험: 단일 vs 복합 인덱스 성능 비교
                test_query = """
                    SELECT o.order_id, o.order_date, o.total_amount
                    FROM orders o
                    WHERE o.status = 'shipped' AND o.order_date >= '2023-01-01'
                    LIMIT 100
                """

                # 1. 인덱스 없이 실행
                cursor.execute("DROP INDEX IF EXISTS idx_orders_status")
                cursor.execute("DROP INDEX IF EXISTS idx_orders_date")
                cursor.execute("DROP INDEX IF EXISTS idx_orders_composite")

                start_time = time.time()
                cursor.execute(test_query)
                no_index_results = cursor.fetchall()
                no_index_time = time.time() - start_time

                results['no_index'] = {
                    'execution_time_ms': round(no_index_time * 1000, 2),
                    'row_count': len(no_index_results)
                }

                # 2. 단일 인덱스들 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)")

                start_time = time.time()
                cursor.execute(test_query)
                single_index_results = cursor.fetchall()
                single_index_time = time.time() - start_time

                results['single_indexes'] = {
                    'execution_time_ms': round(single_index_time * 1000, 2),
                    'row_count': len(single_index_results)
                }

                # 3. 복합 인덱스 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_composite ON orders(status, order_date)")

                start_time = time.time()
                cursor.execute(test_query)
                composite_results = cursor.fetchall()
                composite_time = time.time() - start_time

                results['composite_index'] = {
                    'execution_time_ms': round(composite_time * 1000, 2),
                    'row_count': len(composite_results)
                }

            elif experiment_type == 'partial_index':
                # 부분 인덱스 실험: 전체 vs 부분 인덱스
                test_query = """
                    SELECT * FROM orders
                    WHERE status = 'pending' AND total_amount > 100
                    LIMIT 50
                """

                # 1. 전체 인덱스
                cursor.execute("DROP INDEX IF EXISTS idx_orders_full")
                cursor.execute("DROP INDEX IF EXISTS idx_orders_partial")
                cursor.execute("CREATE INDEX idx_orders_full ON orders(status, total_amount)")

                start_time = time.time()
                cursor.execute(test_query)
                full_index_results = cursor.fetchall()
                full_index_time = time.time() - start_time

                results['full_index'] = {
                    'execution_time_ms': round(full_index_time * 1000, 2),
                    'row_count': len(full_index_results)
                }

                # 2. 부분 인덱스 (status='pending'인 것만)
                cursor.execute("DROP INDEX idx_orders_full")
                cursor.execute("CREATE INDEX idx_orders_partial ON orders(total_amount) WHERE status = 'pending'")

                start_time = time.time()
                cursor.execute(test_query)
                partial_results = cursor.fetchall()
                partial_time = time.time() - start_time

                results['partial_index'] = {
                    'execution_time_ms': round(partial_time * 1000, 2),
                    'row_count': len(partial_results)
                }

            elif experiment_type == 'functional_index':
                # 함수 기반 인덱스 실험
                test_query = """
                    SELECT user_id, COUNT(*) as order_count
                    FROM orders
                    WHERE EXTRACT(YEAR FROM order_date) = 2023
                    GROUP BY user_id
                    LIMIT 20
                """

                # 1. 일반 인덱스
                cursor.execute("DROP INDEX IF EXISTS idx_orders_date_func")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date_normal ON orders(order_date)")

                start_time = time.time()
                cursor.execute(test_query)
                normal_results = cursor.fetchall()
                normal_time = time.time() - start_time

                results['normal_index'] = {
                    'execution_time_ms': round(normal_time * 1000, 2),
                    'row_count': len(normal_results)
                }

                # 2. 함수 기반 인덱스
                cursor.execute("DROP INDEX IF EXISTS idx_orders_date_normal")
                cursor.execute("CREATE INDEX idx_orders_date_func ON orders(EXTRACT(YEAR FROM order_date))")

                start_time = time.time()
                cursor.execute(test_query)
                func_results = cursor.fetchall()
                func_time = time.time() - start_time

                results['functional_index'] = {
                    'execution_time_ms': round(func_time * 1000, 2),
                    'row_count': len(func_results)
                }

        conn.close()

        return jsonify({
            'experiment_type': experiment_type,
            'results': results,
            'analysis': analyze_indexing_results(results, experiment_type)
        })

    except Exception as e:
        import traceback
        logger.error(f"Error in advanced indexing: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

def analyze_indexing_results(results, experiment_type):
    """인덱스 실험 결과 분석"""
    analysis = {'recommendations': [], 'best_performer': None}

    if not results:
        return analysis

    # 가장 빠른 결과 찾기
    fastest = min(results.keys(), key=lambda x: results[x]['execution_time_ms'])
    analysis['best_performer'] = fastest

    if experiment_type == 'composite_index':
        if fastest == 'composite_index':
            analysis['recommendations'].append("복합 인덱스가 단일 인덱스보다 효율적입니다")
        elif fastest == 'single_indexes':
            analysis['recommendations'].append("이 쿼리에는 단일 인덱스가 충분합니다")
        else:
            analysis['recommendations'].append("인덱스 사용이 오히려 성능을 저하시킬 수 있습니다")

    elif experiment_type == 'partial_index':
        if fastest == 'partial_index':
            analysis['recommendations'].append("부분 인덱스가 저장공간과 성능 모두에서 효율적입니다")
        else:
            analysis['recommendations'].append("전체 인덱스가 더 나은 성능을 보입니다")

    elif experiment_type == 'functional_index':
        if fastest == 'functional_index':
            analysis['recommendations'].append("함수 기반 인덱스가 복잡한 조건에서 효율적입니다")
        else:
            analysis['recommendations'].append("일반 인덱스로도 충분한 성능을 얻을 수 있습니다")

    return analysis

@app.route('/db-tuning/query-optimization', methods=['POST'])
def query_optimization():
    """복잡한 쿼리 최적화 실험"""
    try:
        data = request.get_json()
        optimization_type = data.get('type', 'join_optimization')

        optimizations = {}

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                if optimization_type == 'join_optimization':
                    # JOIN 최적화 실험
                    queries = {
                        'nested_loop': """
                            SET enable_hashjoin = OFF;
                            SET enable_mergejoin = OFF;
                            SELECT o.order_id, u.name, p.name as product_name, o.total_amount
                            FROM orders o
                            JOIN users u ON o.user_id = u.user_id
                            JOIN order_items oi ON o.order_id = oi.order_id
                            JOIN products p ON oi.product_id = p.product_id
                            WHERE o.status = 'shipped' LIMIT 100;
                        """,
                        'hash_join': """
                            SET enable_nestloop = OFF;
                            SET enable_mergejoin = OFF;
                            SELECT o.order_id, u.name, p.name as product_name, o.total_amount
                            FROM orders o
                            JOIN users u ON o.user_id = u.user_id
                            JOIN order_items oi ON o.order_id = oi.order_id
                            JOIN products p ON oi.product_id = p.product_id
                            WHERE o.status = 'shipped' LIMIT 100;
                        """,
                        'merge_join': """
                            SET enable_nestloop = OFF;
                            SET enable_hashjoin = OFF;
                            SELECT o.order_id, u.name, p.name as product_name, o.total_amount
                            FROM orders o
                            JOIN users u ON o.user_id = u.user_id
                            JOIN order_items oi ON o.order_id = oi.order_id
                            JOIN products p ON oi.product_id = p.product_id
                            WHERE o.status = 'shipped' LIMIT 100;
                        """
                    }

                elif optimization_type == 'subquery_optimization':
                    # 서브쿼리 최적화 실험
                    queries = {
                        'exists_subquery': """
                            SELECT u.user_id, u.name
                            FROM users u
                            WHERE EXISTS (
                                SELECT 1 FROM orders o
                                WHERE o.user_id = u.user_id AND o.status = 'shipped'
                            ) LIMIT 100;
                        """,
                        'join_instead': """
                            SELECT DISTINCT u.user_id, u.name
                            FROM users u
                            JOIN orders o ON u.user_id = o.user_id
                            WHERE o.status = 'shipped' LIMIT 100;
                        """,
                        'in_subquery': """
                            SELECT u.user_id, u.name
                            FROM users u
                            WHERE u.user_id IN (
                                SELECT o.user_id FROM orders o WHERE o.status = 'shipped'
                            ) LIMIT 100;
                        """
                    }

                # 각 쿼리 실행 및 성능 측정
                for query_name, query in queries.items():
                    try:
                        start_time = time.time()
                        cursor.execute(query)
                        results = cursor.fetchall()
                        execution_time = time.time() - start_time

                        # 실행 계획 가져오기 (마지막 SELECT문만)
                        last_query = query.strip().split(';')[-2] + ';'  # 마지막 SELECT
                        cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {last_query}")
                        plan = cursor.fetchone()[0][0]

                        optimizations[query_name] = {
                            'execution_time_ms': round(execution_time * 1000, 2),
                            'row_count': len(results),
                            'plan': plan
                        }

                    except Exception as e:
                        optimizations[query_name] = {'error': str(e)}

                # 설정 리셋
                cursor.execute("RESET ALL")

            return jsonify({
                'optimization_type': optimization_type,
                'results': optimizations,
                'analysis': {
                    'fastest': min([k for k in optimizations if 'error' not in optimizations[k]],
                                  key=lambda x: optimizations[x]['execution_time_ms'], default=None),
                    'recommendations': generate_optimization_recommendations(optimizations)
                }
            })
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in query optimization: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/db-tuning/query-benchmark', methods=['POST'])
def query_benchmark():
    """쿼리 성능 벤치마크 - 다양한 쿼리 패턴의 성능 측정"""
    try:
        data = request.get_json() or {}
        benchmark_type = data.get('type', 'basic_queries')
        iterations = data.get('iterations', 5)

        conn = psycopg2.connect(
            host='postgres', database='ecommerce',
            user='postgres', password='postgres',
            cursor_factory=RealDictCursor
        )

        benchmark_results = {}

        with conn.cursor() as cursor:
            if benchmark_type == 'basic_queries':
                # 기본 쿼리 패턴들
                queries = {
                    'simple_select': "SELECT * FROM orders LIMIT 1000",
                    'where_clause': "SELECT * FROM orders WHERE status = 'shipped' LIMIT 500",
                    'join_query': """
                        SELECT o.order_id, u.name, o.total_amount
                        FROM orders o JOIN users u ON o.user_id = u.user_id
                        LIMIT 500
                    """,
                    'group_by': """
                        SELECT status, COUNT(*) as count, AVG(total_amount) as avg_amount
                        FROM orders
                        GROUP BY status
                    """,
                    'order_by': "SELECT * FROM orders ORDER BY order_date DESC LIMIT 500"
                }

            elif benchmark_type == 'complex_queries':
                # 복잡한 쿼리 패턴들
                queries = {
                    'subquery': """
                        SELECT * FROM orders o
                        WHERE o.user_id IN (
                            SELECT user_id FROM users WHERE name LIKE 'User%'
                        )
                        LIMIT 100
                    """,
                    'window_function': """
                        SELECT order_id, total_amount,
                               ROW_NUMBER() OVER (PARTITION BY status ORDER BY total_amount DESC) as rank
                        FROM orders
                        LIMIT 1000
                    """,
                    'multiple_joins': """
                        SELECT o.order_id, u.name, p.name as product_name, oi.quantity
                        FROM orders o
                        JOIN users u ON o.user_id = u.user_id
                        JOIN order_items oi ON o.order_id = oi.order_id
                        JOIN products p ON oi.product_id = p.product_id
                        LIMIT 200
                    """,
                    'aggregation': """
                        SELECT u.name, COUNT(o.order_id) as total_orders,
                               SUM(o.total_amount) as total_spent,
                               AVG(o.total_amount) as avg_order
                        FROM users u
                        LEFT JOIN orders o ON u.user_id = o.user_id
                        GROUP BY u.user_id, u.name
                        HAVING COUNT(o.order_id) > 5
                        LIMIT 100
                    """
                }

            elif benchmark_type == 'analytical_queries':
                # 분석용 쿼리들
                queries = {
                    'daily_sales': """
                        SELECT DATE(order_date) as date,
                               COUNT(*) as orders,
                               SUM(total_amount) as revenue
                        FROM orders
                        WHERE order_date >= '2023-01-01'
                        GROUP BY DATE(order_date)
                        ORDER BY date
                        LIMIT 100
                    """,
                    'top_products': """
                        SELECT p.name, SUM(oi.quantity) as total_sold,
                               SUM(oi.total_price) as total_revenue
                        FROM products p
                        JOIN order_items oi ON p.product_id = oi.product_id
                        JOIN orders o ON oi.order_id = o.order_id
                        WHERE o.status = 'completed'
                        GROUP BY p.product_id, p.name
                        ORDER BY total_sold DESC
                        LIMIT 20
                    """,
                    'user_behavior': """
                        SELECT ub.event_type, COUNT(*) as event_count,
                               COUNT(DISTINCT ub.user_id) as unique_users
                        FROM user_behavior_log ub
                        WHERE ub.timestamp >= NOW() - INTERVAL '7 days'
                        GROUP BY ub.event_type
                    """
                }

            # 각 쿼리를 여러번 실행해서 평균 성능 측정
            for query_name, query_sql in queries.items():
                execution_times = []
                row_counts = []

                for i in range(iterations):
                    start_time = time.time()
                    try:
                        cursor.execute(query_sql)
                        results = cursor.fetchall()
                        execution_time = time.time() - start_time
                        execution_times.append(execution_time * 1000)  # ms로 변환
                        row_counts.append(len(results))
                    except Exception as e:
                        logger.error(f"Error in query {query_name}: {e}")
                        execution_times.append(None)
                        row_counts.append(0)

                # 유효한 실행시간만 필터링
                valid_times = [t for t in execution_times if t is not None]

                if valid_times:
                    benchmark_results[query_name] = {
                        'avg_execution_time_ms': round(sum(valid_times) / len(valid_times), 2),
                        'min_execution_time_ms': round(min(valid_times), 2),
                        'max_execution_time_ms': round(max(valid_times), 2),
                        'avg_row_count': round(sum(row_counts) / len(row_counts)),
                        'iterations': len(valid_times),
                        'query': query_sql.strip()
                    }
                else:
                    benchmark_results[query_name] = {
                        'error': 'All iterations failed',
                        'query': query_sql.strip()
                    }

        conn.close()

        # 성능 분석
        analysis = analyze_benchmark_results(benchmark_results)

        return jsonify({
            'benchmark_type': benchmark_type,
            'iterations': iterations,
            'results': benchmark_results,
            'analysis': analysis
        })

    except Exception as e:
        import traceback
        logger.error(f"Error in query benchmark: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

def analyze_benchmark_results(results):
    """벤치마크 결과 분석"""
    analysis = {
        'fastest_query': None,
        'slowest_query': None,
        'performance_insights': [],
        'recommendations': []
    }

    valid_results = {k: v for k, v in results.items() if 'error' not in v}

    if not valid_results:
        return analysis

    # 가장 빠른/느린 쿼리 찾기
    fastest = min(valid_results.keys(), key=lambda x: valid_results[x]['avg_execution_time_ms'])
    slowest = max(valid_results.keys(), key=lambda x: valid_results[x]['avg_execution_time_ms'])

    analysis['fastest_query'] = {
        'name': fastest,
        'time_ms': valid_results[fastest]['avg_execution_time_ms']
    }
    analysis['slowest_query'] = {
        'name': slowest,
        'time_ms': valid_results[slowest]['avg_execution_time_ms']
    }

    # 성능 인사이트 생성
    avg_time = sum(v['avg_execution_time_ms'] for v in valid_results.values()) / len(valid_results)

    for query_name, data in valid_results.items():
        if data['avg_execution_time_ms'] > avg_time * 2:
            analysis['performance_insights'].append(f"{query_name}: 평균보다 2배 이상 느림")
        elif data['avg_execution_time_ms'] < avg_time * 0.5:
            analysis['performance_insights'].append(f"{query_name}: 매우 빠른 실행시간")

    # 추천사항
    if valid_results[slowest]['avg_execution_time_ms'] > 1000:  # 1초 이상
        analysis['recommendations'].append("느린 쿼리에 대해 인덱스 추가를 고려하세요")
    if len([v for v in valid_results.values() if 'JOIN' in v['query'].upper()]) > 0:
        analysis['recommendations'].append("JOIN 쿼리 성능 최적화를 위해 적절한 인덱스를 확인하세요")

    return analysis

def generate_optimization_recommendations(results):
    """최적화 추천 생성"""
    recommendations = []

    if not results:
        return recommendations

    # 에러가 없는 결과들만 필터링
    valid_results = {k: v for k, v in results.items() if 'error' not in v}

    if len(valid_results) < 2:
        return ["Need more data points for comparison"]

    # 가장 빠른 방법 찾기
    fastest = min(valid_results, key=lambda x: valid_results[x]['execution_time_ms'])
    slowest = max(valid_results, key=lambda x: valid_results[x]['execution_time_ms'])

    fastest_time = valid_results[fastest]['execution_time_ms']
    slowest_time = valid_results[slowest]['execution_time_ms']

    if slowest_time > fastest_time * 2:
        recommendations.append(f"Use {fastest} instead of {slowest} for {round(slowest_time/fastest_time, 1)}x speedup")

    if 'nested_loop' in valid_results and valid_results['nested_loop']['execution_time_ms'] > 1000:
        recommendations.append("Nested loop join is slow - consider adding indexes or using hash join")

    if 'exists_subquery' in valid_results and 'join_instead' in valid_results:
        if valid_results['join_instead']['execution_time_ms'] < valid_results['exists_subquery']['execution_time_ms']:
            recommendations.append("JOIN is faster than EXISTS subquery in this case")

    return recommendations

@app.route('/db-tuning/database-health', methods=['GET'])
def database_health():
    """데이터베이스 상태 종합 모니터링"""
    try:
        conn = psycopg2.connect(
            host='postgres', database='ecommerce',
            user='postgres', password='postgres',
            cursor_factory=RealDictCursor
        )

        health_report = {}

        with conn.cursor() as cursor:
            # 1. 연결 상태
            cursor.execute("SELECT COUNT(*) as active_connections FROM pg_stat_activity WHERE state = 'active'")
            active_connections = cursor.fetchone()['active_connections']

            cursor.execute("SELECT setting::int as max_connections FROM pg_settings WHERE name = 'max_connections'")
            max_connections = cursor.fetchone()['max_connections']

            health_report['connections'] = {
                'active': active_connections,
                'max': max_connections,
                'usage_percent': round((active_connections / max_connections) * 100, 1)
            }

            # 2. 캐시 히트율
            cursor.execute("""
                SELECT
                    sum(heap_blks_read) as heap_read,
                    sum(heap_blks_hit) as heap_hit,
                    round(sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) * 100, 2) as cache_hit_ratio
                FROM pg_statio_user_tables
                WHERE heap_blks_read + heap_blks_hit > 0
            """)
            cache_stats = cursor.fetchone()

            health_report['cache_performance'] = {
                'hit_ratio_percent': float(cache_stats['cache_hit_ratio']) if cache_stats['cache_hit_ratio'] else 0,
                'status': 'Good' if cache_stats['cache_hit_ratio'] and float(cache_stats['cache_hit_ratio']) > 95 else 'Needs Attention'
            }

            # 3. 테이블 크기 및 dead tuples
            cursor.execute("""
                SELECT
                    schemaname, relname as tablename,
                    pg_size_pretty(pg_total_relation_size(relid)) as size,
                    n_dead_tup,
                    n_live_tup,
                    CASE
                        WHEN n_live_tup > 0
                        THEN round(n_dead_tup::numeric / (n_live_tup + n_dead_tup) * 100, 2)
                        ELSE 0
                    END as dead_tuple_percent
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(relid) DESC
                LIMIT 10
            """)
            table_health = cursor.fetchall()

            health_report['table_health'] = [dict(row) for row in table_health]

            # 4. 락 정보
            cursor.execute("""
                SELECT
                    mode,
                    COUNT(*) as lock_count
                FROM pg_locks
                WHERE granted = true
                GROUP BY mode
                ORDER BY lock_count DESC
            """)
            locks = cursor.fetchall()
            health_report['locks'] = [dict(row) for row in locks]

            # 5. 느린 쿼리 (현재 실행 중)
            cursor.execute("""
                SELECT
                    pid,
                    now() - pg_stat_activity.query_start AS duration,
                    query,
                    state
                FROM pg_stat_activity
                WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
                AND state = 'active'
                AND query NOT LIKE '%pg_stat_activity%'
            """)
            slow_queries = cursor.fetchall()
            health_report['slow_queries'] = [
                {
                    'pid': row['pid'],
                    'duration_seconds': row['duration'].total_seconds() if row['duration'] else 0,
                    'query': row['query'][:200] + '...' if len(row['query']) > 200 else row['query'],
                    'state': row['state']
                }
                for row in slow_queries
            ]

            # 6. 디스크 사용량
            cursor.execute("""
                SELECT
                    pg_size_pretty(pg_database_size(current_database())) as database_size,
                    pg_size_pretty(sum(pg_total_relation_size(relid))) as tables_size
                FROM pg_stat_user_tables
            """)
            size_info = cursor.fetchone()
            health_report['disk_usage'] = dict(size_info)

            # 7. 인덱스 효율성
            cursor.execute("""
                SELECT
                    schemaname, relname as tablename, indexrelname as indexname,
                    idx_tup_read, idx_tup_fetch,
                    CASE
                        WHEN idx_tup_read > 0
                        THEN round(idx_tup_fetch::numeric / idx_tup_read * 100, 2)
                        ELSE 0
                    END as efficiency_percent
                FROM pg_stat_user_indexes
                WHERE idx_tup_read > 1000
                ORDER BY idx_tup_read DESC
                LIMIT 10
            """)
            index_efficiency = cursor.fetchall()
            health_report['index_efficiency'] = [dict(row) for row in index_efficiency]

        conn.close()

        # 전체 상태 평가
        health_score = calculate_health_score(health_report)
        health_report['overall_health'] = health_score

        return jsonify({
            'timestamp': datetime.now().isoformat(),
            'health_report': health_report,
            'recommendations': generate_health_recommendations(health_report)
        })

    except Exception as e:
        import traceback
        logger.error(f"Error in database health check: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

def calculate_health_score(health_report):
    """데이터베이스 건강도 점수 계산 (0-100)"""
    score = 100

    # 연결 사용률이 80% 이상이면 감점
    if health_report['connections']['usage_percent'] > 80:
        score -= 20

    # 캐시 히트율이 95% 미만이면 감점
    if health_report['cache_performance']['hit_ratio_percent'] < 95:
        score -= 15

    # dead tuple이 많은 테이블이 있으면 감점
    for table in health_report['table_health']:
        if float(table['dead_tuple_percent']) > 20:
            score -= 10

    # 느린 쿼리가 있으면 감점
    if health_report['slow_queries']:
        score -= len(health_report['slow_queries']) * 5

    return {
        'score': max(0, score),
        'status': 'Excellent' if score >= 90 else 'Good' if score >= 70 else 'Fair' if score >= 50 else 'Poor'
    }

def generate_health_recommendations(health_report):
    """건강도 기반 추천사항 생성"""
    recommendations = []

    # 연결 수 확인
    if health_report['connections']['usage_percent'] > 80:
        recommendations.append({
            'category': 'connections',
            'message': f"연결 사용률이 {health_report['connections']['usage_percent']}%로 높습니다. 연결 풀링을 고려하세요.",
            'priority': 'high'
        })

    # 캐시 성능 확인
    if health_report['cache_performance']['hit_ratio_percent'] < 95:
        recommendations.append({
            'category': 'cache',
            'message': f"캐시 히트율이 {health_report['cache_performance']['hit_ratio_percent']}%로 낮습니다. shared_buffers 증가를 고려하세요.",
            'priority': 'medium'
        })

    # dead tuple 확인
    for table in health_report['table_health']:
        if float(table['dead_tuple_percent']) > 20:
            recommendations.append({
                'category': 'maintenance',
                'message': f"테이블 {table['tablename']}의 dead tuple이 {table['dead_tuple_percent']}%입니다. VACUUM을 실행하세요.",
                'priority': 'medium'
            })

    # 느린 쿼리 확인
    if health_report['slow_queries']:
        recommendations.append({
            'category': 'performance',
            'message': f"{len(health_report['slow_queries'])}개의 느린 쿼리가 실행 중입니다. 쿼리 최적화가 필요합니다.",
            'priority': 'high'
        })

    # 인덱스 효율성 확인
    inefficient_indexes = [idx for idx in health_report['index_efficiency'] if float(idx['efficiency_percent']) < 50]
    if inefficient_indexes:
        recommendations.append({
            'category': 'indexes',
            'message': f"{len(inefficient_indexes)}개의 비효율적인 인덱스가 발견되었습니다. 인덱스 재검토가 필요합니다.",
            'priority': 'low'
        })

    return recommendations

@app.route('/heavy-join-query')
def heavy_join_query():
    """무거운 조인 쿼리 - 실제 복잡한 SQL이 로그에 나타나도록"""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # 복잡한 JOIN과 윈도우 함수가 포함된 쿼리
                heavy_query = """
                    SELECT
                        o.order_id,
                        o.user_id,
                        o.created_at,
                        oi.product_id,
                        p.name as product_name,
                        c.name as category_name,
                        b.name as brand_name,
                        oi.quantity,
                        oi.unit_price,
                        (oi.quantity * oi.unit_price) as item_total,
                        ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at DESC) as user_order_rank,
                        COUNT(*) OVER (PARTITION BY o.user_id) as user_total_orders,
                        AVG(oi.unit_price) OVER (PARTITION BY p.category_id) as category_avg_price,
                        RANK() OVER (PARTITION BY p.category_id ORDER BY (oi.quantity * oi.unit_price) DESC) as item_value_rank_in_category
                    FROM orders o
                    INNER JOIN order_items oi ON o.order_id = oi.order_id
                    INNER JOIN products p ON oi.product_id = p.product_id
                    INNER JOIN categories c ON p.category_id = c.category_id
                    INNER JOIN brands b ON p.brand_id = b.brand_id
                    WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY o.created_at DESC, (oi.quantity * oi.unit_price) DESC
                    LIMIT 100
                """

                cursor.execute(heavy_query)
                results = cursor.fetchall()

                return jsonify({
                    "message": "Heavy JOIN query with window functions executed",
                    "result_count": len(results),
                    "query_complexity": "Multiple JOINs + Window Functions (ROW_NUMBER, COUNT, AVG, RANK)"
                })

    except Exception as e:
        logger.error(f"Error in heavy join query: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/complex-sql-test')
def complex_sql_test():
    """복잡한 SQL 쿼리 테스트 - 다양한 복잡한 쿼리들을 순차 실행"""
    try:
        conn = get_db_connection()
        results = []

        with conn:
            with conn.cursor() as cursor:
                # 1. 복잡한 JOIN 쿼리
                complex_join_query = """
                    SELECT
                        p.name as product_name,
                        c.name as category,
                        b.name as brand,
                        COUNT(DISTINCT oi.order_id) as total_orders,
                        SUM(oi.quantity) as total_sold,
                        AVG(oi.unit_price) as avg_price,
                        MAX(o.created_at) as last_order_date
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    JOIN brands b ON p.brand_id = b.brand_id
                    LEFT JOIN order_items oi ON p.product_id = oi.product_id
                    LEFT JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY p.product_id, p.name, c.name, b.name
                    HAVING COUNT(DISTINCT oi.order_id) > 5
                    ORDER BY total_sold DESC
                    LIMIT 10
                """
                cursor.execute(complex_join_query)
                join_results = cursor.fetchall()
                results.append({"query_type": "Complex JOIN with aggregation", "count": len(join_results)})

                # 2. 윈도우 함수 쿼리
                window_query = """
                    SELECT
                        user_id,
                        created_at,
                        LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at) as prev_order,
                        LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at) as next_order,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) as order_sequence,
                        COUNT(*) OVER (PARTITION BY user_id) as total_user_orders
                    FROM orders
                    WHERE created_at >= CURRENT_DATE - INTERVAL '60 days'
                    ORDER BY user_id, created_at
                    LIMIT 50
                """
                cursor.execute(window_query)
                window_results = cursor.fetchall()
                results.append({"query_type": "Window functions with LAG/LEAD", "count": len(window_results)})

                # 3. 서브쿼리 및 CTE
                cte_query = """
                    WITH monthly_sales AS (
                        SELECT
                            DATE_TRUNC('month', o.created_at) as month,
                            SUM(oi.quantity * oi.unit_price) as monthly_revenue,
                            COUNT(DISTINCT o.order_id) as monthly_orders
                        FROM orders o
                        JOIN order_items oi ON o.order_id = oi.order_id
                        WHERE o.created_at >= CURRENT_DATE - INTERVAL '6 months'
                        GROUP BY DATE_TRUNC('month', o.created_at)
                    ),
                    revenue_stats AS (
                        SELECT
                            month,
                            monthly_revenue,
                            monthly_orders,
                            LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
                            CASE
                                WHEN LAG(monthly_revenue) OVER (ORDER BY month) > 0
                                THEN ((monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) /
                                      LAG(monthly_revenue) OVER (ORDER BY month)) * 100
                                ELSE 0
                            END as growth_rate
                        FROM monthly_sales
                    )
                    SELECT * FROM revenue_stats
                    WHERE growth_rate IS NOT NULL
                    ORDER BY month DESC
                """
                cursor.execute(cte_query)
                cte_results = cursor.fetchall()
                results.append({"query_type": "CTE with growth rate calculation", "count": len(cte_results)})

                # 4. 집계 및 통계 함수
                stats_query = """
                    SELECT
                        c.name as category,
                        COUNT(DISTINCT p.product_id) as product_count,
                        AVG(p.price) as avg_price,
                        STDDEV(p.price) as price_stddev,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.price) as median_price,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY p.price) as p95_price,
                        MIN(p.price) as min_price,
                        MAX(p.price) as max_price
                    FROM categories c
                    JOIN products p ON c.category_id = p.category_id
                    GROUP BY c.category_id, c.name
                    HAVING COUNT(DISTINCT p.product_id) > 10
                    ORDER BY avg_price DESC
                """
                cursor.execute(stats_query)
                stats_results = cursor.fetchall()
                results.append({"query_type": "Statistical aggregations with percentiles", "count": len(stats_results)})

        return jsonify({
            "message": "Complex SQL queries executed successfully",
            "results": results,
            "total_queries": len(results)
        })

    except Exception as e:
        logger.error(f"Error in complex SQL test: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)