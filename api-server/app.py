from flask import Flask, jsonify, request
import redis
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch
from datetime import datetime

app = Flask(__name__)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis 연결
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Elasticsearch 연결
es = Elasticsearch(['http://elasticsearch:9200'])

# PostgreSQL 연결
def get_db_connection():
    return psycopg2.connect(
        host='postgres',
        database='ecommerce',
        user='postgres',
        password='postgres',
        cursor_factory=RealDictCursor
    )

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

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
    """사용자의 주문 내역 조회 (PostgreSQL)"""
    try:
        page = request.args.get('page', 1, type=int)
        size = request.args.get('size', 20, type=int)
        status = request.args.get('status', '')

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 기본 쿼리
                query = """
                    SELECT o.order_id, o.order_date, o.status, o.total_amount,
                           o.shipping_address, o.payment_method,
                           oi.product_id, oi.quantity, oi.unit_price,
                           p.name as product_name
                    FROM orders o
                    JOIN order_items oi ON o.order_id = oi.order_id
                    JOIN products p ON oi.product_id = p.product_id
                    WHERE o.user_id = %s
                """
                params = [user_id]

                # 상태 필터 추가
                if status:
                    query += " AND o.status = %s"
                    params.append(status)

                query += " ORDER BY o.order_date DESC LIMIT %s OFFSET %s"
                params.extend([size, (page - 1) * size])

                cursor.execute(query, params)
                orders_data = cursor.fetchall()

                # 주문별로 그룹화
                orders = {}
                for row in orders_data:
                    order_id = row['order_id']
                    if order_id not in orders:
                        orders[order_id] = {
                            'order_id': order_id,
                            'order_date': row['order_date'].isoformat(),
                            'status': row['status'],
                            'total_amount': float(row['total_amount']),
                            'shipping_address': row['shipping_address'],
                            'payment_method': row['payment_method'],
                            'items': []
                        }

                    orders[order_id]['items'].append({
                        'product_id': row['product_id'],
                        'product_name': row['product_name'],
                        'quantity': row['quantity'],
                        'unit_price': float(row['unit_price'])
                    })

                return jsonify({
                    "user_id": user_id,
                    "orders": list(orders.values()),
                    "page": page,
                    "size": size
                })

    except Exception as e:
        logger.error(f"Error getting orders for user {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/products-db', methods=['GET'])
def get_products_from_db():
    """PostgreSQL에서 상품 조회"""
    try:
        page = request.args.get('page', 1, type=int)
        size = request.args.get('size', 20, type=int)
        category = request.args.get('category', '')
        brand = request.args.get('brand', '')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT p.product_id, p.name, p.description, p.price, p.rating,
                           p.stock_quantity, c.name as category, b.name as brand,
                           p.created_at
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                    JOIN brands b ON p.brand_id = b.brand_id
                    WHERE p.is_active = true
                """
                params = []

                # 필터 조건 추가
                if category:
                    query += " AND c.name = %s"
                    params.append(category)

                if brand:
                    query += " AND b.name = %s"
                    params.append(brand)

                if min_price is not None:
                    query += " AND p.price >= %s"
                    params.append(min_price)

                if max_price is not None:
                    query += " AND p.price <= %s"
                    params.append(max_price)

                # 총 개수 조회
                count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]

                # 페이징 추가
                query += " ORDER BY p.created_at DESC LIMIT %s OFFSET %s"
                params.extend([size, (page - 1) * size])

                cursor.execute(query, params)
                products = cursor.fetchall()

                # 데이터 형식 변환
                product_list = []
                for product in products:
                    product_dict = dict(product)
                    product_dict['price'] = float(product_dict['price'])
                    product_dict['rating'] = float(product_dict['rating']) if product_dict['rating'] else 0
                    product_dict['created_at'] = product_dict['created_at'].isoformat()
                    product_list.append(product_dict)

                return jsonify({
                    "products": product_list,
                    "total_count": total_count,
                    "page": page,
                    "size": size
                })

    except Exception as e:
        logger.error(f"Error getting products from database: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/user-behavior/<user_id>', methods=['GET'])
def get_user_behavior(user_id):
    """사용자 행동 분석 (PostgreSQL)"""
    try:
        days = request.args.get('days', 7, type=int)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 최근 N일간 사용자 행동 통계
                cursor.execute("""
                    SELECT
                        action_type,
                        COUNT(*) as count,
                        COUNT(DISTINCT product_id) as unique_products
                    FROM user_behavior_log
                    WHERE user_id = %s
                    AND created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY action_type
                    ORDER BY count DESC
                """, (user_id, days))

                behavior_stats = cursor.fetchall()

                # 최근 활동 상품들
                cursor.execute("""
                    SELECT
                        ubl.product_id,
                        p.name as product_name,
                        c.name as category,
                        COUNT(*) as interaction_count,
                        MAX(ubl.created_at) as last_interaction
                    FROM user_behavior_log ubl
                    JOIN products p ON ubl.product_id = p.product_id
                    JOIN categories c ON p.category_id = c.category_id
                    WHERE ubl.user_id = %s
                    AND ubl.created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY ubl.product_id, p.name, c.name
                    ORDER BY interaction_count DESC, last_interaction DESC
                    LIMIT 10
                """, (user_id, days))

                recent_products = cursor.fetchall()

                # 데이터 형식 변환
                stats = [dict(row) for row in behavior_stats]
                products = []
                for row in recent_products:
                    product_dict = dict(row)
                    product_dict['last_interaction'] = product_dict['last_interaction'].isoformat()
                    products.append(product_dict)

                return jsonify({
                    "user_id": user_id,
                    "period_days": days,
                    "behavior_stats": stats,
                    "recent_products": products
                })

    except Exception as e:
        logger.error(f"Error getting user behavior for {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/analytics/dashboard', methods=['GET'])
def get_analytics_dashboard():
    """분석 대시보드 (PostgreSQL)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 전체 통계
                cursor.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM users) as total_users,
                        (SELECT COUNT(*) FROM products WHERE is_active = true) as total_products,
                        (SELECT COUNT(*) FROM orders) as total_orders,
                        (SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE status IN ('shipped', 'delivered')) as total_revenue
                """)
                overview = dict(cursor.fetchone())

                # 카테고리별 판매 통계
                cursor.execute("""
                    SELECT
                        c.name as category,
                        COUNT(oi.order_item_id) as items_sold,
                        SUM(oi.total_price) as revenue
                    FROM categories c
                    JOIN products p ON c.category_id = p.category_id
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.status IN ('shipped', 'delivered')
                    GROUP BY c.name
                    ORDER BY revenue DESC
                    LIMIT 10
                """)
                category_stats = [dict(row) for row in cursor.fetchall()]

                # 최근 주문 동향 (일별)
                cursor.execute("""
                    SELECT
                        DATE(order_date) as order_date,
                        COUNT(*) as order_count,
                        SUM(total_amount) as daily_revenue
                    FROM orders
                    WHERE order_date >= NOW() - INTERVAL '30 days'
                    GROUP BY DATE(order_date)
                    ORDER BY order_date DESC
                    LIMIT 30
                """)
                daily_orders = []
                for row in cursor.fetchall():
                    row_dict = dict(row)
                    row_dict['order_date'] = row_dict['order_date'].isoformat()
                    row_dict['daily_revenue'] = float(row_dict['daily_revenue'])
                    daily_orders.append(row_dict)

                # 숫자 형식 변환
                overview['total_revenue'] = float(overview['total_revenue'])
                for stat in category_stats:
                    stat['revenue'] = float(stat['revenue'])

                return jsonify({
                    "overview": overview,
                    "category_stats": category_stats,
                    "daily_orders": daily_orders
                })

    except Exception as e:
        logger.error(f"Error getting analytics dashboard: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)