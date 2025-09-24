#!/usr/bin/env python3
"""
DB 튜닝 실험용 API 모듈
- Full Table Scan vs Index Scan 성능 비교
- 쿼리 실행 계획 분석
- 슬로우 쿼리 모니터링
- 인덱스 효율성 분석
"""

import time
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Blueprint, jsonify, request
import logging

logger = logging.getLogger(__name__)

db_tuning_bp = Blueprint('db_tuning', __name__)

def get_db_connection():
    """PostgreSQL 연결"""
    return psycopg2.connect(
        host='postgres',
        database='ecommerce',
        user='postgres',
        password='postgres',
        cursor_factory=RealDictCursor
    )

def measure_query_performance(query, params=None):
    """쿼리 실행 시간 측정"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            start_time = time.time()
            cursor.execute(query, params or [])
            results = cursor.fetchall()
            execution_time = time.time() - start_time

            return {
                'execution_time_ms': round(execution_time * 1000, 2),
                'row_count': len(results),
                'results': results[:10]  # 처음 10개만 반환
            }
    finally:
        conn.close()

def get_query_plan(query, params=None):
    """쿼리 실행 계획 분석"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # EXPLAIN ANALYZE로 실제 실행 계획과 성능 데이터 가져오기
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
            cursor.execute(explain_query, params or [])
            plan = cursor.fetchone()[0][0]

            return plan
    finally:
        conn.close()

@db_tuning_bp.route('/scan-comparison', methods=['GET'])
def scan_comparison():
    """Full Table Scan vs Index Scan 성능 비교"""
    try:
        table = request.args.get('table', 'orders')
        limit = request.args.get('limit', 1000, type=int)

        results = {}

        # 1. Full Table Scan (인덱스 사용 금지)
        full_scan_query = f"""
            SET enable_indexscan = OFF;
            SET enable_bitmapscan = OFF;
            SELECT * FROM {table} LIMIT %s;
        """

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                start_time = time.time()
                cursor.execute("SET enable_indexscan = OFF")
                cursor.execute("SET enable_bitmapscan = OFF")
                cursor.execute(f"SELECT * FROM {table} LIMIT %s", [limit])
                full_scan_results = cursor.fetchall()
                full_scan_time = time.time() - start_time

                # 실행 계획 가져오기
                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM {table} LIMIT %s", [limit])
                full_scan_plan = cursor.fetchone()[0][0]

                # 설정 리셋
                cursor.execute("RESET enable_indexscan")
                cursor.execute("RESET enable_bitmapscan")

                # 2. Index Scan (기본 설정)
                start_time = time.time()
                cursor.execute(f"SELECT * FROM {table} ORDER BY {table[:-1]}_id LIMIT %s", [limit])
                index_scan_results = cursor.fetchall()
                index_scan_time = time.time() - start_time

                # 실행 계획 가져오기
                cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM {table} ORDER BY {table[:-1]}_id LIMIT %s", [limit])
                index_scan_plan = cursor.fetchone()[0][0]

        finally:
            conn.close()

        return jsonify({
            'table': table,
            'limit': limit,
            'full_table_scan': {
                'execution_time_ms': round(full_scan_time * 1000, 2),
                'row_count': len(full_scan_results),
                'execution_plan': full_scan_plan
            },
            'index_scan': {
                'execution_time_ms': round(index_scan_time * 1000, 2),
                'row_count': len(index_scan_results),
                'execution_plan': index_scan_plan
            },
            'performance_ratio': round(full_scan_time / index_scan_time, 2) if index_scan_time > 0 else 'N/A'
        })

    except Exception as e:
        logger.error(f"Error in scan comparison: {e}")
        return jsonify({"error": str(e)}), 500

@db_tuning_bp.route('/query-analyzer', methods=['POST'])
def query_analyzer():
    """사용자 정의 쿼리 성능 분석"""
    try:
        data = request.get_json()
        query = data.get('query')
        params = data.get('params', [])

        if not query:
            return jsonify({"error": "Query is required"}), 400

        # 실행 시간 측정
        performance = measure_query_performance(query, params)

        # 실행 계획 분석
        execution_plan = get_query_plan(query, params)

        # 성능 분석
        analysis = analyze_query_performance(execution_plan)

        return jsonify({
            'query': query,
            'performance': performance,
            'execution_plan': execution_plan,
            'analysis': analysis
        })

    except Exception as e:
        logger.error(f"Error in query analyzer: {e}")
        return jsonify({"error": str(e)}), 500

def analyze_query_performance(plan):
    """실행 계획 기반 성능 분석"""
    analysis = {
        'recommendations': [],
        'bottlenecks': [],
        'scan_types': []
    }

    def traverse_plan(node):
        node_type = node.get('Node Type', '')

        # 스캔 타입 분석
        if 'Scan' in node_type:
            analysis['scan_types'].append({
                'type': node_type,
                'relation': node.get('Relation Name', ''),
                'cost': node.get('Total Cost', 0),
                'rows': node.get('Actual Rows', 0),
                'time': node.get('Actual Total Time', 0)
            })

            # Full Table Scan 감지
            if node_type == 'Seq Scan' and node.get('Actual Rows', 0) > 10000:
                analysis['bottlenecks'].append(f"Full table scan on {node.get('Relation Name', 'unknown')}")
                analysis['recommendations'].append(f"Consider adding index for {node.get('Relation Name', 'unknown')}")

        # 조인 분석
        if 'Join' in node_type:
            if node.get('Actual Total Time', 0) > 100:
                analysis['bottlenecks'].append(f"Slow {node_type}")
                analysis['recommendations'].append("Consider optimizing join conditions or adding indexes")

        # 자식 노드 순회
        for child in node.get('Plans', []):
            traverse_plan(child)

    traverse_plan(plan)
    return analysis

@db_tuning_bp.route('/slow-queries', methods=['GET'])
def slow_queries():
    """슬로우 쿼리 모니터링"""
    try:
        # PostgreSQL의 pg_stat_statements 확장을 사용 (활성화되어 있어야 함)
        query = """
            SELECT
                query,
                calls,
                total_time,
                mean_time,
                rows,
                100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
            FROM pg_stat_statements
            WHERE mean_time > 100  -- 100ms 이상 쿼리
            ORDER BY mean_time DESC
            LIMIT 20
        """

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                slow_queries = cursor.fetchall()
        finally:
            conn.close()

        return jsonify({
            'slow_queries': [dict(row) for row in slow_queries],
            'total_count': len(slow_queries)
        })

    except Exception as e:
        # pg_stat_statements가 없는 경우 대체 방법
        logger.warning(f"pg_stat_statements not available: {e}")
        return jsonify({
            'slow_queries': [],
            'message': 'pg_stat_statements extension not enabled',
            'recommendation': 'Enable pg_stat_statements for query monitoring'
        })

@db_tuning_bp.route('/index-analysis', methods=['GET'])
def index_analysis():
    """인덱스 사용률 및 효율성 분석"""
    try:
        # 인덱스 사용 통계
        query = """
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_tup_read,
                idx_tup_fetch,
                CASE
                    WHEN idx_tup_read > 0
                    THEN round(100.0 * idx_tup_fetch / idx_tup_read, 2)
                    ELSE 0
                END as efficiency_percent
            FROM pg_stat_user_indexes
            ORDER BY idx_tup_read DESC
        """

        # 사용되지 않는 인덱스 찾기
        unused_indexes_query = """
            SELECT
                schemaname,
                tablename,
                indexname,
                pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes
            WHERE idx_tup_read = 0
            AND idx_tup_fetch = 0
            AND indexname NOT LIKE '%_pkey'
        """

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                index_stats = cursor.fetchall()

                cursor.execute(unused_indexes_query)
                unused_indexes = cursor.fetchall()
        finally:
            conn.close()

        return jsonify({
            'index_statistics': [dict(row) for row in index_stats],
            'unused_indexes': [dict(row) for row in unused_indexes],
            'recommendations': generate_index_recommendations(index_stats, unused_indexes)
        })

    except Exception as e:
        logger.error(f"Error in index analysis: {e}")
        return jsonify({"error": str(e)}), 500

def generate_index_recommendations(index_stats, unused_indexes):
    """인덱스 최적화 추천"""
    recommendations = []

    # 사용되지 않는 인덱스 제거 추천
    for index in unused_indexes:
        recommendations.append({
            'type': 'remove_unused_index',
            'message': f"Consider dropping unused index: {index['indexname']}",
            'query': f"DROP INDEX {index['indexname']};",
            'benefit': f"Save {index['size']} disk space"
        })

    # 효율성이 낮은 인덱스 확인
    for index in index_stats:
        if index['idx_tup_read'] > 1000 and index['efficiency_percent'] < 50:
            recommendations.append({
                'type': 'low_efficiency_index',
                'message': f"Index {index['indexname']} has low efficiency ({index['efficiency_percent']}%)",
                'suggestion': "Review query patterns and consider composite indexes"
            })

    return recommendations

@db_tuning_bp.route('/table-stats', methods=['GET'])
def table_stats():
    """테이블 통계 및 성능 정보"""
    try:
        query = """
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
                pg_size_pretty(pg_total_relation_size(relid)) as total_size,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            ORDER BY n_live_tup DESC
        """

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                table_stats = cursor.fetchall()
        finally:
            conn.close()

        # 테이블 최적화 추천
        recommendations = []
        for table in table_stats:
            if table['dead_tuple_percent'] > 20:
                recommendations.append({
                    'table': table['tablename'],
                    'issue': 'High dead tuple percentage',
                    'recommendation': f"VACUUM {table['tablename']};",
                    'dead_tuple_percent': table['dead_tuple_percent']
                })

        return jsonify({
            'table_statistics': [dict(row) for row in table_stats],
            'maintenance_recommendations': recommendations
        })

    except Exception as e:
        logger.error(f"Error in table stats: {e}")
        return jsonify({"error": str(e)}), 500

@db_tuning_bp.route('/connection-pool-stats', methods=['GET'])
def connection_pool_stats():
    """데이터베이스 연결 및 활동 통계"""
    try:
        query = """
            SELECT
                datname as database_name,
                numbackends as active_connections,
                xact_commit as transactions_committed,
                xact_rollback as transactions_rolled_back,
                blks_read as disk_blocks_read,
                blks_hit as buffer_blocks_hit,
                CASE
                    WHEN (blks_read + blks_hit) > 0
                    THEN round(100.0 * blks_hit / (blks_read + blks_hit), 2)
                    ELSE 0
                END as cache_hit_ratio
            FROM pg_stat_database
            WHERE datname = 'ecommerce'
        """

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                db_stats = cursor.fetchone()
        finally:
            conn.close()

        return jsonify({
            'database_statistics': dict(db_stats) if db_stats else {},
            'analysis': {
                'cache_performance': 'Good' if db_stats and db_stats['cache_hit_ratio'] > 95 else 'Needs attention',
                'recommendations': [
                    "Increase shared_buffers if cache hit ratio < 95%",
                    "Monitor active connections to prevent bottlenecks"
                ]
            }
        })

    except Exception as e:
        logger.error(f"Error in connection pool stats: {e}")
        return jsonify({"error": str(e)}), 500

# Blueprint에 라우트들 등록
def register_db_tuning_routes(app):
    """Flask 앱에 DB 튜닝 라우트 등록"""
    app.register_blueprint(db_tuning_bp, url_prefix='/db-tuning')