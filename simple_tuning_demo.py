#!/usr/bin/env python3
"""
간단한 DB 튜닝 데모 - 120만 건 orders 테이블 성능 비교
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import json

def connect_db():
    return psycopg2.connect(
        host='localhost',
        database='ecommerce',
        user='postgres',
        password='postgres',
        cursor_factory=RealDictCursor
    )

def demo_slow_vs_fast_query():
    """느린 쿼리 vs 빠른 쿼리 비교"""
    print("=== 대용량 DB 튜닝 데모: 느린 쿼리 vs 빠른 쿼리 ===\n")

    conn = connect_db()

    try:
        with conn.cursor() as cursor:
            # 테이블 크기 확인
            cursor.execute("SELECT COUNT(*) FROM orders")
            total_orders = cursor.fetchone()['count']
            print(f"총 주문 데이터: {total_orders:,}건\n")

            # 1. 느린 쿼리: WHERE 절에서 함수 사용
            print("1. 📉 느린 쿼리 (WHERE절에서 EXTRACT 함수 사용)")
            print("   SELECT COUNT(*), AVG(total_amount) FROM orders")
            print("   WHERE EXTRACT(YEAR FROM order_date) = 2023")
            print("   AND EXTRACT(MONTH FROM order_date) = 6")

            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(*) as count, AVG(total_amount) as avg_amount
                FROM orders
                WHERE EXTRACT(YEAR FROM order_date) = 2023
                AND EXTRACT(MONTH FROM order_date) = 6
            """)
            slow_result = cursor.fetchone()
            slow_time = time.time() - start_time

            print(f"   ⏱️  실행시간: {slow_time:.3f}초")
            print(f"   📊 결과: {slow_result['count']}건, 평균금액: {slow_result['avg_amount']:.2f}원\n")

            # 2. 빠른 쿼리: 날짜 범위 사용 (인덱스 활용 가능)
            print("2. 🚀 빠른 쿼리 (날짜 범위로 인덱스 활용)")
            print("   SELECT COUNT(*), AVG(total_amount) FROM orders")
            print("   WHERE order_date >= '2023-06-01' AND order_date < '2023-07-01'")

            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(*) as count, AVG(total_amount) as avg_amount
                FROM orders
                WHERE order_date >= '2023-06-01'
                AND order_date < '2023-07-01'
            """)
            fast_result = cursor.fetchone()
            fast_time = time.time() - start_time

            print(f"   ⏱️  실행시간: {fast_time:.3f}초")
            print(f"   📊 결과: {fast_result['count']}건, 평균금액: {fast_result['avg_amount']:.2f}원\n")

            # 성능 비교
            speedup = slow_time / fast_time if fast_time > 0 else 0
            print(f"🎯 성능 개선: {speedup:.1f}배 빨라짐!")
            print(f"💡 핵심: WHERE 절에서 함수 사용을 피하고 인덱스를 활용하세요.")

    finally:
        conn.close()

def demo_pagination_performance():
    """페이징 성능 비교"""
    print("\n=== 대용량 데이터 페이징 성능 비교 ===\n")

    conn = connect_db()

    try:
        with conn.cursor() as cursor:
            page = 5000  # 깊은 페이지
            limit = 20
            offset = (page - 1) * limit

            # 1. 나쁜 방법: OFFSET 사용
            print(f"1. 📉 OFFSET 방식 (페이지 {page})")
            print(f"   SELECT * FROM orders ORDER BY order_id LIMIT {limit} OFFSET {offset}")

            start_time = time.time()
            cursor.execute("SELECT * FROM orders ORDER BY order_id LIMIT %s OFFSET %s", [limit, offset])
            offset_results = cursor.fetchall()
            offset_time = time.time() - start_time

            print(f"   ⏱️  실행시간: {offset_time:.3f}초")
            print(f"   📊 조회된 행: {len(offset_results)}개\n")

            # 2. 좋은 방법: Cursor 기반
            if offset_results:
                # 이전 페이지의 마지막 ID 기준
                cursor.execute("SELECT order_id FROM orders ORDER BY order_id LIMIT 1 OFFSET %s", [offset-1])
                last_id = cursor.fetchone()['order_id']

                print(f"2. 🚀 Cursor 방식 (order_id > {last_id})")
                print(f"   SELECT * FROM orders WHERE order_id > {last_id} ORDER BY order_id LIMIT {limit}")

                start_time = time.time()
                cursor.execute("SELECT * FROM orders WHERE order_id > %s ORDER BY order_id LIMIT %s", [last_id, limit])
                cursor_results = cursor.fetchall()
                cursor_time = time.time() - start_time

                print(f"   ⏱️  실행시간: {cursor_time:.3f}초")
                print(f"   📊 조회된 행: {len(cursor_results)}개\n")

                # 성능 비교
                speedup = offset_time / cursor_time if cursor_time > 0 else 0
                print(f"🎯 성능 개선: {speedup:.1f}배 빨라짐!")
                print(f"💡 핵심: 깊은 페이징에는 OFFSET 대신 WHERE > last_id를 사용하세요.")

    finally:
        conn.close()

if __name__ == "__main__":
    try:
        demo_slow_vs_fast_query()
        demo_pagination_performance()
        print("\n✨ 데모 완료! 대용량 데이터에서는 쿼리 최적화가 매우 중요합니다.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        print("PostgreSQL 서버가 실행 중인지 확인하세요.")