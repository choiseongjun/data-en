#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """PostgreSQL 연결 테스트"""
    pg_config = {
        'host': 'localhost',  # Docker로 실행 중이므로 localhost 사용
        'database': 'ecommerce',
        'user': 'postgres',
        'password': 'postgres',
        'cursor_factory': RealDictCursor
    }

    conn = None
    try:
        logger.info("Testing PostgreSQL connection...")
        conn = psycopg2.connect(**pg_config)
        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute('SELECT 1 as test')
            result = cursor.fetchone()
            logger.info(f"Connection test successful: {result}")

            # 테이블 존재 확인
            cursor.execute("SELECT COUNT(*) FROM orders")
            count = cursor.fetchone()
            logger.info(f"Orders table has {count['count']} records")

        logger.info("Connection test passed!")
        return True

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
    finally:
        if conn and conn.closed == 0:
            conn.close()
            logger.info("Connection closed")

if __name__ == "__main__":
    success = test_connection()
    print("SUCCESS" if success else "FAILED")