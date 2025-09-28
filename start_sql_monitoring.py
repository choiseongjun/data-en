#!/usr/bin/env python3
"""
간단한 SQL 모니터링 로드 제너레이터
초마다 API를 호출하여 실시간 SQL 로그를 생성합니다.
"""

import requests
import time
import random
from datetime import datetime

def generate_sql_load():
    """초마다 API 호출하여 SQL 로그 생성"""

    # 다양한 API 엔드포인트
    endpoints = [
        "/test-sql-logging",
        "/health",
        "/popular-products?limit=10",
        "/popular-products?limit=20",
        "/recommendations/user_000001",
        "/recommendations/user_000002",
        "/user-stats/user_000001"
    ]

    print("🚀 SQL 실시간 모니터링 시작!")
    print("📊 Kibana에서 실시간 대시보드를 확인하세요: http://localhost:5601")
    print("🛑 Ctrl+C로 중지")
    print("-" * 60)

    request_count = 0

    try:
        while True:
            # 랜덤하게 API 선택
            endpoint = random.choice(endpoints)

            try:
                url = f"http://localhost:5000{endpoint}"
                response = requests.get(url, timeout=5)

                status = "✅" if response.status_code == 200 else "❌"
                timestamp = datetime.now().strftime("%H:%M:%S")
                request_count += 1

                print(f"[{timestamp}] {status} #{request_count:03d} {endpoint} ({response.status_code})")

            except Exception as e:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] ❌ #{request_count:03d} {endpoint} - Error: {str(e)}")

            # 1초 대기
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n🛑 모니터링 중지됨. 총 {request_count}개 요청 생성")

if __name__ == "__main__":
    generate_sql_load()