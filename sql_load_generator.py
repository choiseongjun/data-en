#!/usr/bin/env python3
"""
SQL 로드 제너레이터 - 초마다 다양한 API를 호출하여 SQL 로그 생성
"""

import requests
import time
import random
import threading
from datetime import datetime

API_BASE_URL = "http://localhost:5000"

# 다양한 API 엔드포인트들
API_ENDPOINTS = [
    "/test-sql-logging",
    "/health",
    "/recommendations/user_000001",
    "/recommendations/user_000002",
    "/recommendations/user_000003",
    "/user-stats/user_000001",
    "/user-stats/user_000002",
    "/popular-products?limit=10",
    "/popular-products?limit=20",
    "/popular-products?limit=5"
]

# DB 튜닝 API들 (더 복잡한 쿼리들)
DB_TUNING_ENDPOINTS = [
    "/db-tuning/heavy-queries",
    "/db-tuning/scan-comparison?table=orders&limit=50",
    "/db-tuning/scan-comparison?table=orders&limit=100",
    "/db-tuning/aggregation-optimization",
    "/db-tuning/join-performance",
    "/db-tuning/pagination-performance?page=1&limit=20",
    "/db-tuning/pagination-performance?page=10&limit=10"
]

# 복잡한 분석 API들 (시간이 오래 걸리는 쿼리들)
COMPLEX_ANALYTICS_ENDPOINTS = [
    "/analytics/complex-order-analysis",
    "/analytics/heavy-aggregation",
    "/analytics/recursive-category-tree",
    "/analytics/customer-cohort-analysis",
    "/analytics/full-table-scan-test"
]

class SQLLoadGenerator:
    def __init__(self):
        self.running = False
        self.request_count = 0
        self.error_count = 0

    def make_request(self, endpoint):
        """단일 API 요청"""
        try:
            url = f"{API_BASE_URL}{endpoint}"
            response = requests.get(url, timeout=10)

            status = "✅" if response.status_code == 200 else "❌"
            timestamp = datetime.now().strftime("%H:%M:%S")

            print(f"[{timestamp}] {status} {endpoint} ({response.status_code}) - {len(response.text)} bytes")

            self.request_count += 1
            if response.status_code != 200:
                self.error_count += 1

        except Exception as e:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] ❌ {endpoint} - Error: {str(e)}")
            self.error_count += 1

    def generate_load(self, interval=1.0):
        """지속적으로 로드 생성"""
        print(f"🚀 SQL Load Generator started - {interval}초 간격")
        print("📊 실시간 SQL 로그가 Kibana로 전송됩니다")
        print("🛑 Ctrl+C로 중지")
        print("-" * 60)

        self.running = True

        try:
            while self.running:
                # 60% 확률로 일반 API, 25% 확률로 DB 튜닝 API, 15% 확률로 복잡한 분석 API
                rand = random.random()
                if rand < 0.6:
                    endpoint = random.choice(API_ENDPOINTS)
                elif rand < 0.85:
                    endpoint = random.choice(DB_TUNING_ENDPOINTS)
                else:
                    endpoint = random.choice(COMPLEX_ANALYTICS_ENDPOINTS)

                # 백그라운드에서 요청 실행 (블로킹 방지)
                thread = threading.Thread(target=self.make_request, args=(endpoint,))
                thread.daemon = True
                thread.start()

                time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n🛑 Load Generator 중지됨")
            print(f"📈 총 요청: {self.request_count}, 에러: {self.error_count}")
            self.running = False

    def burst_load(self, count=10):
        """버스트 로드 (한번에 여러 요청)"""
        print(f"💥 Burst Load: {count}개 요청을 동시에 발생")

        threads = []
        all_endpoints = API_ENDPOINTS + DB_TUNING_ENDPOINTS + COMPLEX_ANALYTICS_ENDPOINTS
        for i in range(count):
            endpoint = random.choice(all_endpoints)
            thread = threading.Thread(target=self.make_request, args=(endpoint,))
            thread.daemon = True
            threads.append(thread)

        # 모든 스레드 시작
        for thread in threads:
            thread.start()
            time.sleep(0.1)  # 약간의 지연으로 순차 시작

        # 모든 스레드 완료 대기
        for thread in threads:
            thread.join()

        print(f"✅ Burst Load 완료: {count}개 요청")

def main():
    generator = SQLLoadGenerator()

    print("🎯 SQL Load Generator")
    print("=" * 50)
    print("1. 연속 로드 (기본: 1초 간격)")
    print("2. 연속 로드 (빠른: 0.5초 간격)")
    print("3. 연속 로드 (매우 빠른: 0.2초 간격)")
    print("4. 버스트 로드 (한번에 10개)")
    print("5. 대량 버스트 (한번에 50개)")

    choice = input("\n선택하세요 (1-5, 또는 Enter로 기본값): ").strip()

    if choice == "2":
        generator.generate_load(0.5)
    elif choice == "3":
        generator.generate_load(0.2)
    elif choice == "4":
        generator.burst_load(10)
    elif choice == "5":
        generator.burst_load(50)
    else:
        generator.generate_load(1.0)

if __name__ == "__main__":
    main()