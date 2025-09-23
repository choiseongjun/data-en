#!/bin/bash

echo "🚀 E-commerce 실시간 추천 시스템 시작"
echo "=================================="

# 기존 컨테이너 정리
echo "🧹 기존 컨테이너 정리 중..."
docker-compose down -v

# 이미지 빌드 및 컨테이너 시작
echo "🔨 Docker 이미지 빌드 및 컨테이너 시작..."
docker-compose up -d --build

# 서비스 시작 대기
echo "⏰ 서비스 초기화 대기 중..."
sleep 30

echo "✅ 시스템 상태 확인"
echo "==================="

# Kafka 상태 확인
echo "📊 Kafka 상태:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "❌ Kafka 연결 실패"

# Elasticsearch 상태 확인
echo "🔍 Elasticsearch 상태:"
curl -s http://localhost:9200/_cluster/health | jq .status 2>/dev/null || echo "❌ Elasticsearch 연결 실패"

# Redis 상태 확인
echo "💾 Redis 상태:"
docker exec redis redis-cli ping 2>/dev/null || echo "❌ Redis 연결 실패"

# API 서버 상태 확인
echo "🌐 API 서버 상태:"
curl -s http://localhost:5000/health | jq .status 2>/dev/null || echo "❌ API 서버 연결 실패"

echo ""
echo "🎯 접속 정보"
echo "============"
echo "📊 Kibana (Elasticsearch 시각화): http://localhost:5601"
echo "⚡ Spark UI (작업 모니터링): http://localhost:8080"
echo "🌐 API Server (추천 API): http://localhost:5000"
echo ""
echo "📋 주요 API 엔드포인트:"
echo "  • 사용자 추천: http://localhost:5000/recommendations/user_000001"
echo "  • 트렌딩 상품: http://localhost:5000/trending"
echo "  • 상품 검색: http://localhost:5000/search?q=phone"
echo "  • 사용자 통계: http://localhost:5000/user-stats/user_000001"
echo ""
echo "📝 실시간 로그 확인:"
echo "  docker-compose logs -f"
echo ""
echo "🛑 시스템 종료:"
echo "  docker-compose down"