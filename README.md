# 실시간 전자상거래 추천 시스템 (Real-time E-commerce Recommendation System)

## 📋 프로젝트 개요

Apache Kafka, Spark, Redis, PostgreSQL, Elasticsearch를 활용한 **Lambda Architecture 기반**의 실시간 전자상거래 추천 시스템입니다. 대용량 데이터 처리와 실시간 분석을 위한 완전한 데이터 엔지니어링 파이프라인을 제공합니다.

## 🏗️ 시스템 아키텍처

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Data Generator │ -> │    Kafka     │ -> │  Spark Streaming│
│   (실시간 생성)   │    │  (메시지큐)   │    │   (실시간 처리)   │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                                           │
         v                                           v
┌─────────────────┐                         ┌─────────────────┐
│   PostgreSQL    │    ┌──────────────┐    │      Redis      │
│  (주문/사용자)   │ -> │  ETL Service │    │   (실시간 통계)   │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                     │                     │
         v                     v                     v
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│ Elasticsearch   │    │    Kibana    │    │   API Server    │
│   (검색/분석)    │    │   (시각화)    │    │  (REST API)     │
└─────────────────┘    └──────────────┘    └─────────────────┘
```

## 🚀 핵심 기능

### 💡 실시간 데이터 생성
- **시간별 패턴**: 새벽 3시(0.1x) ~ 저녁 8시(10.0x) 피크
- **요일별 패턴**: 평일 vs 주말 차별화
- **프로모션 이벤트**: 2000% 활동 증가 (20x 배율)
- **처리량**: 평상시 초당 100개, 피크시 초당 32,000개

### 🔄 Lambda Architecture
- **Speed Layer**: Kafka → Spark → Redis (실시간)
- **Batch Layer**: PostgreSQL → ETL → Elasticsearch (배치)
- **Serving Layer**: API Server (통합 조회)

### 📊 다양한 데이터 타입
- 사용자 행동 (조회, 장바구니, 구매, 좋아요, 검색)
- 주문 및 주문 아이템
- 상품 정보 (카테고리, 브랜드)
- 실시간 통계 및 추천

## 🔧 기술 스택

- **Kafka**: 실시간 사용자 행동 데이터 스트리밍
- **Spark**: 실시간 머신러닝 추천 알고리즘 (ALS)
- **Redis**: 추천 결과 및 실시간 통계 캐싱
- **Elasticsearch**: 상품 검색 및 저장
- **Flask**: REST API 서버

## 🚀 빠른 시작

### 1. 시스템 실행

```bash
# 모든 서비스 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f
```

### 2. 서비스 접속

- **Kibana**: http://localhost:5601 (Elasticsearch 시각화)
- **Spark UI**: http://localhost:8080 (Spark 마스터 모니터링)
- **API Server**: http://localhost:5000 (추천 API)

### 3. 데이터 확인

몇 분 후 다음 명령어로 데이터 생성 확인:

```bash
# Kafka 토픽 확인
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Redis 데이터 확인
docker exec -it redis redis-cli keys "*"

# Elasticsearch 인덱스 확인
curl http://localhost:9200/_cat/indices
```

## 📊 API 사용법

### 1. 사용자별 추천 조회
```bash
curl http://localhost:5000/recommendations/user_000001
```

### 2. 트렌딩 상품 조회
```bash
curl http://localhost:5000/trending?limit=10
```

### 3. 상품 검색
```bash
# 텍스트 검색
curl "http://localhost:5000/search?q=phone"

# 카테고리 필터
curl "http://localhost:5000/search?category=Electronics"

# 가격 범위 필터
curl "http://localhost:5000/search?min_price=100&max_price=500"
```

### 4. 사용자 통계 조회
```bash
curl http://localhost:5000/user-stats/user_000001
```

### 5. 인기 상품 조회
```bash
curl http://localhost:5000/popular-products?limit=20
```

### 6. DB 튜닝 및 쿼리 최적화 (120만 건 대용량 데이터)

#### 6.1 대용량 데이터 실제 성능 튜닝 예시

##### 6.1.1 느린 쿼리 vs 최적화된 쿼리 비교
```bash
# WHERE절에서 함수 사용 vs 날짜 범위 사용 비교
curl "http://localhost:5000/db-tuning/heavy-queries"
```

##### 6.1.2 대용량 테이블 페이징 최적화
```bash
# OFFSET vs Cursor 기반 페이징 성능 비교 (깊은 페이지)
curl "http://localhost:5000/db-tuning/pagination-performance?page=10000&limit=20"
```

##### 6.1.3 대용량 데이터 집계 쿼리 최적화
```bash
# 복합 인덱스 활용한 집계 성능 개선
curl "http://localhost:5000/db-tuning/aggregation-optimization"
```

##### 6.1.4 대용량 테이블 JOIN 최적화
```bash
# 서브쿼리 사전 필터링을 통한 JOIN 성능 개선
curl "http://localhost:5000/db-tuning/join-performance"
```

#### 6.2 Full Table Scan vs Index Scan 성능 비교
```bash
curl "http://localhost:5000/db-tuning/scan-comparison?table=orders&limit=100"
```

#### 6.3 인덱스 힌트 실험
```bash
# 기본 쿼리 vs 강제 인덱스 스캔 성능 비교
curl -X POST "http://localhost:5000/db-tuning/index-hints-simple" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT order_id, status FROM orders LIMIT 50"}'
```

#### 6.4 복잡한 쿼리 최적화 실험
```bash
# JOIN 최적화 비교 (Nested Loop, Hash Join, Merge Join)
curl -X POST "http://localhost:5000/db-tuning/query-optimization" \
  -H "Content-Type: application/json" \
  -d '{"type": "join_optimization"}'

# 서브쿼리 최적화 실험
curl -X POST "http://localhost:5000/db-tuning/query-optimization" \
  -H "Content-Type: application/json" \
  -d '{"type": "subquery_optimization"}'
```

#### 6.5 쿼리 실행 계획 분석
```bash
curl -X POST "http://localhost:5000/db-tuning/query-plan" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.user_id LIMIT 10"}'
```

#### 6.6 테이블 통계 및 인덱스 분석
```bash
# 테이블 통계 조회
curl "http://localhost:5000/db-tuning/table-stats"

# 인덱스 사용률 분석
curl "http://localhost:5000/db-tuning/index-analysis"
```

## 🔍 모니터링

### Kafka 모니터링
```bash
# 토픽별 메시지 수 확인
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic user-events-all

# 실시간 메시지 확인
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events-all \
  --from-beginning
```

### Redis 모니터링
```bash
# Redis 정보 확인
docker exec -it redis redis-cli info

# 키 개수 확인
docker exec -it redis redis-cli dbsize

# 특정 패턴 키 확인
docker exec -it redis redis-cli keys "recommendations:*"
```

### Elasticsearch 모니터링
```bash
# 클러스터 상태
curl http://localhost:9200/_cluster/health

# 인덱스별 문서 수
curl http://localhost:9200/_cat/count/products

# 상품 검색 예시
curl -X GET "http://localhost:9200/products/_search" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "size": 5}'
```

## 🧪 테스트 데이터

시스템이 실행되면 자동으로 다음 데이터가 생성됩니다:

- **상품**: 1,000개의 가상 상품 (10개 카테고리)
- **사용자**: 5,000명의 가상 사용자
- **이벤트**: 초당 0.5~10개의 사용자 행동 이벤트

### 이벤트 타입별 분포
- `view`: 50% (상품 조회)
- `cart`: 20% (장바구니 추가)
- `purchase`: 10% (구매)
- `like`: 10% (좋아요)
- `search`: 10% (검색)

## 🎯 추천 알고리즘

### ALS (Alternating Least Squares)
- 사용자-상품 상호작용 행렬 기반
- 이벤트 타입별 가중치 적용:
  - `view`: 1.0
  - `cart`: 3.0
  - `like`: 4.0
  - `purchase`: 5.0

### 실시간 처리
- 10분 윈도우, 5분 슬라이딩
- 5분마다 새로운 추천 생성
- Redis에 1시간 캐싱

## 🔧 Spark 작업 실행

### 추천 엔진 수동 실행
```bash
docker exec -it spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/recommendation_engine.py
```

## 📈 성능 튜닝

### Kafka 설정
- `num.partitions`: 토픽별 파티션 수 조정
- `replica.factor`: 복제 수 설정

### Spark 설정
- Worker 메모리: 기본 1GB (docker-compose.yml에서 조정)
- 배치 간격: 5분 (recommendation_engine.py에서 조정)

### Redis 설정
- TTL: 추천 결과 1시간, 통계 5분
- 메모리 최적화: Redis 설정 조정

## 🛠️ 문제 해결

### 일반적인 문제

1. **Kafka 연결 실패**
   ```bash
   # Kafka 상태 확인
   docker-compose ps kafka
   docker-compose logs kafka
   ```

2. **Spark 작업 실패**
   ```bash
   # Spark 로그 확인
   docker-compose logs spark-master
   docker-compose logs spark-worker
   ```

3. **추천 결과 없음**
   ```bash
   # 데이터 생성 확인
   docker-compose logs data-generator

   # Redis 데이터 확인
   docker exec -it redis redis-cli keys "recommendations:*"
   ```

### 시스템 재시작
```bash
# 전체 시스템 재시작
docker-compose down
docker-compose up -d

# 특정 서비스만 재시작
docker-compose restart data-generator
docker-compose restart api-server
```

## 📝 로그 레벨 조정

각 서비스의 로그 레벨을 조정하려면:

- **Data Generator**: `data-generator/data_generator.py`의 logging 설정
- **API Server**: `api-server/app.py`의 logging 설정
- **Spark**: `recommendation_engine.py`의 logging 설정

## 🔄 확장 가능성

이 시스템은 다음과 같이 확장할 수 있습니다:

1. **다중 Kafka 파티션**: 처리량 증가
2. **Spark 워커 추가**: 병렬 처리 향상
3. **Redis 클러스터**: 고가용성
4. **Elasticsearch 클러스터**: 검색 성능 향상
5. **추가 ML 모델**: 다양한 추천 알고리즘
