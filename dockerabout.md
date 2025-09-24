  # 1. API 서버만 재시작 (가장 빠름)
  docker-compose restart api-server

  # 2. API 서버 정지 후 재시작
  docker-compose stop api-server
  docker-compose start api-server

  # 3. 전체 서비스 재시작 (무거움)
  docker-compose restart

  # 4. 로그 실시간 모니터링
  docker-compose logs -f api-server

  # 5. 서버 상태 확인
  docker-compose ps api-server

  # 6. 에러 발생 시 컨테이너 재빌드
  docker-compose up -d --build api-server

    1. API 서버만 재빌드 (가장 많이 사용)

  docker-compose up -d --build api-server

  2. 전체 서비스 재빌드 (무거움)

  docker-compose up -d --build

  3. 캐시 없이 완전 재빌드 (가장 확실함)

  docker-compose build --no-cache api-server
  docker-compose up -d api-server

  4. 컨테이너 정지 후 재빌드

  docker-compose down api-server
  docker-compose up -d --build api-server

  💡 추천 순서:

  1. 일반적인 경우: docker-compose up -d --build api-server
  2. 문제 지속시: docker-compose build --no-cache api-server && docker-compose up -d
  api-server
  3. 완전 초기화: docker-compose down && docker-compose up -d --build

    docker-compose up -d --build api-server