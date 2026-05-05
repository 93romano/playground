# Redis 실습 가이드

## 1. 설치 및 설정

### Mac
```bash
brew install redis
brew services start redis
```

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install redis-server
sudo systemctl start redis
```

### Docker
```bash
docker run -d -p 6379:6379 --name redis-practice redis:latest
```

## 2. 기본 명령어

### Redis CLI 접속
```bash
redis-cli
```

### 주요 명령어
- `PING` : 연결 테스트
- `INFO` : 서버 정보
- `KEYS *` : 모든 키 조회
- `FLUSHDB` : 현재 DB 모든 키 삭제
- `FLUSHALL` : 모든 DB 키 삭제
- `QUIT` : 종료

## 3. 데이터 타입별 실습

### String (문자열)
```bash
# 설정
SET name "홍길동"
SET counter 0
SET user:1:name "김철수"

# 조회
GET name
GET counter

# 증가/감소
INCR counter
DECR counter
INCRBY counter 5

# 만료 시간 설정
SETEX session:abc123 3600 "user_data"
TTL session:abc123

# 여러 개 한번에
MSET key1 "value1" key2 "value2" key3 "value3"
MGET key1 key2 key3
```

### List (리스트)
```bash
# 추가
LPUSH tasks "작업1"
LPUSH tasks "작업2"
RPUSH tasks "작업3"

# 조회
LRANGE tasks 0 -1
LLEN tasks
LINDEX tasks 0

# 제거
LPOP tasks
RPOP tasks
LREM tasks 1 "작업2"

# 실시간 로그
LPUSH logs "2024-01-20 10:00:00 - 사용자 로그인"
LPUSH logs "2024-01-20 10:00:05 - 페이지 조회"
LTRIM logs 0 99  # 최근 100개만 유지
```

### Set (집합)
```bash
# 추가
SADD languages "JavaScript"
SADD languages "Python" "Java" "Go"

# 조회
SMEMBERS languages
SISMEMBER languages "Python"
SCARD languages

# 집합 연산
SADD frontend "JavaScript" "TypeScript" "CSS"
SADD backend "Python" "Java" "Go" "JavaScript"

SINTER frontend backend  # 교집합
SUNION frontend backend  # 합집합
SDIFF backend frontend   # 차집합

# 랜덤 선택
SRANDMEMBER languages
SPOP languages  # 랜덤 제거
```

### Hash (해시)
```bash
# 사용자 정보 저장
HSET user:1001 name "이영희"
HSET user:1001 email "lee@example.com"
HSET user:1001 age 28

# 여러 필드 한번에
HMSET user:1002 name "박민수" email "park@example.com" age 35

# 조회
HGET user:1001 name
HMGET user:1001 name email
HGETALL user:1001

# 필드 관리
HEXISTS user:1001 phone
HDEL user:1001 age
HKEYS user:1001
HVALS user:1001

# 숫자 증가
HINCRBY user:1001 login_count 1
```

### Sorted Set (정렬 집합)
```bash
# 리더보드/순위표
ZADD leaderboard 100 "player1"
ZADD leaderboard 150 "player2"
ZADD leaderboard 120 "player3"

# 순위 조회
ZRANGE leaderboard 0 -1  # 오름차순
ZREVRANGE leaderboard 0 -1  # 내림차순
ZREVRANGE leaderboard 0 2 WITHSCORES  # 상위 3명 점수와 함께

# 특정 멤버 정보
ZSCORE leaderboard "player1"
ZRANK leaderboard "player1"  # 순위 (0부터 시작)
ZREVRANK leaderboard "player1"  # 역순위

# 점수 증가
ZINCRBY leaderboard 30 "player1"

# 범위 조회
ZRANGEBYSCORE leaderboard 100 200
ZCOUNT leaderboard 100 200
```

## 4. 실전 예제

### 캐싱 패턴
```bash
# 1. Cache-Aside 패턴
# 키가 없으면 DB 조회 후 캐시 저장
GET product:123
# 없으면 DB에서 조회 후
SET product:123 "{\"name\":\"노트북\",\"price\":1500000}" EX 3600

# 2. Write-Through 패턴
# 데이터 변경시 캐시도 함께 업데이트
SET product:123 "{\"name\":\"노트북\",\"price\":1400000}"
# DB도 업데이트
```

### 세션 관리
```bash
# 세션 생성
SETEX session:uuid123 1800 "{\"user_id\":1001,\"username\":\"홍길동\"}"

# 세션 연장
EXPIRE session:uuid123 1800

# 세션 조회
GET session:uuid123
TTL session:uuid123

# 세션 삭제
DEL session:uuid123
```

### Rate Limiting
```bash
# API 호출 제한 (분당 10회)
SET api:user:1001:minute:45 0 EX 60 NX
INCR api:user:1001:minute:45

# 확인
GET api:user:1001:minute:45
# 10 초과시 요청 거부
```

### 실시간 알림 큐
```bash
# 알림 추가
LPUSH notifications:user:1001 "{\"type\":\"message\",\"content\":\"새 메시지\"}"

# 알림 조회 (블로킹)
BRPOP notifications:user:1001 30  # 30초 대기

# 미확인 알림 수
LLEN notifications:user:1001
```

### 분산 락
```bash
# 락 획득 (5초 만료)
SET lock:resource:xyz "owner123" NX EX 5

# 락 해제
DEL lock:resource:xyz

# 락 확인
GET lock:resource:xyz
```

## 5. Pub/Sub (발행-구독)

### 구독자 (터미널 1)
```bash
SUBSCRIBE news sports
# 또는 패턴 구독
PSUBSCRIBE news:*
```

### 발행자 (터미널 2)
```bash
PUBLISH news "속보: Redis 6.0 출시"
PUBLISH sports "축구 경기 결과"

# 구독자 수 확인
PUBSUB CHANNELS
PUBSUB NUMSUB news
```

## 6. 트랜잭션

```bash
# 트랜잭션 시작
MULTI

# 명령어 큐잉
SET account:1 1000
SET account:2 2000
DECRBY account:1 100
INCRBY account:2 100

# 실행
EXEC

# 또는 취소
DISCARD
```

### Watch (낙관적 락)
```bash
WATCH balance
GET balance
# balance가 변경되지 않았다면 실행
MULTI
SET balance 900
EXEC
```

## 7. 스크립트 (Lua)

```bash
# 간단한 스크립트
EVAL "return redis.call('GET', KEYS[1])" 1 mykey

# 복잡한 로직
EVAL "
  local current = redis.call('GET', KEYS[1])
  if not current then
    current = 0
  end
  current = current + ARGV[1]
  redis.call('SET', KEYS[1], current)
  return current
" 1 counter 5
```

## 8. 파이프라인

```bash
# Node.js에서 파이프라인 사용 예
# 여러 명령을 한번에 전송하여 성능 향상
```

## 9. 데이터 영속성

### RDB (스냅샷)
```bash
# 즉시 스냅샷
BGSAVE

# 마지막 저장 시간
LASTSAVE
```

### AOF (Append Only File)
```bash
# AOF 재작성
BGREWRITEAOF
```

## 10. 모니터링

```bash
# 실시간 명령 모니터링
MONITOR

# 메모리 정보
INFO memory

# 통계
INFO stats

# 슬로우 로그
SLOWLOG GET 10

# 설정 확인
CONFIG GET *
```

## 11. 클러스터 관련

```bash
# 클러스터 정보
CLUSTER INFO
CLUSTER NODES

# 슬롯 정보
CLUSTER SLOTS
```

## 12. 성능 최적화 팁

1. **적절한 만료 시간 설정**
   - 메모리 관리를 위해 TTL 설정

2. **파이프라인 사용**
   - 여러 명령 한번에 전송

3. **적절한 데이터 구조 선택**
   - 용도에 맞는 데이터 타입 사용

4. **키 네이밍 컨벤션**
   - `object:id:field` 형식 권장

5. **메모리 정책**
   - maxmemory-policy 설정 (LRU, LFU 등)