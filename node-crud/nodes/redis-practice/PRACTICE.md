# Redis 실습 시트

## 목차
1. [개요](#개요)
2. [환경 설정](#환경-설정)
3. [데이터 타입](#데이터-타입)
4. [실용 패턴](#실용-패턴)
5. [Node.js 연동](#nodejs-연동)
6. [실습 과제](#실습-과제)
7. [테스트 코드 설명](#테스트-코드-설명)

---

## 개요

Redis는 **인메모리 키-값 저장소**로, 데이터를 메모리에 저장하기 때문에 매우 빠르다 (읽기/쓰기 < 1ms).

### 특징
- **초고속**: 모든 데이터가 메모리에 있어 디스크 I/O 없음
- **다양한 자료구조**: String, List, Set, Hash, Sorted Set 등
- **영속성 옵션**: RDB 스냅샷, AOF 로그로 데이터 보존 가능
- **Pub/Sub**: 메시지 브로커 기능
- **단일 스레드**: 원자적 연산 보장 (race condition 없음)

### 언제 쓰나?
| 용도 | 설명 |
|------|------|
| 캐싱 | DB 쿼리 결과를 캐시해서 응답 속도 향상 |
| 세션 저장소 | 로그인 세션을 서버 메모리 대신 Redis에 저장 |
| Rate Limiting | API 호출 횟수 제한 |
| 랭킹/리더보드 | Sorted Set으로 실시간 순위 관리 |
| 실시간 알림 | Pub/Sub으로 메시지 전달 |
| 큐 | List로 작업 큐 구현 |
| 분산 락 | 여러 서버간 동기화 |

---

## 환경 설정

### 1. Redis 설치 (macOS)
```bash
brew install redis
brew services start redis
```

### 2. 접속 확인
```bash
redis-cli ping
# PONG 이 나오면 성공
```

### 3. Redis CLI 기본 명령
```bash
redis-cli

# 기본 조작
SET name "홍길동"
GET name
DEL name
EXISTS name

# 키 목록
KEYS *
KEYS user:*

# 전체 삭제 (주의!)
FLUSHDB
```

### 4. Node.js 패키지 설치
```bash
cd redis-practice
npm install
```

---

## 데이터 타입

### 1. String - 가장 기본
```bash
# 설정/조회
SET key value
GET key

# 숫자 증감 (원자적)
SET counter 0
INCR counter        # 1
INCR counter        # 2
DECR counter        # 1
INCRBY counter 10   # 11

# 만료 시간
SET temp "임시" EX 60    # 60초 후 자동 삭제
TTL temp                  # 남은 시간 확인
PERSIST temp             # 만료 취소

# 조건부 설정
SETNX key value          # 키가 없을 때만 설정 (분산 락에 사용)
```

**활용**: 캐시, 카운터, 세션, 분산 락

### 2. List - 순서가 있는 목록
```bash
# 추가
LPUSH tasks "작업1"    # 왼쪽(앞)에 추가
RPUSH tasks "작업2"    # 오른쪽(뒤)에 추가

# 조회
LRANGE tasks 0 -1      # 전체 조회
LRANGE tasks 0 2       # 0~2번째 조회
LLEN tasks             # 길이

# 제거
LPOP tasks             # 왼쪽에서 제거 후 반환
RPOP tasks             # 오른쪽에서 제거 후 반환
BLPOP tasks 30         # 블로킹 팝 (30초 대기)
```

**활용**: 메시지 큐, 최근 활동 내역, 타임라인

### 3. Set - 중복 없는 집합
```bash
# 추가
SADD languages "JavaScript"
SADD languages "Python"
SADD languages "JavaScript"   # 중복 무시

# 조회
SMEMBERS languages             # 전체 멤버
SISMEMBER languages "Python"   # 멤버 확인 (0 or 1)
SCARD languages                # 멤버 수

# 집합 연산
SADD frontend "JavaScript" "TypeScript" "CSS"
SADD backend "JavaScript" "Python" "Go"

SINTER frontend backend        # 교집합: JavaScript
SUNION frontend backend        # 합집합: 전체
SDIFF frontend backend         # 차집합: TypeScript, CSS
```

**활용**: 태그, 고유 방문자, 좋아요 사용자 목록

### 4. Hash - 필드-값 쌍 (객체)
```bash
# 설정
HSET user:1001 name "홍길동"
HSET user:1001 email "hong@example.com"
HSET user:1001 age 25

# 또는 한번에
HMSET user:1001 name "홍길동" email "hong@example.com" age 25

# 조회
HGET user:1001 name            # 특정 필드
HGETALL user:1001              # 전체 필드
HMGET user:1001 name email     # 여러 필드

# 수정
HINCRBY user:1001 age 1        # 숫자 필드 증가

# 삭제
HDEL user:1001 email           # 필드 삭제
```

**활용**: 사용자 프로필, 상품 정보, 설정값

### 5. Sorted Set - 점수가 있는 정렬 집합
```bash
# 추가 (점수, 멤버)
ZADD leaderboard 100 "player1"
ZADD leaderboard 200 "player2"
ZADD leaderboard 150 "player3"

# 조회 (오름차순)
ZRANGE leaderboard 0 -1                   # 전체
ZRANGE leaderboard 0 -1 WITHSCORES       # 점수 포함

# 조회 (내림차순)
ZREVRANGE leaderboard 0 2 WITHSCORES     # 상위 3명

# 점수 범위 조회
ZRANGEBYSCORE leaderboard 100 200

# 순위 조회
ZRANK leaderboard "player2"               # 오름차순 순위
ZREVRANK leaderboard "player2"            # 내림차순 순위

# 점수 증가
ZINCRBY leaderboard 50 "player1"          # player1에 50점 추가
```

**활용**: 랭킹, 우선순위 큐, 시간순 정렬 데이터

---

## 실용 패턴

### 캐싱 패턴 (Cache-Aside)
```
1. 클라이언트 요청
2. Redis에서 캐시 확인
3. 캐시 히트 → 바로 반환
4. 캐시 미스 → DB 조회 → Redis에 저장 → 반환
```

```javascript
async function getProduct(id) {
    const cacheKey = `product:${id}`;

    // 캐시 확인
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    // DB 조회
    const product = await db.query('SELECT * FROM products WHERE id = $1', [id]);

    // 캐시 저장 (1시간)
    await redis.set(cacheKey, JSON.stringify(product), 'EX', 3600);

    return product;
}
```

### 세션 관리
```javascript
// 세션 생성
const sessionId = crypto.randomUUID();
await redis.set(`session:${sessionId}`, JSON.stringify({
    userId: 1001,
    username: '홍길동'
}), 'EX', 1800); // 30분

// 세션 조회
const session = JSON.parse(await redis.get(`session:${sessionId}`));

// 세션 연장
await redis.expire(`session:${sessionId}`, 1800);

// 세션 삭제 (로그아웃)
await redis.del(`session:${sessionId}`);
```

### Rate Limiting
```javascript
async function isRateLimited(userId, maxRequests = 100, windowSec = 60) {
    const key = `rate:${userId}`;
    const current = await redis.incr(key);

    if (current === 1) {
        await redis.expire(key, windowSec);
    }

    return current > maxRequests;
}
```

### 분산 락 (Distributed Lock)
```javascript
async function acquireLock(resource, ttl = 5000) {
    const lockKey = `lock:${resource}`;
    const lockValue = crypto.randomUUID();

    // NX: 키가 없을 때만 설정, PX: 밀리초 만료
    const acquired = await redis.set(lockKey, lockValue, 'NX', 'PX', ttl);

    return acquired ? lockValue : null;
}

async function releaseLock(resource, lockValue) {
    const lockKey = `lock:${resource}`;
    const current = await redis.get(lockKey);

    if (current === lockValue) {
        await redis.del(lockKey);
    }
}
```

---

## Node.js 연동

### redis 패키지 (v3 - 콜백 기반)
```javascript
const redis = require('redis');
const { promisify } = require('util');

const client = redis.createClient({ host: 'localhost', port: 6379 });

// Promise로 변환
const getAsync = promisify(client.get).bind(client);
const setAsync = promisify(client.set).bind(client);

await setAsync('key', 'value');
const value = await getAsync('key');
```

### ioredis 패키지 (Promise 기본 지원, 실무 추천)
```javascript
const Redis = require('ioredis');

const redis = new Redis({
    host: 'localhost',
    port: 6379,
    // password: 'your_password',
    retryStrategy(times) {
        return Math.min(times * 50, 2000);
    }
});

// 바로 async/await 사용 가능
await redis.set('key', 'value');
const value = await redis.get('key');

// 파이프라인 (여러 명령을 한번에 전송)
const pipeline = redis.pipeline();
pipeline.set('a', '1');
pipeline.set('b', '2');
pipeline.get('a');
pipeline.get('b');
const results = await pipeline.exec();
```

---

## 실습 과제

### 과제 1: 기본 테스트 실행
```bash
# Redis가 실행 중인지 확인
redis-cli ping

# 테스트 실행
npm test
```

### 과제 2: 쇼핑 카트 구현
Redis Hash를 사용해서 장바구니를 구현해보기:
- `cart:{userId}` 키에 상품ID: 수량 형태로 저장
- 상품 추가, 수량 변경, 상품 제거, 전체 조회 기능

```javascript
// 힌트
await redis.hset('cart:1001', 'product:1', 2);  // 상품1 2개
await redis.hincrby('cart:1001', 'product:1', 1); // 1개 추가
await redis.hgetall('cart:1001');                 // 전체 조회
await redis.hdel('cart:1001', 'product:1');       // 상품 제거
```

### 과제 3: 실시간 채팅 구현
Pub/Sub을 사용해서 간단한 채팅 시스템을 만들어보기:
- 채널 구독/해제
- 메시지 발행
- 최근 메시지 100개를 List에 저장

### 과제 4: 게임 랭킹 시스템
Sorted Set을 사용해서 구현:
- 점수 등록/업데이트
- 전체 랭킹 조회
- 특정 유저의 순위 조회
- 상위 N명 조회

### 과제 5: 캐시 무효화 전략
아래 전략 중 하나를 선택해서 직접 구현해보기:
1. **TTL 기반**: 일정 시간 후 자동 만료
2. **Write-Through**: 데이터 변경 시 캐시도 같이 업데이트
3. **Cache Invalidation**: 데이터 변경 시 캐시 삭제

---

## 테스트 코드 설명

### test.js 구조

| 테스트 함수 | 설명 | 핵심 개념 |
|------------|------|----------|
| `testString` | String 타입 조작 | SET, GET, INCR, EXPIRE, TTL |
| `testList` | List 타입 조작 | LPUSH, RPUSH, LRANGE, LPOP, RPOP |
| `testSet` | Set 타입 조작 | SADD, SMEMBERS, SISMEMBER |
| `testHash` | Hash 타입 조작 | HSET, HGET, HGETALL |
| `testSortedSet` | Sorted Set 조작 | ZADD, ZRANGE, ZREVRANGE |
| `testCaching` | 캐싱 패턴 | 캐시 미스 → 저장 → 캐시 히트 |
| `testSession` | 세션 관리 | 생성, 조회, 연장, 삭제 |
| `testRateLimiting` | 호출 제한 | INCR + EXPIRE 조합 |
| `testTransaction` | 트랜잭션 (MULTI) | MULTI, EXEC (원자적 실행) |
| `testPubSub` | 메시지 브로커 | SUBSCRIBE, PUBLISH |

### 실행 방법
```bash
# Redis 서비스가 실행 중이어야 합니다
npm test
```

### 주의사항
- Redis 서비스가 실행 중이어야 합니다 (`redis-cli ping`으로 확인)
- 테스트 종료 시 `FLUSHDB`로 테스트 데이터가 정리됩니다
- Pub/Sub 테스트는 별도의 클라이언트 연결이 필요합니다
- 현재 test.js는 redis v3 (콜백 기반)을 사용합니다. 실무에서는 ioredis를 추천합니다
