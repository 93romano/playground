# Game Database System

게임 데이터를 PostgreSQL과 Redis를 사용하여 관리하는 시스템입니다.

## 필요 조건

- Node.js (v14 이상)
- PostgreSQL
- Redis

## 설치 방법

1. 저장소 클론
```bash
git clone <repository-url>
cd redis-postgreSQL
```

2. 의존성 설치
```bash
npm install
```

3. 환경 변수 설정
`.env` 파일을 생성하고 다음 내용을 입력하세요:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=gamedb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

REDIS_HOST=localhost
REDIS_PORT=6379
```

4. PostgreSQL 데이터베이스 생성
```sql
CREATE DATABASE gamedb;
```

5. 데이터베이스 초기화
```bash
npm run init-db
```

## 실행 방법

서버 시작:
```bash
npm start
```

## API 엔드포인트

### 매치 관리
- `POST /api/matches/start` - 새 매치 시작
  ```json
  {
    "players": [
      {"userId": 1, "team": "red"},
      {"userId": 2, "team": "blue"}
    ]
  }
  ```

### 실시간 게임 데이터
- `POST /api/matches/:matchId/position` - 플레이어 위치 업데이트
  ```json
  {
    "userId": 1,
    "position": {"x": 100, "y": 200, "z": 0}
  }
  ```

- `POST /api/matches/:matchId/hit` - 히트 이벤트 기록
  ```json
  {
    "attackerId": 1,
    "victimId": 2,
    "damage": 50
  }
  ```

### 점수 및 랭킹
- `POST /api/matches/:matchId/score` - 플레이어 점수 업데이트
  ```json
  {
    "userId": 1,
    "score": 100,
    "kills": 1,
    "deaths": 0
  }
  ```

- `GET /api/rankings` - 상위 10명의 플레이어 랭킹 조회

## 데이터베이스 구조

### PostgreSQL 테이블
- `users` - 사용자 정보
- `matches` - 매치 정보
- `match_players` - 매치 참가자 정보
- `rankings` - 전체 랭킹 정보

### Redis 키
- `match:{matchId}:positions` - 실시간 플레이어 위치
- `match:{matchId}:hitEvents` - 실시간 히트 이벤트
- `ranking:total_score` - 실시간 랭킹 