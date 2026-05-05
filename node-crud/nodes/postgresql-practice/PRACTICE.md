# PostgreSQL 실습 시트

## 목차
1. [개요](#개요)
2. [환경 설정](#환경-설정)
3. [기본 개념](#기본-개념)
4. [SQL 기초](#sql-기초)
5. [Node.js 연동](#nodejs-연동)
6. [실습 과제](#실습-과제)
7. [테스트 코드 설명](#테스트-코드-설명)

---

## 개요

PostgreSQL은 **오픈소스 관계형 데이터베이스(RDBMS)**로, 데이터 무결성과 확장성이 뛰어나다.

### MySQL과의 차이
| 항목 | PostgreSQL | MySQL |
|------|-----------|-------|
| 타입 시스템 | 엄격함 (사용자 정의 타입 지원) | 유연함 |
| JSON 지원 | JSONB (인덱싱 가능) | JSON (인덱싱 제한적) |
| 동시성 | MVCC 기반 | 락 기반 |
| 트랜잭션 | 완전한 ACID | 스토리지 엔진에 따라 다름 |
| 풀텍스트 검색 | 내장 지원 | 내장 지원 (다른 방식) |

---

## 환경 설정

### 1. PostgreSQL 설치 (macOS)
```bash
brew install postgresql@16
brew services start postgresql@16
```

### 2. 데이터베이스 생성
```bash
# PostgreSQL 접속
psql postgres

# 데이터베이스 생성
CREATE DATABASE practice_db;

# 확인
\l

# 데이터베이스 접속
\c practice_db
```

### 3. Node.js 패키지 설치
```bash
cd postgresql-practice
npm install
```

---

## 기본 개념

### 데이터 타입
```sql
-- 숫자
INTEGER          -- 정수 (-2147483648 ~ 2147483647)
BIGINT           -- 큰 정수
SERIAL           -- 자동 증가 정수 (AUTO_INCREMENT)
DECIMAL(10, 2)   -- 정밀 소수점 (총 10자리, 소수점 2자리)
REAL             -- 부동소수점

-- 문자열
VARCHAR(100)     -- 가변 길이 문자열 (최대 100자)
TEXT             -- 길이 제한 없는 문자열
CHAR(10)         -- 고정 길이 문자열

-- 날짜/시간
DATE             -- 날짜 (2024-01-15)
TIMESTAMP        -- 날짜+시간 (2024-01-15 09:30:00)
TIMESTAMPTZ      -- 시간대 포함 타임스탬프
INTERVAL         -- 시간 간격

-- 기타
BOOLEAN          -- true/false
UUID             -- 고유 식별자
JSONB            -- JSON 데이터 (바이너리, 인덱싱 가능)
ARRAY            -- 배열
```

### 제약 조건
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,                      -- 기본키 (자동증가)
    name VARCHAR(100) NOT NULL,                  -- NULL 불가
    email VARCHAR(100) UNIQUE,                   -- 중복 불가
    age INTEGER CHECK (age >= 0 AND age <= 150), -- 범위 제한
    role VARCHAR(20) DEFAULT 'user',             -- 기본값
    department_id INTEGER REFERENCES departments(id) -- 외래키
);
```

---

## SQL 기초

### CREATE - 테이블 생성
```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    stock INTEGER DEFAULT 0,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);
```

### INSERT - 데이터 삽입
```sql
-- 단일 삽입
INSERT INTO products (name, price, stock, category)
VALUES ('노트북', 1500000, 10, '전자기기');

-- 다중 삽입
INSERT INTO products (name, price, stock, category) VALUES
    ('마우스', 30000, 100, '전자기기'),
    ('키보드', 80000, 50, '전자기기'),
    ('모니터', 500000, 20, '전자기기');

-- 삽입 후 결과 반환
INSERT INTO products (name, price, stock, category)
VALUES ('헤드셋', 150000, 30, '전자기기')
RETURNING *;
```

### SELECT - 데이터 조회
```sql
-- 전체 조회
SELECT * FROM products;

-- 특정 컬럼 조회
SELECT name, price FROM products;

-- 조건 조회
SELECT * FROM products WHERE price > 100000;

-- 정렬
SELECT * FROM products ORDER BY price DESC;

-- 페이징 (LIMIT + OFFSET)
SELECT * FROM products ORDER BY id LIMIT 10 OFFSET 20;

-- 집계 함수
SELECT
    category,
    COUNT(*) as count,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price,
    SUM(stock) as total_stock
FROM products
GROUP BY category
HAVING AVG(price) > 50000;

-- LIKE 패턴 매칭
SELECT * FROM products WHERE name LIKE '%노트%';

-- IN 연산자
SELECT * FROM products WHERE category IN ('전자기기', '가구');

-- BETWEEN
SELECT * FROM products WHERE price BETWEEN 10000 AND 100000;
```

### UPDATE - 데이터 수정
```sql
-- 단일 수정
UPDATE products SET price = 1400000 WHERE name = '노트북';

-- 여러 컬럼 수정
UPDATE products
SET price = price * 0.9, stock = stock + 10
WHERE category = '전자기기';

-- 수정 결과 반환
UPDATE products
SET price = price * 1.1
WHERE id = 1
RETURNING *;
```

### DELETE - 데이터 삭제
```sql
-- 조건 삭제
DELETE FROM products WHERE stock = 0;

-- 삭제 결과 반환
DELETE FROM products WHERE id = 5 RETURNING name;

-- 전체 삭제 (주의!)
TRUNCATE TABLE products;
```

### JOIN - 테이블 결합
```sql
-- 테이블 준비
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    ordered_at TIMESTAMP DEFAULT NOW()
);

-- INNER JOIN: 양쪽 모두 있는 데이터만
SELECT u.name, p.name as product, o.quantity
FROM orders o
INNER JOIN users u ON o.user_id = u.id
INNER JOIN products p ON o.product_id = p.id;

-- LEFT JOIN: 왼쪽 테이블 기준 (주문 없는 사용자도 포함)
SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name;

-- RIGHT JOIN: 오른쪽 테이블 기준
-- FULL OUTER JOIN: 양쪽 모두 포함
```

### 서브쿼리
```sql
-- WHERE절 서브쿼리
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- FROM절 서브쿼리
SELECT category, avg_price
FROM (
    SELECT category, AVG(price) as avg_price
    FROM products
    GROUP BY category
) sub
WHERE avg_price > 100000;

-- EXISTS
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

### 트랜잭션
```sql
BEGIN;

UPDATE accounts SET balance = balance - 10000 WHERE id = 1;
UPDATE accounts SET balance = balance + 10000 WHERE id = 2;

-- 문제 없으면
COMMIT;

-- 문제 있으면
ROLLBACK;
```

### 인덱스
```sql
-- 단일 컬럼 인덱스
CREATE INDEX idx_products_name ON products(name);

-- 복합 인덱스
CREATE INDEX idx_products_category_price ON products(category, price);

-- 유니크 인덱스
CREATE UNIQUE INDEX idx_users_email ON users(email);

-- 실행 계획 분석
EXPLAIN ANALYZE SELECT * FROM products WHERE name = '노트북';
```

---

## Node.js 연동

### 기본 연결
```javascript
const { Client } = require('pg');

const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'your_password',
    database: 'practice_db'
});

await client.connect();
```

### Connection Pool (실무 필수)
```javascript
const { Pool } = require('pg');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'your_password',
    database: 'practice_db',
    max: 20,              // 최대 연결 수
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000
});

// 쿼리 실행
const result = await pool.query('SELECT * FROM users WHERE id = $1', [1]);
console.log(result.rows[0]);
```

### Parameterized Query (SQL Injection 방지)
```javascript
// 올바른 방법 - $1, $2 플레이스홀더 사용
const result = await client.query(
    'SELECT * FROM users WHERE name = $1 AND age > $2',
    ['홍길동', 20]
);

// 잘못된 방법 - SQL Injection 위험!
// const result = await client.query(`SELECT * FROM users WHERE name = '${name}'`);
```

### CRUD 패턴
```javascript
// CREATE
async function createUser(name, email, age) {
    const result = await pool.query(
        'INSERT INTO users (name, email, age) VALUES ($1, $2, $3) RETURNING *',
        [name, email, age]
    );
    return result.rows[0];
}

// READ
async function getUser(id) {
    const result = await pool.query('SELECT * FROM users WHERE id = $1', [id]);
    return result.rows[0];
}

// UPDATE
async function updateUser(id, name, email) {
    const result = await pool.query(
        'UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING *',
        [name, email, id]
    );
    return result.rows[0];
}

// DELETE
async function deleteUser(id) {
    const result = await pool.query(
        'DELETE FROM users WHERE id = $1 RETURNING *',
        [id]
    );
    return result.rows[0];
}
```

### 트랜잭션 패턴
```javascript
async function transferMoney(fromId, toId, amount) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [amount, fromId]);
        await client.query('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [amount, toId]);
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release(); // 풀에 연결 반환
    }
}
```

---

## 실습 과제

### 과제 1: 기본 CRUD
- `test.js`를 실행해서 모든 테스트가 통과하는지 확인
- password를 실제 PostgreSQL 비밀번호로 변경

```bash
npm test
```

### 과제 2: 블로그 스키마 설계
아래 요구사항을 만족하는 테이블을 직접 만들어보기:
- 사용자(users): id, username, email, created_at
- 게시글(posts): id, title, content, author_id, created_at, updated_at
- 댓글(comments): id, post_id, author_id, content, created_at
- 태그(tags): id, name
- 게시글-태그(post_tags): post_id, tag_id (다대다 관계)

### 과제 3: 복잡한 쿼리 작성
위 블로그 스키마에서 다음 쿼리를 작성해보기:
1. 가장 많은 게시글을 쓴 사용자 TOP 5
2. 특정 태그가 달린 게시글 목록 (댓글 수 포함)
3. 최근 7일간 작성된 게시글과 작성자 정보
4. 댓글이 가장 많은 게시글 TOP 10

### 과제 4: 인덱스 최적화
- `EXPLAIN ANALYZE`로 쿼리 실행 계획을 분석하고
- 적절한 인덱스를 추가해서 성능을 개선해보기

---

## 테스트 코드 설명

### test.js 구조

| 테스트 함수 | 설명 | 핵심 개념 |
|------------|------|----------|
| `testConnection` | DB 연결 테스트 | Client, connect() |
| `testCreateTable` | 테이블 생성 | CREATE TABLE, 데이터 타입, 제약조건 |
| `testInsert` | 데이터 삽입 | INSERT, RETURNING, Parameterized Query |
| `testSelect` | 데이터 조회 | SELECT, WHERE, 집계함수(AVG) |
| `testUpdate` | 데이터 수정 | UPDATE, SET, RETURNING |
| `testDelete` | 데이터 삭제 | DELETE, RETURNING |
| `testTransaction` | 트랜잭션 | BEGIN, COMMIT, ROLLBACK |
| `testJoin` | JOIN 쿼리 | LEFT JOIN, FOREIGN KEY |
| `testIndexPerformance` | 인덱스 성능 | CREATE INDEX, EXPLAIN ANALYZE |

### 실행 방법
```bash
# 테스트 실행 전 PostgreSQL이 실행 중이어야 합니다
# test.js의 password를 실제 비밀번호로 변경하세요

npm test
```

### 주의사항
- PostgreSQL 서비스가 실행 중이어야 합니다
- `practice_db` 데이터베이스가 미리 생성되어 있어야 합니다
- 테스트는 테이블을 생성/삭제하므로 실제 데이터가 있는 DB에서는 실행하지 마세요
