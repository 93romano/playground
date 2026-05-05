# PostgreSQL 실습 가이드

## 1. 설치 및 설정

### Mac
```bash
brew install postgresql
brew services start postgresql
```

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

## 2. 기본 명령어

### 데이터베이스 접속
```bash
psql -U postgres
```

### 주요 명령어
- `\l` : 데이터베이스 목록
- `\c dbname` : 데이터베이스 연결
- `\dt` : 테이블 목록
- `\d tablename` : 테이블 구조
- `\q` : 종료

## 3. 실습 예제

### 데이터베이스 생성
```sql
CREATE DATABASE practice_db;
\c practice_db;
```

### 테이블 생성
```sql
-- 사용자 테이블
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 게시글 테이블
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    published BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 댓글 테이블
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 인덱스 생성
```sql
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published);
CREATE INDEX idx_comments_post_id ON comments(post_id);
```

## 4. CRUD 연습

### INSERT
```sql
-- 사용자 추가
INSERT INTO users (username, email) VALUES
    ('john_doe', 'john@example.com'),
    ('jane_smith', 'jane@example.com'),
    ('bob_wilson', 'bob@example.com');

-- 게시글 추가
INSERT INTO posts (user_id, title, content, published) VALUES
    (1, 'PostgreSQL 기초', 'PostgreSQL은 강력한 관계형 데이터베이스입니다.', true),
    (2, 'SQL 쿼리 최적화', 'EXPLAIN ANALYZE를 사용해보세요.', true),
    (1, '초안', '작성 중...', false);

-- 댓글 추가
INSERT INTO comments (post_id, user_id, content) VALUES
    (1, 2, '좋은 글이네요!'),
    (1, 3, '도움이 되었습니다.');
```

### SELECT
```sql
-- 모든 사용자 조회
SELECT * FROM users;

-- 게시된 글만 조회
SELECT p.*, u.username
FROM posts p
JOIN users u ON p.user_id = u.id
WHERE p.published = true;

-- 댓글이 있는 게시글 조회
SELECT p.title, COUNT(c.id) as comment_count
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.id, p.title
HAVING COUNT(c.id) > 0;
```

### UPDATE
```sql
-- 게시글 수정
UPDATE posts
SET title = 'PostgreSQL 완전정복',
    updated_at = CURRENT_TIMESTAMP
WHERE id = 1;

-- 사용자 이메일 변경
UPDATE users
SET email = 'john.doe@newdomain.com'
WHERE username = 'john_doe';
```

### DELETE
```sql
-- 댓글 삭제
DELETE FROM comments WHERE id = 1;

-- 미발행 게시글 삭제
DELETE FROM posts WHERE published = false;
```

## 5. 고급 기능

### 트랜잭션
```sql
BEGIN;
    INSERT INTO users (username, email) VALUES ('new_user', 'new@example.com');
    INSERT INTO posts (user_id, title, content) VALUES
        (currval('users_id_seq'), '첫 게시글', '안녕하세요!');
COMMIT;
```

### 뷰 생성
```sql
CREATE VIEW post_summary AS
SELECT
    p.id,
    p.title,
    u.username as author,
    COUNT(c.id) as comment_count,
    p.created_at
FROM posts p
JOIN users u ON p.user_id = u.id
LEFT JOIN comments c ON p.id = c.post_id
WHERE p.published = true
GROUP BY p.id, p.title, u.username, p.created_at;
```

### 저장 프로시저
```sql
CREATE OR REPLACE FUNCTION get_user_posts(user_name VARCHAR)
RETURNS TABLE(
    post_id INT,
    title VARCHAR,
    created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT p.id, p.title, p.created_at
    FROM posts p
    JOIN users u ON p.user_id = u.id
    WHERE u.username = user_name;
END;
$$ LANGUAGE plpgsql;

-- 사용
SELECT * FROM get_user_posts('john_doe');
```

## 6. 성능 최적화

### EXPLAIN 사용
```sql
EXPLAIN ANALYZE
SELECT * FROM posts WHERE user_id = 1;
```

### 통계 정보 확인
```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## 7. 백업과 복원

### 백업
```bash
pg_dump -U postgres -d practice_db > backup.sql
```

### 복원
```bash
psql -U postgres -d new_db < backup.sql
```