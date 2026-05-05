# Topic 2: Databases & Roles (데이터베이스와 역할)

## 1. Cluster vs Database (클러스터 vs 데이터베이스)

A PostgreSQL **cluster** is a single running instance managed by one Postmaster.
A cluster can contain multiple **databases**.

PostgreSQL **클러스터**는 하나의 Postmaster가 관리하는 단일 인스턴스입니다.
하나의 클러스터 안에 여러 **데이터베이스**를 만들 수 있습니다.

```
Cluster (클러스터)
+-- postgres      <-- default database (기본 데이터베이스)
+-- template0     <-- clean template, never modify (깨끗한 템플릿, 수정 금지)
+-- template1     <-- customizable template (커스터마이징 가능한 템플릿)
+-- study_db      <-- user-created (사용자가 만든 DB)
```

### Verify (확인)

```sql
\l   -- list all databases (모든 데이터베이스 목록)
```

---

## 2. Database List Columns (\l 결과 컬럼 설명)

| Column              | Meaning (의미)                                                                 |
|---------------------|--------------------------------------------------------------------------------|
| **Owner**           | The role that owns this database. Has full control. / 데이터베이스 소유자. 전체 제어 권한 보유. |
| **Encoding**        | Character encoding (e.g. UTF8 = all languages). / 문자 인코딩 (UTF8 = 모든 언어 지원). |
| **Collate**         | String sorting rules. `C` = byte-order (fastest). / 문자열 정렬 규칙. `C` = 바이트 순서 (가장 빠름). |
| **Ctype**           | Character classification rules (uppercase, lowercase, digit). / 문자 분류 규칙 (대문자, 소문자, 숫자 판단). |
| **ICU Locale**      | ICU library locale. Empty = not used. / ICU 라이브러리 로케일. 비어있음 = 미사용. |
| **Locale Provider** | `libc` (OS library) or `icu` (ICU library). / 로케일 제공 라이브러리. |
| **Access Privileges** | Who can do what. Empty = default privileges apply. / 접근 권한. 비어있음 = 기본 권한 적용. |

### Collate: C vs Locale (C와 로케일 비교)

```
-- Collate = C (byte order, 바이트 순서):
'A' < 'B' < 'a' < 'b'       <-- uppercase first

-- Collate = ko_KR.UTF-8 (linguistic order, 언어적 순서):
'a' < 'A' < 'b' < 'B'       <-- language-aware
'가' < '나' < '다'            <-- Korean sorting works properly
```

### Access Privileges Syntax (접근 권한 문법)

Format: `grantee=privileges/grantor` (형식: `권한받은자=권한/권한준자`)

```
=c/kimdonghyuk                <-- PUBLIC (everyone) can CONNECT
kimdonghyuk=CTc/kimdonghyuk   <-- kimdonghyuk has CREATE + TEMP + CONNECT
```

| Letter | Privilege (권한)  |
|--------|-------------------|
| `C`    | CREATE            |
| `T`    | TEMPORARY         |
| `c`    | CONNECT           |

Empty access privileges = **default privileges apply**:
- Owner: everything (모든 권한)
- PUBLIC: CONNECT + TEMP (접속 + 임시 테이블)

---

## 3. Roles (역할)

PostgreSQL has no separate "user" and "group". Everything is a **role**.
A role with `LOGIN` acts as a user. Without `LOGIN`, it acts as a group.

PostgreSQL은 "사용자"와 "그룹"을 따로 구분하지 않습니다. 모든 것이 **role**입니다.
`LOGIN` 권한이 있으면 사용자, 없으면 그룹처럼 동작합니다.

```sql
-- Create a user role (사용자 역할 생성)
CREATE ROLE study_user WITH LOGIN PASSWORD 'study123';

-- Create a group role (그룹 역할 생성)
CREATE ROLE readonly;

-- Grant role to another role (역할 부여)
GRANT readonly TO study_user;

-- List all roles (모든 역할 조회)
\du
```

---

## 4. Creating a Database (데이터베이스 생성)

```sql
CREATE DATABASE study_db OWNER study_user;

-- Connect to the database (데이터베이스 접속)
\c study_db
```

**Important:** Every SQL statement must end with a **semicolon (`;`)**.
**중요:** 모든 SQL 문은 반드시 **세미콜론(`;`)**으로 끝나야 합니다.

```
postgres=#   <-- ready for new statement (새 문장 입력 대기)
postgres-#   <-- waiting for more input, missing semicolon (세미콜론 누락, 추가 입력 대기)
```

---

## 5. Privilege Layers (권한 계층)

PostgreSQL uses a layered permission system.
PostgreSQL은 계층적 권한 시스템을 사용합니다.

```
Database level (데이터베이스 레벨)
|  CONNECT  -- can I connect? (접속 가능?)
|  TEMP     -- can I create temp tables? (임시 테이블 생성 가능?)
|  CREATE   -- can I create schemas? (스키마 생성 가능?)
|
+-- Schema level (스키마 레벨)
    |  USAGE  -- can I access objects? (객체 접근 가능?)
    |  CREATE -- can I create tables? (테이블 생성 가능?)
    |
    +-- Table level (테이블 레벨)
        SELECT -- read (읽기)
        INSERT -- add rows (행 추가)
        UPDATE -- modify rows (행 수정)
        DELETE -- remove rows (행 삭제)
```

For PUBLIC with default privileges (기본 권한에서 PUBLIC):
- **Can** connect (접속 가능)
- **Can** create temporary tables (임시 테이블 생성 가능)
- **Cannot** create schemas or real tables (스키마나 일반 테이블 생성 불가)
- **Cannot** SELECT, INSERT, UPDATE, DELETE unless explicitly granted (명시적으로 부여하지 않는 한 조회/수정/삭제 불가)

---

## 6. Schema (스키마)

A schema is a **namespace** inside a database. The default schema is `public`.
스키마는 데이터베이스 안의 **네임스페이스**입니다. 기본 스키마는 `public`입니다.

```
Database: study_db
+-- Schema: public       <-- default (기본)
|   +-- users table
|   +-- orders table
+-- Schema: analytics    <-- custom (사용자 정의)
|   +-- daily_stats table
```

```sql
-- Create a schema (스키마 생성)
CREATE SCHEMA analytics;

-- Create table in specific schema (특정 스키마에 테이블 생성)
CREATE TABLE analytics.daily_stats (id serial PRIMARY KEY, stat_date date);

-- Show schemas (스키마 목록)
\dn

-- Show current search path (현재 검색 경로)
SHOW search_path;
```
