# SimpleDB 학습 가이드

SimpleDB 데이터베이스 엔진을 단계별로 학습하기 위한 자료입니다.

## 📚 학습 순서

### 1단계: 기초 레이어
- [✅ Page 클래스 기초](./01_Page_클래스_기초.md) - 데이터베이스의 기본 저장 단위
- [ ] StorageManager - 디스크 I/O와 페이지 관리

### 2단계: 메모리 관리  
- [ ] BufferPoolManager - LRU 버퍼 풀과 pin/unpin 메커니즘

### 3단계: 데이터 구조
- [ ] Record 클래스 - variant 타입 데이터와 직렬화

### 4단계: 인덱싱
- [ ] BTree 구현 - B-tree 구조와 인덱스 관리

### 5단계: 쿼리 처리
- [ ] SQLParser - SQL 문법 파싱
- [ ] Database 클래스 - 쿼리 실행과 전체 시스템 통합

### 6단계: 통합 테스트
- [ ] 전체 시스템 동작 확인

## 🎯 학습 목표

각 단계를 완료하면:
- 데이터베이스 내부 구조 이해
- 페이지 기반 저장소 개념
- 버퍼 풀 관리 원리  
- B-tree 인덱싱 구조
- SQL 쿼리 실행 과정

## 🛠️ 실습 환경

```bash
# 프로젝트 빌드
mkdir -p build && cd build
cmake ..
make

# 실행
./simpledb
```

## 📂 프로젝트 구조

```
src/
├── page.{h,cpp}                 - 페이지 추상화
├── storage_manager.{h,cpp}      - 디스크 I/O 
├── buffer_pool_manager.{h,cpp}  - 버퍼 풀 관리
├── record.{h,cpp}              - 레코드 구조
├── btree.{h,cpp}               - B-tree 인덱싱
├── sql_parser.{h,cpp}          - SQL 파싱
├── database.{h,cpp}            - 데이터베이스 엔진
└── main.cpp                    - 데모 애플리케이션
```