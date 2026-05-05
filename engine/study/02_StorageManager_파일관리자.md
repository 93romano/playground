# 2단계: StorageManager - 데이터베이스의 창고 관리자

## 🏢 회사 창고 관리자를 상상해보세요!

### StorageManager가 하는 일

Page 클래스가 "방"이었다면, StorageManager는 **"창고 관리자"**입니다.

**창고 관리자의 역할:**
- 📦 박스(Page)를 창고(디스크)에 저장
- 📋 어떤 박스가 어디에 있는지 기록 관리
- 🔍 필요할 때 박스를 찾아서 가져오기
- ✍️ 박스 내용이 바뀌면 창고에 다시 저장

## 📚 웹 개발자에게 친숙한 비유

### 1. 파일 시스템 = 클라우드 스토리지
```cpp
std::string db_file_;        // "database.db" - 구글 드라이브의 폴더명 같은 것
std::fstream file_stream_;   // 파일을 읽고 쓰는 도구 (API 클라이언트 같은 역할)
```

**실생활 예시:**
- `db_file_` = "내 프로젝트 폴더" (Google Drive에서)
- `file_stream_` = Google Drive API (파일 업로드/다운로드하는 도구)

### 2. 페이지 ID = URL 주소
```cpp
page_id_t next_page_id_{0};  // 다음에 사용할 방 번호
```

**웹 개발 비유:**
- Page ID `0` = `https://mysite.com/page/0`
- Page ID `1` = `https://mysite.com/page/1`
- `next_page_id_`는 다음에 만들 페이지 번호 (자동 증가하는 ID)

## 🔧 주요 기능들 (웹개발자 관점)

### 1. 생성자 - 서버 초기화
```cpp
StorageManager::StorageManager(const std::string& db_file) : db_file_(db_file)
```

**웹 서버 시작할 때와 비슷:**
```javascript
// Node.js 서버 시작할 때
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`서버가 ${PORT}번 포트에서 실행중`);
});
```

### 2. OpenFile() - 데이터베이스 연결
```cpp
file_stream_.open(db_file_, std::ios::in | std::ios::out | std::ios::binary);
```

**MongoDB 연결하는 것과 비슷:**
```javascript
// MongoDB 연결
const mongoose = require('mongoose');
mongoose.connect('mongodb://localhost:27017/mydb', {
  useNewUrlParser: true,
  useUnifiedTopology: true
});
```

**파일 열기 과정:**
1. 파일이 있으면 → 기존 DB 열기
2. 파일이 없으면 → 새 DB 파일 생성 후 열기
3. 파일 크기 확인해서 → 다음 페이지 번호 계산

### 3. ReadPage() - 데이터 조회 (GET 요청)
```cpp
std::unique_ptr<Page> StorageManager::ReadPage(page_id_t page_id)
```

**REST API의 GET 요청과 같은 개념:**
```javascript
// Express.js에서 특정 데이터 조회
app.get('/api/pages/:pageId', async (req, res) => {
  const pageId = req.params.pageId;
  const data = await database.findById(pageId);
  res.json(data);
});
```

**동작 과정:**
1. `file_stream_.seekg(page_id * PAGE_SIZE, std::ios::beg)`
   → 파일에서 정확한 위치로 이동 (데이터베이스에서 특정 ID 찾기)
2. `file_stream_.read(page->GetData(), PAGE_SIZE)`
   → 4KB 데이터를 읽어옴 (SELECT 쿼리 실행)
3. Page 객체로 만들어서 반환

### 4. WritePage() - 데이터 저장 (PUT 요청)
```cpp
bool StorageManager::WritePage(const Page& page)
```

**REST API의 PUT/POST 요청과 같은 개념:**
```javascript
// Express.js에서 데이터 업데이트
app.put('/api/pages/:pageId', async (req, res) => {
  const pageId = req.params.pageId;
  const updated = await database.updateById(pageId, req.body);
  res.json({ success: updated });
});
```

**동작 과정:**
1. `file_stream_.seekp(page.GetPageId() * PAGE_SIZE)`
   → 저장할 위치로 이동
2. `file_stream_.write(page.GetData(), PAGE_SIZE)`
   → 4KB 데이터를 디스크에 쓰기
3. `file_stream_.flush()`
   → 강제로 디스크에 반영 (트랜잭션 commit 같은 개념)

### 5. AllocatePage() - 새 ID 생성 (자동증가 ID)
```cpp
page_id_t StorageManager::AllocatePage() {
    return next_page_id_++;
}
```

**Auto Increment ID와 같은 개념:**
```javascript
// MongoDB에서 새 문서 생성시 ObjectId 자동 생성
const newDocument = new MyModel({
  // _id는 자동으로 생성됨
  title: "새 페이지",
  content: "내용"
});
```

## 🔍 핵심 개념

### 1. 바이너리 파일 처리
```cpp
std::ios::binary  // 텍스트가 아닌 바이너리 데이터로 처리
```
- JSON이나 텍스트가 아닌 **순수 바이트 데이터**로 저장
- 용량이 작고 읽기/쓰기가 빠름
- 사람이 읽을 수는 없지만 컴퓨터가 처리하기엔 최적

### 2. 페이지 위치 계산
```cpp
page_id * PAGE_SIZE  // 페이지 시작 위치 계산
```

**배열 인덱스와 비슷한 개념:**
```javascript
// JavaScript 배열에서 인덱스로 접근
const pages = [page0, page1, page2, page3];
const page2 = pages[2];  // 2번 인덱스

// StorageManager는 파일에서:
// Page 0: 파일의 0번째 바이트부터 4KB
// Page 1: 파일의 4096번째 바이트부터 4KB
// Page 2: 파일의 8192번째 바이트부터 4KB
```

## 💡 왜 이렇게 설계했을까?

### 1. 고정 크기 페이지
- **장점**: 계산이 쉬움 (page_id * PAGE_SIZE)
- **단점**: 작은 데이터도 4KB를 다 차지
- **웹 개발 비유**: CSS Grid의 고정 셀 크기와 비슷

### 2. 바이너리 파일
- **장점**: 빠르고 용량 효율적
- **단점**: 사람이 직접 읽기 어려움
- **웹 개발 비유**: JSON vs MessagePack 차이

## 📍 관련 파일 위치
- `src/storage_manager.h:7-23` - StorageManager 클래스 정의
- `src/storage_manager.cpp:4-63` - 실제 구현부

## ➡️ 다음 단계
BufferPoolManager를 보면 이 StorageManager를 어떻게 효율적으로 사용하는지 알 수 있어요. (메모리 캐시 같은 개념!)