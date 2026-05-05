# 3단계: BufferPoolManager - 똑똑한 메모리 캐시 매니저

## 💾 웹 개발자에게 익숙한 Redis 캐시를 상상해보세요!

### BufferPoolManager가 하는 일

**Redis 캐시 + 더 똑똑한 기능**을 합친 것입니다!

- 🏃‍♂️ **빠른 접근**: 자주 사용하는 페이지를 메모리에 보관
- 🧠 **똑똑한 관리**: LRU 알고리즘으로 효율적 메모리 사용
- 💾 **자동 저장**: 변경된 데이터를 적절한 시점에 디스크에 저장
- 🔒 **동시성 관리**: 여러 프로세스가 같은 페이지를 안전하게 사용

## 🏢 실생활 비유: 도서관 열람실

### 도서관 시스템과 비교해보세요:

**도서관 구조:**
- 📚 **서고 (StorageManager)**: 모든 책이 보관된 창고 (느림, 용량 많음)
- 🪑 **열람실 (BufferPool)**: 사람들이 실제로 책을 읽는 공간 (빠름, 자리 제한)
- 📋 **사서 (BufferPoolManager)**: 책 대출/반납을 관리하는 사람

## 📊 웹 개발자 관점에서 이해하기

### 1. Frame = 캐시 슬롯
```cpp
struct Frame {
    std::unique_ptr<Page> page;    // 캐시된 데이터
    int pin_count{0};              // 현재 사용 중인 사용자 수
    bool is_dirty{false};          // 수정되었는지 여부
};
```

**Redis Hash와 비슷:**
```javascript
// Redis에서 캐시 데이터
const cacheData = {
  key: "page_123",
  value: { /* 페이지 데이터 */ },
  ttl: 3600,        // TTL 대신 pin_count 사용
  dirty: false      // 수정 여부
};
```

### 2. 메모리 풀 관리
```cpp
std::vector<std::unique_ptr<Frame>> frames_;  // 캐시 슬롯들
std::list<Frame*> free_list_;                 // 사용 가능한 슬롯
std::list<Frame*> lru_list_;                  // 사용 빈도 순서
```

**웹 서버의 커넥션 풀과 비슷:**
```javascript
// Express.js + Redis 커넥션 풀
const redis = require('redis');
const pool = redis.createClient({
  // 최대 연결 수 제한
  socket: {
    connectTimeout: 60000,
    lazyConnect: true,
    keepAlive: true
  },
  // LRU 방식으로 오래된 연결 정리
});
```

## 🔧 주요 기능들 상세 분석

### 1. FetchPage() - 캐시에서 데이터 가져오기
```cpp
Page* BufferPoolManager::FetchPage(page_id_t page_id)
```

**React Query 같은 캐시 라이브러리와 비슷:**
```javascript
// React Query의 useQuery 훅
const { data, isLoading } = useQuery(['page', pageId], async () => {
  // 1. 캐시에 있는지 확인
  const cached = queryClient.getQueryData(['page', pageId]);
  if (cached) {
    // 2. 캐시 히트 - 사용 빈도 업데이트
    return cached;
  }

  // 3. 캐시 미스 - 서버에서 데이터 가져오기
  const data = await fetchPageFromServer(pageId);
  return data;
});
```

**동작 과정:**
1. **캐시 확인**: `page_table_.find(page_id)` - HashMap에서 O(1) 검색
2. **캐시 히트**: 이미 메모리에 있으면 → 사용 횟수 증가 + LRU 리스트 맨 앞으로 이동
3. **캐시 미스**: 없으면 → 빈 슬롯 찾기 → 디스크에서 읽기 → 캐시에 저장

### 2. Pin/Unpin 시스템 - 참조 카운팅
```cpp
frame->pin_count++;  // 사용 시작
frame->pin_count--;  // 사용 끝
```

**JavaScript 가비지 컬렉션과 비슷한 개념:**
```javascript
// 웹 개발에서 EventListener 관리와 비슷
class ComponentManager {
  constructor() {
    this.listeners = new Map(); // page_id -> listener count
  }

  // Pin = addEventListener
  addListener(elementId, callback) {
    const current = this.listeners.get(elementId) || 0;
    this.listeners.set(elementId, current + 1);
    // 다른 곳에서 사용 중이므로 제거하면 안됨
  }

  // Unpin = removeEventListener
  removeListener(elementId) {
    const current = this.listeners.get(elementId) || 0;
    this.listeners.set(elementId, current - 1);
    // count가 0이 되면 안전하게 제거 가능
  }
}
```

### 3. LRU 알고리즘 - 가장 오래된 것 삭제
```cpp
std::list<Frame*> lru_list_;  // Least Recently Used 리스트
```

**브라우저 캐시와 똑같은 원리:**
```javascript
// LRU Cache 구현 (브라우저 캐시와 같은 원리)
class LRUCache {
  constructor(capacity) {
    this.capacity = capacity;
    this.cache = new Map(); // JavaScript Map은 insertion order 보장
  }

  get(key) {
    if (this.cache.has(key)) {
      // 사용했으므로 맨 뒤로 이동 (가장 최근 사용)
      const value = this.cache.get(key);
      this.cache.delete(key);
      this.cache.set(key, value);
      return value;
    }
    return null;
  }

  put(key, value) {
    if (this.cache.size >= this.capacity) {
      // 가장 오래된 것(맨 앞) 삭제
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
}
```

### 4. Dirty 플래그 - 변경사항 추적
```cpp
bool is_dirty{false};  // 수정되었는지 확인
```

**Git의 Working Directory와 비슷:**
```javascript
// Git 상태와 비슷한 개념
const fileStatus = {
  'file1.js': 'clean',     // is_dirty = false
  'file2.js': 'modified',  // is_dirty = true
  'file3.js': 'modified'   // is_dirty = true
};

// 커밋할 때 (Flush할 때)
Object.keys(fileStatus).forEach(file => {
  if (fileStatus[file] === 'modified') {
    // 변경된 파일만 저장 (git add + commit)
    saveToStorage(file);
    fileStatus[file] = 'clean';
  }
});
```

### 5. GetVictimFrame() - 제거할 슬롯 선택
```cpp
BufferPoolManager::Frame* BufferPoolManager::GetVictimFrame()
```

**메모리 가비지 컬렉션과 비슷:**
```javascript
// 메모리 정리 알고리즘 (단순화된 버전)
class MemoryManager {
  findVictim() {
    // 1. 빈 슬롯부터 확인
    for (let slot of this.freeSlots) {
      if (slot.isEmpty) return slot;
    }

    // 2. 사용하지 않는 오래된 것 찾기
    for (let slot of this.usedSlots.reverse()) {
      if (slot.refCount === 0) {  // pin_count === 0
        return slot;
      }
    }

    // 3. 사용 가능한 슬롯 없음
    return null; // Out of Memory!
  }
}
```

## 🎯 핵심 개념 정리

### 1. 왜 BufferPool이 필요한가?
```cpp
// StorageManager만 사용하면
Page* page = storage_manager->ReadPage(1);  // 디스크 읽기 (느림)
// 매번 디스크 접근 → 성능 저하

// BufferPoolManager 사용하면
Page* page = buffer_pool->FetchPage(1);     // 메모리에서 읽기 (빠름)
// 첫 번째만 디스크, 나머지는 메모리 → 성능 향상
```

### 2. 웹 개발과의 차이점
| 웹 개발 캐시 | BufferPool |
|-------------|------------|
| TTL로 만료 관리 | LRU로 공간 관리 |
| 네트워크 지연 최적화 | 디스크 I/O 최적화 |
| 서버 재시작시 초기화 | 프로그램 종료시 저장 |
| 캐시 미스 = API 호출 | 캐시 미스 = 디스크 읽기 |

### 3. 성능 이점
- **캐시 히트율 90%**라면 → 디스크 접근을 1/10로 줄임
- **메모리 접근**: 나노초 단위
- **디스크 접근**: 밀리초 단위 (1000배 차이!)

## 📍 관련 파일 위치
- `src/buffer_pool_manager.h:8-35` - BufferPoolManager 클래스 정의
- `src/buffer_pool_manager.cpp:4-160` - 실제 구현부

## ➡️ 다음 단계
Record 클래스를 보면 실제로 페이지에 어떤 데이터가 저장되는지 알 수 있어요!