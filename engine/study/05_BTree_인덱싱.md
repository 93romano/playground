# 5단계: BTree - 초고속 데이터 검색의 마법사 🌳

## 🏃‍♂️ BTree = 구글 검색엔진의 비밀무기

### BTree가 하는 일

웹 개발할 때 이런 경험 있으시죠?
```javascript
// 10만 개 데이터에서 특정 사용자 찾기
const users = [...]; // 100,000개
const user = users.find(u => u.id === 12345); // 최악의 경우 100,000번 검색!
```

**BTree = 데이터가 아무리 많아도 몇 번 안에 찾는 마법사**
- 📈 100만 개 → 최대 **20번** 만에 찾기
- 🚀 10억 개 → 최대 **30번** 만에 찾기
- ⚡ O(log n) 검색 성능

## 🏢 실생활 비유: 회사 조직도

### 왜 BTree가 빠른가?

**일반적인 검색 (배열)**
```
사장님이 "김철수 찾아줘"
→ 1층부터 한명씩 다 확인 (최악 10,000명)
```

**BTree 검색 (조직도 활용)**
```
사장님: "김철수 찾아줘"
→ 부장: "개발팀인지 마케팅팀인지?"
→ 팀장: "프론트엔드팀인지 백엔드팀인지?"
→ 팀원: "김철수 여기 있어요!"
총 3번만에 발견!
```

## 🎯 웹 개발자에게 친숙한 비교

### 1. BTree vs 웹 개발의 검색 방법들

| 방법 | 데이터량 | 검색 시간 | 웹에서 예시 |
|------|---------|----------|------------|
| **선형 검색** | 1만개 → 5000번 | O(n) | `array.find()` |
| **해시 테이블** | 1만개 → 1번 | O(1) | JavaScript `Map`, `Set` |
| **BTree** | 1만개 → 4번 | O(log n) | 데이터베이스 인덱스 |

### 2. React에서 BTree 개념 응용
```javascript
// 비효율적인 컴포넌트 검색
function findComponent(components, id) {
  return components.find(c => c.id === id); // O(n)
}

// BTree 개념을 적용한 효율적 검색
class ComponentIndex {
  constructor() {
    this.index = new Map(); // 해시 테이블 활용
  }

  // O(1) 검색
  find(id) {
    return this.index.get(id);
  }
}
```

## 🌳 BTree 구조 상세 분석

### 1. BTreeNode 구조 - DOM Element와 비슷
```cpp
struct BTreeNode {
    bool is_leaf{false};              // 말단 노드인가?
    std::vector<int> keys;            // 정렬된 키들
    std::vector<page_id_t> children;  // 자식 노드들
    std::vector<Record> records;      // 실제 데이터 (leaf에만)
    page_id_t next_leaf{INVALID_PAGE_ID}; // 다음 leaf 연결 (Linked List)
};
```

**DOM Tree와 비교:**
```javascript
// DOM Element (비슷한 구조)
class DOMElement {
  constructor() {
    this.tagName = '';           // keys와 비슷
    this.children = [];          // children과 동일
    this.textContent = '';       // records와 비슷
    this.nextSibling = null;     // next_leaf와 비슷
  }
}
```

### 2. Order = 4 (각 노드에 최대 3개 키)
```cpp
constexpr int BTREE_ORDER = 4;
```

**실제 노드 구조:**
```
Internal Node: [10 | 20 | 30]
               ↓    ↓    ↓    ↓
             <10  10-20 20-30 >30
```

**React Router와 비슷한 개념:**
```javascript
// Route 구조가 BTree와 비슷
<Router>
  <Route path="/api/*">           {/* 10보다 작은 경우 */}
    <Route path="users" />
    <Route path="posts" />
  </Route>
  <Route path="/admin/*">         {/* 10-20 사이 */}
    <Route path="dashboard" />
  </Route>
</Router>
```

## 🔧 주요 기능들 상세 분석

### 1. Search() - 트리 탐색
```cpp
bool BTree::Search(int key, Record& record)
```

**동작 과정:**
```cpp
// 1. 루트부터 시작
page_id_t current = root_page_id_;

// 2. Leaf 노드까지 내려가기
while (!node.is_leaf) {
    // 키 범위에 따라 자식 선택
    int index = FindKeyIndex(node.keys, key);
    current = node.children[index];
}

// 3. Leaf에서 이진 검색
auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
if (it != leaf.keys.end() && *it == key) {
    // 찾았다!
}
```

**웹 개발과 비교:**
```javascript
// URL 라우팅과 비슷한 과정
function findRoute(path) {
  let current = rootRouter;         // 1. 루트부터 시작

  while (current.hasChildren()) {   // 2. 말단까지 내려가기
    for (let route of current.children) {
      if (path.startsWith(route.path)) {
        current = route;
        break;
      }
    }
  }

  return current.component;         // 3. 최종 컴포넌트 반환
}
```

### 2. Insert() - 데이터 삽입과 노드 분할
```cpp
bool BTree::Insert(int key, const Record& record)
```

**핵심 아이디어: 노드가 꽉 차면 둘로 나누기**

**Before (노드가 가득 참):**
```
[10 | 20 | 30]  ← 여기에 25 추가하려고 함
```

**After (분할 후):**
```
       [25] ← 중간값을 부모로 올림
      ↙    ↘
  [10|20]  [30] ← 둘로 나눔
```

**React 컴포넌트 분할과 비슷:**
```javascript
// 컴포넌트가 너무 커지면 분할
function HugeComponent() {
  // 너무 많은 기능...
}

// 분할 후
function ParentComponent() {
  return (
    <>
      <LeftComponent />   {/* [10|20] */}
      <RightComponent />  {/* [30] */}
    </>
  );
}
```

### 3. RangeScan() - 범위 검색의 마법
```cpp
std::vector<Record> BTree::RangeScan(int start_key, int end_key)
```

**핵심: Leaf 노드들이 연결리스트로 연결됨**

**구조:**
```
Leaf1: [1|3|5] → Leaf2: [7|9|11] → Leaf3: [13|15|17] → null
```

**범위 검색 (5~12):**
1. **5 찾기** → Leaf1에서 시작
2. **순차 탐색** → Leaf1의 [5], Leaf2의 [7,9,11]
3. **12 초과시** → 중단

**웹에서 비슷한 패턴:**
```javascript
// Pagination + 순차 검색
async function getRangeData(startId, endId) {
  let currentPage = findStartPage(startId);
  const results = [];

  while (currentPage && currentPage.hasData()) {
    for (let item of currentPage.items) {
      if (item.id >= startId && item.id <= endId) {
        results.push(item);
      } else if (item.id > endId) {
        return results; // 범위 초과시 중단
      }
    }
    currentPage = currentPage.next; // 다음 페이지
  }

  return results;
}
```

### 4. Split 알고리즘 - 노드 분할의 정교함

**Leaf Node Split:**
```cpp
void BTree::SplitLeafNode(page_id_t leaf_page_id, int key, const Record& record)
```

**과정:**
```cpp
// 1. 모든 키를 정렬된 배열에 넣기
std::vector<int> all_keys = leaf.keys;
all_keys.insert(sorted_position, new_key); // [1,3,5,7,9]

// 2. 중간에서 나누기
int mid = all_keys.size() / 2; // mid = 2
left_keys = [1,3];   // 앞쪽
right_keys = [5,7,9]; // 뒤쪽

// 3. 연결 리스트 업데이트
left.next_leaf = right_page_id;
right.next_leaf = old_next;
```

**배열 분할과 비슷한 JavaScript:**
```javascript
function splitArray(arr, newItem) {
  // 1. 정렬된 위치에 삽입
  const allItems = [...arr];
  const insertPos = findInsertPosition(allItems, newItem);
  allItems.splice(insertPos, 0, newItem);

  // 2. 중간에서 분할
  const mid = Math.floor(allItems.length / 2);
  const left = allItems.slice(0, mid);
  const right = allItems.slice(mid);

  return { left, right };
}
```

## 🚀 성능 분석 - 왜 이렇게 빠른가?

### 1. 시간 복잡도 비교

```javascript
// 배열 검색: O(n)
function linearSearch(arr, target) {
  for (let i = 0; i < arr.length; i++) {  // 최악 n번
    if (arr[i] === target) return i;
  }
}

// 이진 검색: O(log n)
function binarySearch(arr, target) {
  // 매번 절반씩 줄어들어서 log₂(n)번
}

// BTree 검색: O(log n)
// 각 레벨에서 최대 4개 중 선택 → 높이 = log₄(n)
```

### 2. 실제 성능 계산

| 데이터 수 | 배열 검색 | BTree 검색 | 성능 향상 |
|----------|----------|-----------|----------|
| 1,000 | 500번 | 5번 | **100배** |
| 1,000,000 | 500,000번 | 10번 | **50,000배** |
| 1,000,000,000 | 500,000,000번 | 15번 | **33,333,333배** |

### 3. 메모리 지역성 (Cache Friendly)
```cpp
// BTree는 페이지 단위로 읽음
struct BTreeNode {
    int keys[3];        // 연속된 메모리
    page_id_t children[4]; // 연속된 메모리
    // → CPU 캐시에 효율적으로 로드
};
```

**웹에서 비슷한 최적화:**
```javascript
// 비효율적: 객체마다 따로 접근
const users = [
  {id: 1, name: "김철수"},
  {id: 2, name: "이영희"}
];

// 효율적: 같은 타입 데이터를 연속으로 배치
const userIds = [1, 2, 3, 4];     // 연속된 메모리
const userNames = ["김철수", "이영희"]; // 연속된 메모리
```

## 💡 웹 개발에서의 응용

### 1. 인덱싱 전략
```javascript
// MongoDB 인덱싱과 동일한 원리
db.users.createIndex({ "email": 1 });  // 이메일로 BTree 인덱스

// 검색시 BTree 활용
db.users.find({ email: "user@example.com" }); // O(log n)
db.users.find({ email: { $gte: "a", $lte: "m" } }); // Range Scan
```

### 2. 프론트엔드 최적화
```javascript
// 대용량 데이터 가상 스크롤링
class VirtualList {
  constructor(items) {
    this.tree = this.buildSearchTree(items); // BTree 구조
  }

  // 화면에 보일 아이템만 렌더링
  getVisibleItems(startIndex, endIndex) {
    return this.tree.rangeSearch(startIndex, endIndex); // O(log n)
  }
}
```

### 3. 상태 관리 최적화
```javascript
// Redux/Zustand에서 대용량 데이터
class IndexedState {
  constructor() {
    this.data = new Map();        // O(1) 단건 조회
    this.sortedKeys = [];         // 정렬된 키 (BTree leaf와 비슷)
  }

  rangeSelect(start, end) {
    const startIdx = this.binarySearch(start);
    const endIdx = this.binarySearch(end);
    return this.sortedKeys.slice(startIdx, endIdx + 1)
                          .map(key => this.data.get(key));
  }
}
```

## 🔍 직렬화와 메모리 관리

### 1. 노드 직렬화 - JSON.stringify()와 비슷
```cpp
void BTree::SerializeNode(const BTreeNode& node, Page* page) {
    // 1. 노드 타입 저장
    std::memcpy(data, &node.is_leaf, sizeof(bool));

    // 2. 키 개수와 키들 저장
    std::memcpy(data, &key_count, sizeof(size_t));
    for (int key : node.keys) {
        std::memcpy(data, &key, sizeof(int));
    }

    // 3. 자식/레코드 저장
    if (is_leaf) {
        for (const auto& record : node.records) {
            record.Serialize(data + offset); // Record 직렬화 재사용
        }
    }
}
```

**웹에서 비슷한 직렬화:**
```javascript
// 트리 구조 직렬화
function serializeTree(node) {
  return {
    isLeaf: node.isLeaf,
    keys: node.keys,
    children: node.isLeaf ? null : node.children.map(serializeTree),
    data: node.isLeaf ? node.data : null,
    nextSibling: node.nextSibling
  };
}
```

### 2. BufferPool과의 연계
```cpp
// 1. 페이지 가져오기
Page* page = buffer_pool_manager_->FetchPage(page_id);

// 2. 노드로 역직렬화
BTreeNode node = DeserializeNode(page);

// 3. 작업 수행

// 4. 직렬화 후 저장
SerializeNode(node, page);
buffer_pool_manager_->UnpinPage(page_id, true); // dirty = true
```

## 🎯 핵심 개념 정리

### 1. BTree의 핵심 아이디어
1. **균형 트리**: 모든 경로의 길이가 비슷 → 일정한 성능
2. **정렬된 구조**: 이진 검색 활용 가능
3. **블록 단위 I/O**: 디스크 특성에 최적화
4. **범위 검색**: Linked List로 연결된 Leaf 노드

### 2. 웹 개발자가 배울 점
- **알고리즘적 사고**: 문제를 분할해서 해결
- **캐시 친화적 설계**: 연속된 메모리 접근
- **트레이드오프**: 삽입 복잡도 vs 검색 성능
- **인덱싱 전략**: 적절한 인덱스 설계의 중요성

### 3. 실무에서 활용
- 데이터베이스 쿼리 최적화 이해
- 대용량 데이터 처리 방법
- 메모리와 디스크 I/O 최적화
- 복잡한 자료구조 설계 능력

## 📍 관련 파일 위치
- `src/btree.h:8-49` - BTree 클래스와 BTreeNode 구조 정의
- `src/btree.cpp:11-82` - 핵심 연산 (Insert, Search, Delete, RangeScan)
- `src/btree.cpp:221-296` - 노드 분할 알고리즘 (Split)
- `src/btree.cpp:303-366` - 직렬화/역직렬화

## ➡️ 다음 단계
SQLParser를 보면 이 BTree를 사용해서 실제 SQL 쿼리가 어떻게 실행되는지 알 수 있어요!