# 7단계: Database 클래스 - 모든 것을 통합하는 지휘자 🎼

## 🎯 Database = 오케스트라 지휘자

### Database가 하는 일

지금까지 배운 모든 컴포넌트들을:

- **Page** (4KB 저장 단위) 📦
- **StorageManager** (디스크 I/O) 💾
- **BufferPoolManager** (메모리 캐시) ⚡
- **Record** (데이터 구조) 📊
- **BTree** (인덱스 검색) 🌳
- **SQLParser** (쿼리 파싱) 📝

**→ 하나로 통합해서 완전한 데이터베이스로 만드는 마에스트로!**

## 🏢 실생활 비유: IT 회사 CEO

### CEO가 하는 일과 비슷

**회사 구조:**

```
CEO (Database)
├── CTO (SQLParser) - 기술 명세서 해석
├── 개발팀장 (BTree) - 빠른 데이터 검색
├── 인프라팀장 (BufferPoolManager) - 서버 메모리 관리
├── DB관리자 (StorageManager) - 데이터 백업/복구
└── 데이터팀 (Record) - 데이터 구조 설계
```

**CEO의 역할:**

1. **요청 접수**: "사용자 정보 조회해줘" (SQL 쿼리)
2. **업무 분담**: 각 팀에게 적절한 업무 배정
3. **결과 취합**: 각 팀 결과를 모아서 최종 답변
4. **품질 관리**: 에러 처리, 성능 최적화

## 🎯 웹 개발자에게 친숙한 비교

### 1. Express.js 애플리케이션과 비슷한 구조

**Express.js 서버:**

```javascript
const express = require("express");
const app = express();

// 미들웨어들 (Database의 컴포넌트들과 비슷)
app.use(bodyParser.json()); // SQLParser와 비슷
app.use(cors()); // 보안 처리
app.use(helmet()); // 에러 처리
app.use(morgan("combined")); // 로깅

// 라우팅 (Database의 ExecuteQuery와 비슷)
app.get("/api/users", getUsersController);
app.post("/api/users", createUserController);
```

**Database 클래스:**

```cpp
class Database {
    SQLParser* parser_;           // bodyParser와 비슷
    BufferPoolManager* buffer_;   // 캐시 미들웨어
    StorageManager* storage_;     // 데이터베이스 커넥션
    BTree* index_;               // 검색 최적화

    bool ExecuteQuery(sql);      // 라우터 역할
};
```

### 2. React 애플리케이션의 App 컴포넌트

**React App.js:**

```javascript
function App() {
  return (
    <Router>
      {" "}
      {/* SQLParser - URL 파싱 */}
      <Header /> {/* UI 컴포넌트 */}
      <Routes>
        <Route path="/users" element={<UserList />} />
        <Route path="/users/:id" element={<UserDetail />} />
      </Routes>
      <Footer />
    </Router>
  );
}
```

**Database 클래스도 비슷한 역할:**

```cpp
bool Database::ExecuteQuery(const std::string& sql) {
    auto query = parser_->Parse(sql);        // 라우팅

    switch (query->type) {
        case QueryType::SELECT:
            return ExecuteSelect(*query);    // UserList 컴포넌트
        case QueryType::INSERT:
            return ExecuteInsert(*query);    // UserCreate 컴포넌트
        case QueryType::CREATE_TABLE:
            return ExecuteCreateTable(*query); // TableSetup 컴포넌트
    }
}
```

## 🔧 주요 기능들 상세 분석

### 1. 생성자 - 시스템 초기화

```cpp
Database::Database(const std::string& db_file) {
    storage_manager_ = std::make_unique<StorageManager>(db_file);
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(50, storage_manager_.get());
    parser_ = std::make_unique<SQLParser>();
}
```

**Next.js 애플리케이션 초기화와 비슷:**

```javascript
// _app.js 또는 layout.js
export default function RootLayout({ children }) {
  // 전역 상태 초기화
  const queryClient = new QueryClient(); // BufferPool과 비슷
  const [store] = useState(() => createStore()); // 상태 관리

  return (
    <QueryClientProvider client={queryClient}>
      {" "}
      {/* 캐시 제공 */}
      <Provider store={store}>
        {" "}
        {/* 상태 제공 */}
        <DatabaseProvider>
          {" "}
          {/* DB 커넥션 */}
          {children}
        </DatabaseProvider>
      </Provider>
    </QueryClientProvider>
  );
}
```

### 2. ExecuteQuery() - 중앙 라우터

```cpp
bool Database::ExecuteQuery(const std::string& sql) {
    auto query = parser_->Parse(sql);  // 1. 파싱
    if (!query) return false;          // 2. 검증

    last_results_.clear();             // 3. 이전 결과 초기화

    switch (query->type) {             // 4. 타입별 라우팅
        case QueryType::SELECT:   return ExecuteSelect(*query);
        case QueryType::INSERT:   return ExecuteInsert(*query);
        case QueryType::CREATE_TABLE: return ExecuteCreateTable(*query);
    }
}
```

**tRPC 라우터와 매우 비슷:**

```javascript
export const appRouter = router({
  // SELECT와 비슷
  getUsers: publicProcedure
    .input(z.object({ filter: z.string().optional() }))
    .query(async ({ input }) => {
      const users = await db.user.findMany({
        where: input.filter
          ? {
              name: { contains: input.filter },
            }
          : undefined,
      });
      return users;
    }),

  // INSERT와 비슷
  createUser: publicProcedure
    .input(z.object({ name: z.string(), age: z.number() }))
    .mutation(async ({ input }) => {
      return await db.user.create({
        data: input,
      });
    }),
});
```

### 3. ExecuteSelect() - 복잡한 쿼리 최적화

```cpp
bool Database::ExecuteSelect(const Query& query) {
    Table& table = *table_it->second;

    if (query.conditions.empty()) {
        // 전체 검색 - RangeScan 사용
        auto all_records = table.index->RangeScan(INT_MIN, INT_MAX);
    } else {
        // 조건부 검색 - 인덱스 활용
        for (const auto& condition : query.conditions) {
            if (condition.column == "id" && condition.op == "=") {
                // O(log n) 검색
                table.index->Search(key, record);
            }
        }
    }
}
```

**MongoDB Aggregation Pipeline과 비슷:**

```javascript
// Database의 ExecuteSelect와 같은 최적화
async function executeSelect(query) {
  const pipeline = [];

  // WHERE 조건이 없으면 전체 스캔
  if (!query.conditions.length) {
    pipeline.push({ $match: {} });
  } else {
    // 인덱스 활용 가능한 조건 우선 처리
    const indexCondition = query.conditions.find(
      (cond) => cond.column === "_id" && cond.op === "="
    );

    if (indexCondition) {
      // O(1) 검색 (인덱스 활용)
      return await User.findById(indexCondition.value);
    } else {
      // 필터링 조건들
      const matchStage = {};
      query.conditions.forEach((cond) => {
        matchStage[cond.column] = { [`$${cond.op}`]: cond.value };
      });
      pipeline.push({ $match: matchStage });
    }
  }

  return await User.aggregate(pipeline);
}
```

### 4. ExecuteInsert() - 데이터 삽입

```cpp
bool Database::ExecuteInsert(const Query& query) {
    Table& table = *table_it->second;

    // 1. 컬럼 수 검증
    if (query.values.size() != table.columns.size()) {
        return false;
    }

    // 2. Record 생성
    Record record(query.values);

    // 3. 첫 번째 값을 키로 사용 (Primary Key)
    int key = std::get<int>(query.values[0]);

    // 4. BTree에 삽입
    return table.index->Insert(key, record);
}
```

**Prisma ORM과 비슷한 검증 과정:**

```javascript
// Database의 ExecuteInsert와 같은 검증
async function executeInsert(data) {
  // 1. 스키마 검증 (컬럼 수 확인과 같음)
  const validatedData = userCreateSchema.parse(data);

  // 2. 중복 키 검증
  const existingUser = await User.findUnique({
    where: { id: validatedData.id },
  });

  if (existingUser) {
    throw new Error("User already exists");
  }

  // 3. 데이터 삽입
  return await User.create({
    data: validatedData,
  });
}

const userCreateSchema = z.object({
  id: z.number(),
  name: z.string(),
  age: z.number(),
});
```

### 5. EvaluateCondition() - WHERE 조건 평가

```cpp
bool Database::EvaluateCondition(const Record& record, const Condition& condition, const Table& table) {
    int column_index = GetColumnIndex(condition.column, table);
    Value record_value = GetRecordValue(record, column_index);

    if (condition.op == "=") {
        return record_value == condition.value;
    } else if (condition.op == ">") {
        if (std::holds_alternative<int>(record_value)) {
            return std::get<int>(record_value) > std::get<int>(condition.value);
        }
    }
    // ... 다른 연산자들
}
```

**Lodash의 filter 함수와 비슷:**

```javascript
function evaluateCondition(record, condition) {
  const recordValue = record[condition.column];
  const conditionValue = condition.value;

  switch (condition.op) {
    case "=":
      return recordValue === conditionValue;
    case ">":
      return recordValue > conditionValue;
    case "<":
      return recordValue < conditionValue;
    default:
      return false;
  }
}

// 사용 예
const users = [
  { id: 1, name: "Alice", age: 25 },
  { id: 2, name: "Bob", age: 30 },
];

const filtered = users.filter((user) =>
  evaluateCondition(user, { column: "age", op: ">", value: 27 })
);
```

## 🚀 전체 시스템 실행 흐름

### 실제 쿼리 실행 과정

**SQL 쿼리:** `SELECT * FROM users WHERE age > 25`

**1단계: 파싱 (SQLParser)**

```
"SELECT * FROM users WHERE age > 25"
↓
Query {
  type: SELECT,
  table_name: "users",
  columns: ["*"],
  conditions: [{column: "age", op: ">", value: 25}]
}
```

**2단계: 테이블 찾기 (Database)**

```cpp
auto table_it = tables_.find("users");  // HashMap O(1) 검색
Table& table = *table_it->second;
```

**3단계: 데이터 검색 (BTree)**

```cpp
// 조건에 따라 다른 전략 선택
if (has_id_condition) {
    // O(log n) 단건 검색
    table.index->Search(key, record);
} else {
    // O(n) 전체 스캔 (하지만 인덱스 순서로)
    auto all_records = table.index->RangeScan(INT_MIN, INT_MAX);
}
```

**4단계: 조건 평가 (Database)**

```cpp
for (const auto& record : records) {
    bool matches = true;
    for (const auto& condition : query.conditions) {
        if (!EvaluateCondition(record, condition, table)) {
            matches = false; // age <= 25인 경우 제외
            break;
        }
    }
    if (matches) {
        last_results_.push_back(record); // 결과에 추가
    }
}
```

**5단계: 결과 반환**

```cpp
return true; // 성공
// 사용자는 db.GetLastResults()로 결과 조회
```

### 웹 애플리케이션 요청 흐름과 비교

**웹 요청:** `GET /api/users?age_gt=25`

**1단계: 라우팅 (Express Router)**

```javascript
app.get('/api/users', async (req, res) => {
  const { age_gt } = req.query; // 파라미터 파싱
```

**2단계: 비즈니스 로직 (Service Layer)**

```javascript
const users = await userService.findMany({
  where: { age: { gt: parseInt(age_gt) } },
});
```

**3단계: 데이터베이스 쿼리 (ORM)**

```javascript
// Prisma가 내부적으로 SQL 생성 후 실행
// SELECT * FROM users WHERE age > 25
```

**4단계: 결과 반환**

```javascript
  res.json({ success: true, data: users });
});
```

## 🎯 성능 최적화 전략

### 1. 쿼리 최적화

**현재 구현의 최적화:**

```cpp
// ID 조건이 있으면 인덱스 활용
if (condition.column == "id" && condition.op == "=") {
    // O(log n) - BTree 검색
    table.index->Search(key, record);
} else {
    // O(n) - 전체 스캔
    auto all_records = table.index->RangeScan(INT_MIN, INT_MAX);
}
```

**웹에서 비슷한 최적화:**

```javascript
// 쿼리 최적화 미들웨어
function optimizeQuery(query) {
  // 1. 인덱스가 있는 컬럼 우선 활용
  if (query.where.id) {
    return { strategy: "index", field: "id" };
  }

  // 2. 복합 인덱스 활용 가능성 체크
  if (query.where.userId && query.where.createdAt) {
    return { strategy: "composite", fields: ["userId", "createdAt"] };
  }

  // 3. 전체 스캔 필요
  return { strategy: "full_scan" };
}
```

### 2. 캐시 활용

**BufferPool 자동 캐싱:**

```cpp
// BTree 검색 시 자동으로 캐시됨
Page* page = buffer_pool_manager_->FetchPage(page_id);
// 이후 동일 페이지 접근시 메모리에서 바로 반환
```

**웹에서 비슷한 캐시 전략:**

```javascript
// React Query 캐시 전략
const { data: users } = useQuery(
  ["users", { age_gt: 25 }],
  () => fetchUsers({ age_gt: 25 }),
  {
    staleTime: 5 * 60 * 1000, // 5분간 fresh
    cacheTime: 10 * 60 * 1000, // 10분간 캐시 보존
  }
);
```

### 3. 메모리 관리

**RAII 패턴 사용:**

```cpp
// 자동 메모리 관리
std::unique_ptr<StorageManager> storage_manager_;
std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
// 소멸자에서 자동으로 정리됨
```

## 🔧 실전 활용 예시

### 1. main.cpp에서 실제 사용법

```cpp
Database db("demo.db");

// 1. 테이블 생성
db.ExecuteQuery("CREATE TABLE users (id INT, name VARCHAR, age INT)");

// 2. 데이터 삽입
db.ExecuteQuery("INSERT INTO users VALUES (1, 'Alice', 25)");

// 3. 데이터 조회
if (db.ExecuteQuery("SELECT * FROM users WHERE id = 1")) {
    auto results = db.GetLastResults();
    for (const auto& record : results) {
        std::cout << record.ToString() << std::endl;
    }
}
```

### 2. 웹에서 비슷한 사용법

```javascript
// 데이터베이스 초기화
const db = new DatabaseClient("postgresql://localhost:5432/mydb");

// 1. 테이블 생성 (마이그레이션)
await db.execute(`
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INTEGER
  )
`);

// 2. 데이터 삽입
await db.execute(`
  INSERT INTO users (name, age) VALUES ('Alice', 25)
`);

// 3. 데이터 조회
const users = await db.query(
  `
  SELECT * FROM users WHERE age > $1
`,
  [25]
);

console.log(users);
```

## 🎯 확장 가능성

### 1. 트랜잭션 지원

**현재 미구현, 하지만 추가 가능:**

```cpp
class Database {
    // 트랜잭션 기능 추가
    bool BeginTransaction();
    bool CommitTransaction();
    bool RollbackTransaction();

private:
    bool in_transaction_{false};
    std::vector<Operation> transaction_log_;
};
```

### 2. 다중 인덱스 지원

**현재는 ID만 인덱스, 확장 가능:**

```cpp
struct Table {
    std::string name;
    std::vector<Column> columns;

    // 다중 인덱스 지원
    std::unordered_map<std::string, std::unique_ptr<BTree>> indexes;
    // indexes["id"] = primary_index
    // indexes["name"] = secondary_index
};
```

### 3. 조인 연산 지원

**현재 단일 테이블만, 확장 가능:**

```cpp
struct Query {
    // ... 기존 필드들
    std::vector<std::string> join_tables;
    std::vector<JoinCondition> join_conditions;
};

bool Database::ExecuteJoin(const Query& query) {
    // 중첩 루프 조인 또는 해시 조인 구현
}
```

## 💡 핵심 개념 정리

### 1. 시스템 아키텍처

- **레이어드 아키텍처**: 각 컴포넌트가 명확한 책임
- **의존성 주입**: 생성자에서 필요한 의존성 주입
- **RAII 패턴**: 자동 자원 관리
- **단일 책임 원칙**: 각 클래스가 하나의 목적

### 2. 웹 개발자가 배울 점

- **시스템 통합 설계**: 여러 모듈을 하나로 조합하는 방법
- **성능 최적화**: 인덱스, 캐시, 쿼리 최적화 전략
- **에러 처리**: 시스템 전체에서 일관된 에러 처리
- **확장성 설계**: 기능 추가가 용이한 구조

### 3. 실무 응용

- 마이크로서비스 아키텍처 설계
- API 게이트웨이 패턴 구현
- 대용량 데이터 처리 시스템
- 실시간 데이터 파이프라인

## 📍 관련 파일 위치

- `src/database.h:15-38` - Database 클래스 정의와 Table 구조체
- `src/database.cpp:10-30` - 중앙 쿼리 라우터 (ExecuteQuery)
- `src/database.cpp:32-78` - SELECT 쿼리 실행 최적화
- `src/database.cpp:118-145` - WHERE 조건 평가 로직
- `src/main.cpp:4-47` - 전체 시스템 사용 예시

## 🎉 축하합니다!

7단계: Database 전체시스템 학습 가이드를 만들었어요!

✨ 전체 학습 여정 완료!

이제 SimpleDB 데이터베이스 엔진을 완전히 이해하셨습니다:

🏗️ 아키텍처 전체 그림

사용자 SQL → Database → SQLParser → Query
↓
BTree ← BufferPool ← StorageManager ← Page
↓
최종 결과 반환

📚 7단계 학습 완료

1. ✅ Page: 4KB 저장 단위 기초
2. ✅ StorageManager: 디스크 I/O 관리
3. ✅ BufferPoolManager: LRU 메모리 캐시
4. ✅ Record: 다양한 데이터 타입 처리
5. ✅ BTree: O(log n) 초고속 검색
6. ✅ SQLParser: 쿼리 파싱과 AST 생성
7. ✅ Database: 모든 컴포넌트 통합 지휘

💪 습득한 핵심 능력

웹 개발자로서 얻은 것:

- 📈 성능 최적화 마인드: 캐시, 인덱스, 알고리즘 복잡도 이해
- 🏗️ 시스템 아키텍처 설계: 레이어드 아키텍처, 의존성 관리
- 🔧 저수준 동작 원리: 메모리, 디스크 I/O, 자료구조
- 🚀 실무 응용 아이디어: GraphQL, MongoDB, Prisma 내부 동작 이해

이제 MySQL, PostgreSQL, MongoDB 같은 데이터베이스들이 내부적으로 어떻게 작동하는지,
그리고 여러분의 웹 애플리케이션을 어떻게 최적화해야 하는지 완전히 이해하게 되셨어요!

**SimpleDB 전체 시스템 학습 완료!**

이제 여러분은:
✅ **데이터베이스 내부 구조** 완전 이해
✅ **페이지 기반 저장소** 개념 습득
✅ **버퍼 풀 관리 원리** 학습
✅ **B-tree 인덱싱 구조** 마스터
✅ **SQL 쿼리 실행 과정** 파악
✅ **시스템 통합 아키텍처** 설계 능력

웹 개발에서 만나는 모든 데이터베이스가 내부적으로 어떻게 동작하는지, 그리고 어떻게 최적화해야 하는지 이해하게 되셨습니다! 🚀
