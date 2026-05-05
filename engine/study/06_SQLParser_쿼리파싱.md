# 6단계: SQLParser - 사람 언어를 기계어로 번역하는 똑똑한 통역사 📝

## 🗣️ SQLParser = 구글 번역기 + 컴파일러

### SQLParser가 하는 일

웹 개발할 때 이런 경험 있으시죠?
```sql
SELECT name, age FROM users WHERE age > 25
```

**사람이 읽기 쉬운 SQL** → **컴퓨터가 실행할 수 있는 명령어**로 변환

SQLParser = **"SQL 문장을 분해해서 컴퓨터가 이해할 수 있는 구조로 만드는 통역사"**

## 🧩 웹 개발자에게 친숙한 비유

### 1. Babel 트랜스파일러와 비슷한 역할
```javascript
// 최신 JavaScript (사람이 쓰기 편한 코드)
const users = await fetch('/api/users');
const adults = users.filter(u => u.age > 18);

// ↓ Babel이 변환 ↓

// 구형 브라우저용 JavaScript (기계가 이해하는 코드)
var users = fetch('/api/users');
var adults = users.filter(function(u) { return u.age > 18; });
```

**SQLParser도 같은 역할:**
```sql
SELECT name FROM users WHERE age > 25
```
**↓ SQLParser가 변환 ↓**
```cpp
Query {
  type: SELECT,
  columns: ["name"],
  table_name: "users",
  conditions: [{column: "age", op: ">", value: 25}]
}
```

### 2. Express.js 라우터 파싱과 비슷
```javascript
// Express.js에서 URL 파싱
app.get('/api/users/:id', (req, res) => {
  // URL: "/api/users/123"
  // → req.params = {id: "123"}
});

// SQLParser에서 쿼리 파싱
parser.parse("SELECT * FROM users WHERE id = 123")
// → Query {table: "users", conditions: [{column: "id", value: 123}]}
```

## 🔧 주요 기능들 상세 분석

### 1. Tokenize() - 문장을 단어로 나누기

**자연어 처리와 똑같은 원리:**
```cpp
std::vector<std::string> SQLParser::Tokenize(const std::string& sql)
```

**동작 과정:**
```sql
"SELECT name, age FROM users WHERE age > 25"
```
**↓ 토큰화 ↓**
```cpp
["SELECT", "name", ",", "age", "FROM", "users", "WHERE", "age", ">", "25"]
```

**웹 개발에서 비슷한 작업:**
```javascript
// URL 파싱
const url = "/api/users/123/posts?sort=date&limit=10";
const parts = url.split(/[/?&=]/);
// ["api", "users", "123", "posts", "sort", "date", "limit", "10"]

// CSS 파싱
const css = "color: red; font-size: 16px";
const properties = css.split(';').map(p => p.split(':'));
// [["color", "red"], ["font-size", "16px"]]
```

**특별한 처리가 필요한 경우들:**
```cpp
// 1. 문자열 처리 ('Hello World' → 하나의 토큰)
if (c == '\'' && !in_string) {
    in_string = true; // 문자열 시작
}

// 2. 특수 문자 처리 (괄호, 콤마, 세미콜론)
else if (c == '(' || c == ')' || c == ',' || c == ';') {
    tokens.push_back(std::string(1, c)); // 각각 별도 토큰
}
```

### 2. Query 구조체 - AST(추상 구문 트리)
```cpp
struct Query {
    QueryType type;              // SELECT, INSERT, CREATE_TABLE
    std::string table_name;      // 테이블 이름
    std::vector<std::string> columns;    // 컬럼들
    std::vector<Value> values;           // 값들
    std::vector<Condition> conditions;   // WHERE 조건들
};
```

**React JSX의 Virtual DOM과 비슷:**
```javascript
// JSX (사람이 쓰기 편한 문법)
<div className="user-card">
  <h1>{user.name}</h1>
  <p>{user.age} years old</p>
</div>

// Virtual DOM (기계가 처리하는 구조)
{
  type: 'div',
  props: { className: 'user-card' },
  children: [
    { type: 'h1', children: [user.name] },
    { type: 'p', children: [`${user.age} years old`] }
  ]
}
```

**GraphQL AST와도 비슷:**
```javascript
// GraphQL 쿼리
query {
  user(id: "123") {
    name
    age
  }
}

// AST 구조
{
  kind: 'OperationDefinition',
  operation: 'query',
  selectionSet: {
    selections: [{
      kind: 'Field',
      name: { value: 'user' },
      arguments: [{ name: 'id', value: '123' }],
      selectionSet: {
        selections: [
          { kind: 'Field', name: { value: 'name' } },
          { kind: 'Field', name: { value: 'age' } }
        ]
      }
    }]
  }
}
```

### 3. ParseSelect() - SELECT 쿼리 분석

```cpp
std::unique_ptr<Query> SQLParser::ParseSelect(const std::vector<std::string>& tokens)
```

**파싱 과정:**
```sql
SELECT name, age FROM users WHERE age > 25 AND status = 'active'
```

**단계별 분석:**
```cpp
// 1. 컬럼 추출 (FROM 전까지)
while (i < tokens.size() && ToUpper(tokens[i]) != "FROM") {
    if (tokens[i] != ",") {
        query->columns.push_back(tokens[i]); // ["name", "age"]
    }
}

// 2. 테이블 이름 추출
if (ToUpper(tokens[i]) == "FROM") {
    query->table_name = tokens[i+1]; // "users"
}

// 3. WHERE 조건 추출
if (ToUpper(tokens[i]) == "WHERE") {
    // age > 25 → {column: "age", op: ">", value: 25}
    // status = 'active' → {column: "status", op: "=", value: "active"}
}
```

**웹 개발에서 비슷한 파싱:**
```javascript
// URL 쿼리 파라미터 파싱
function parseQueryString(url) {
  const query = {};
  const searchParams = new URLSearchParams(url.split('?')[1]);

  for (let [key, value] of searchParams) {
    query[key] = value;
  }

  return query;
}

// "/api/users?age_gt=25&status=active"
// → {age_gt: "25", status: "active"}
```

### 4. ParseValue() - 타입 추론과 변환

```cpp
Value SQLParser::ParseValue(const std::string& str)
```

**TypeScript의 타입 추론과 비슷:**
```cpp
// 1. 숫자 판별
if (IsNumber(str)) {
    if (str.find('.') != std::string::npos) {
        return std::stod(str); // double 타입
    } else {
        return std::stoi(str); // int 타입
    }
}

// 2. 문자열 판별 ('hello' → "hello")
if (str.front() == '\'' && str.back() == '\'') {
    return str.substr(1, str.length() - 2); // 따옴표 제거
}
```

**JavaScript의 자동 타입 변환과 비교:**
```javascript
// JavaScript는 런타임에 자동 변환
const value1 = "123" + 0;    // "1230" (문자열)
const value2 = "123" * 1;    // 123 (숫자)

// SQLParser는 파싱 타임에 명시적 변환
parseValue("123")   // int(123)
parseValue("12.5")  // double(12.5)
parseValue("'hello'") // string("hello")
```

## 🎯 각 쿼리 타입별 상세 분석

### 1. SELECT 쿼리 파싱
```sql
SELECT name, age FROM users WHERE age > 25 AND status = 'active'
```

**파싱 결과:**
```cpp
Query {
    type: QueryType::SELECT,
    table_name: "users",
    columns: ["name", "age"],
    conditions: [
        {column: "age", op: ">", value: 25},
        {column: "status", op: "=", value: "active"}
    ]
}
```

**React Query와 비슷한 구조:**
```javascript
const queryConfig = {
  queryKey: ['users'],
  queryFn: () => fetchUsers({
    select: ['name', 'age'],
    where: [
      {field: 'age', operator: '>', value: 25},
      {field: 'status', operator: '=', value: 'active'}
    ]
  })
};
```

### 2. INSERT 쿼리 파싱
```sql
INSERT INTO users VALUES (123, 'John', 25)
```

**파싱 과정:**
```cpp
// 1. INTO 다음에 테이블 이름
if (ToUpper(tokens[i]) == "INTO") {
    query->table_name = tokens[i+1]; // "users"
}

// 2. VALUES 다음에 괄호 안의 값들
if (ToUpper(tokens[i]) == "VALUES" && tokens[i+1] == "(") {
    // (123, 'John', 25) → [123, "John", 25]
}
```

**웹에서 비슷한 데이터 처리:**
```javascript
// Form 데이터 파싱
const formData = "name=John&age=25&email=john@example.com";
const values = formData.split('&').map(pair => {
  const [key, value] = pair.split('=');
  return decodeURIComponent(value);
});
```

### 3. CREATE TABLE 쿼리 파싱
```sql
CREATE TABLE users (id INT, name VARCHAR, age INT)
```

**파싱 결과:**
```cpp
Query {
    type: QueryType::CREATE_TABLE,
    table_name: "users",
    table_columns: [
        {name: "id", type: "INT"},
        {name: "name", type: "VARCHAR"},
        {name: "age", type: "INT"}
    ]
}
```

**TypeScript 인터페이스 정의와 비슷:**
```typescript
// SQL CREATE TABLE과 비슷한 개념
interface Users {
  id: number;      // INT
  name: string;    // VARCHAR
  age: number;     // INT
}

// Prisma 스키마와도 비슷
model User {
  id   Int    @id
  name String
  age  Int
}
```

## 💡 컴파일러 이론 기초

### 1. 컴파일 과정의 단계
```
SQL 문자열 → 토큰화 → 파싱 → AST → 실행
    ↓          ↓        ↓      ↓     ↓
"SELECT *"  ["SELECT"] Query{} 최적화 BTree검색
```

**웹팩 빌드 과정과 비슷:**
```
JSX → 토큰화 → 파싱 → AST → 트랜스파일 → JS
 ↓      ↓        ↓      ↓        ↓        ↓
<div> ["<","div"] Node{} React.createElement() 브라우저실행
```

### 2. 파싱 패턴들

**재귀하강파서 (Recursive Descent Parser) 개념:**
```cpp
// 각 문법 요소별로 함수 분리
ParseSelect()     // SELECT 문 파싱
ParseInsert()     // INSERT 문 파싱
ParseCreateTable() // CREATE TABLE 문 파싱
```

**웹에서 비슷한 패턴:**
```javascript
// JSON 파서 구현
class JSONParser {
  parseValue() {
    if (this.current === '{') return this.parseObject();
    if (this.current === '[') return this.parseArray();
    if (this.current === '"') return this.parseString();
    if (this.isNumber()) return this.parseNumber();
  }

  parseObject() { /* {...} 파싱 */ }
  parseArray()  { /* [...] 파싱 */ }
  parseString() { /* "..." 파싱 */ }
}
```

## 🚀 실제 사용 예시와 성능

### 1. 토큰화 성능 최적화
```cpp
// 현재 구현: O(n) 시간복잡도
for (size_t i = 0; i < sql.length(); ++i) {
    char c = sql[i];
    // 문자 하나씩 처리
}
```

**웹에서 비슷한 최적화:**
```javascript
// 정규표현식 활용한 토큰화 (더 빠름)
function tokenize(sql) {
  const tokens = sql.match(/[a-zA-Z_][a-zA-Z0-9_]*|'[^']*'|\d+\.?\d*|[(),;]|\S/g);
  return tokens || [];
}

// 스트림 처리 (대용량 데이터)
function* tokenizeStream(sql) {
  let current = '';
  for (const char of sql) {
    if (isDelimiter(char)) {
      if (current) yield current;
      yield char;
      current = '';
    } else {
      current += char;
    }
  }
}
```

### 2. 에러 처리와 사용자 친화성
```cpp
// 현재는 단순히 nullptr 반환
if (tokens.empty()) {
    return nullptr; // 에러 정보가 부족
}
```

**웹에서 좋은 에러 처리 예시:**
```javascript
class SQLParseError extends Error {
  constructor(message, position, token) {
    super(message);
    this.position = position;
    this.token = token;
  }
}

function parse(sql) {
  try {
    return parseSQL(sql);
  } catch (error) {
    throw new SQLParseError(
      `Unexpected token '${error.token}' at position ${error.position}`,
      error.position,
      error.token
    );
  }
}
```

## 🎯 웹 개발에서의 응용

### 1. GraphQL 쿼리 빌더
```javascript
// SQL Parser 개념을 GraphQL에 응용
class GraphQLQueryBuilder {
  constructor() {
    this.query = { selections: [] };
  }

  select(fields) {
    this.query.selections = fields.map(field => ({
      kind: 'Field',
      name: { value: field }
    }));
    return this;
  }

  where(conditions) {
    this.query.arguments = conditions.map(cond => ({
      name: cond.field,
      value: cond.value
    }));
    return this;
  }
}

// 사용법
const query = new GraphQLQueryBuilder()
  .select(['name', 'age'])
  .where([{field: 'age_gt', value: 25}])
  .build();
```

### 2. NoSQL 쿼리 빌더
```javascript
// MongoDB 쿼리 빌더
class MongoQueryBuilder {
  constructor() {
    this.pipeline = [];
  }

  match(conditions) {
    const matchStage = { $match: {} };
    conditions.forEach(cond => {
      matchStage.$match[cond.column] = {
        [`$${cond.op}`]: cond.value
      };
    });
    this.pipeline.push(matchStage);
    return this;
  }

  select(fields) {
    const projectStage = { $project: {} };
    fields.forEach(field => {
      projectStage.$project[field] = 1;
    });
    this.pipeline.push(projectStage);
    return this;
  }
}
```

### 3. 실시간 쿼리 검증
```javascript
// 타이핑하는 동안 실시간 SQL 검증
class SQLEditor {
  constructor() {
    this.parser = new SQLParser();
  }

  onType(sql) {
    try {
      const query = this.parser.parse(sql);
      this.showSuggestions(query);
      this.highlightSyntax(sql);
    } catch (error) {
      this.showError(error.message, error.position);
    }
  }

  showSuggestions(query) {
    if (query.type === 'SELECT' && !query.table_name) {
      this.suggest(['FROM users', 'FROM products', 'FROM orders']);
    }
  }
}
```

## 🔍 확장 가능성과 개선사항

### 1. 더 복잡한 SQL 지원
```sql
-- 현재 미지원하지만 추가 가능한 기능들
SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id
HAVING COUNT(o.id) > 5
ORDER BY order_count DESC
LIMIT 10;
```

### 2. 쿼리 최적화 힌트
```cpp
struct QueryOptimization {
    bool use_index{true};
    std::string preferred_index;
    int estimated_rows{-1};
};

struct Query {
    // ... 기존 필드들
    QueryOptimization optimization;
};
```

## 💻 핵심 개념 정리

### 1. 파서의 핵심 원리
- **토큰화**: 문자열을 의미 있는 단위로 분할
- **구문 분석**: 토큰들을 구조화된 데이터로 변환
- **의미 분석**: 문법적으로 올바른지 검증
- **AST 생성**: 실행 가능한 구조체 생성

### 2. 웹 개발자가 배울 점
- **컴파일러 이론 기초**: Babel, TypeScript, 웹팩의 동작 원리 이해
- **문자열 파싱 기법**: URL, JSON, XML 파싱 능력 향상
- **AST 활용법**: 코드 변환, 분석 도구 개발
- **에러 처리**: 사용자 친화적인 에러 메시지 설계

### 3. 실무에서 활용
- GraphQL 쿼리 빌더 개발
- 커스텀 DSL(Domain Specific Language) 설계
- 코드 생성기, 템플릿 엔진 개발
- API 쿼리 최적화 도구

## 📍 관련 파일 위치
- `src/sql_parser.h:7-51` - Query 구조체와 SQLParser 클래스 정의
- `src/sql_parser.cpp:25-68` - Tokenize 구현 (문자열 파싱)
- `src/sql_parser.cpp:70-110` - ParseSelect 구현 (SELECT 쿼리 파싱)
- `src/sql_parser.cpp:178-212` - 타입 추론과 값 변환 로직

## ➡️ 다음 단계
Database 클래스를 보면 이 파싱된 쿼리가 어떻게 실제로 실행되는지 알 수 있어요!