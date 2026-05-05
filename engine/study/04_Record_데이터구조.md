# 4단계: Record 클래스 - 다양한 데이터 타입을 다루는 똑똑한 상자

## 🎁 Record = 만능 데이터 컨테이너

### Record가 하는 일

웹 개발할 때 이런 경험 있으시죠?
```javascript
// 이런 다양한 타입의 데이터를 한번에 다루기
const userInfo = {
  id: 123,           // 숫자
  name: "김개발",     // 문자열
  score: 95.5,       // 실수
  active: true       // 불린
};
```

**Record 클래스 = 이런 혼합 데이터를 안전하게 저장하는 컨테이너**

## 🧩 웹 개발자에게 친숙한 비유

### 1. TypeScript의 Union Type과 비슷
```cpp
// C++에서 Record
using Value = std::variant<int, double, std::string>;
```

```typescript
// TypeScript에서 비슷한 개념
type Value = number | string | boolean;

interface Record {
  values: Value[];
}
```

### 2. JSON 객체와 비슷한 역할
```javascript
// JSON - 웹에서 데이터 전송할 때
const record = {
  "columns": ["id", "name", "score"],
  "values": [123, "김개발", 95.5]
};

// Record 클래스도 비슷하지만 더 효율적으로 저장
```

## 🔍 핵심 기능들 (웹개발자 관점)

### 1. Variant = TypeScript Union Type
```cpp
std::variant<int, double, std::string> value;
```

**React PropTypes과 비슷한 개념:**
```javascript
// PropTypes (옛날 React)
MyComponent.propTypes = {
  value: PropTypes.oneOfType([
    PropTypes.number,
    PropTypes.string,
    PropTypes.bool
  ])
};

// TypeScript (현재)
interface Props {
  value: number | string | boolean;  // Union Type
}
```

**실제 사용 예:**
```cpp
// C++에서
Value val1 = 42;           // int 저장
Value val2 = 3.14;         // double 저장
Value val3 = "Hello";      // string 저장

// std::visit로 타입별 처리
std::visit([](const auto& v) {
    std::cout << v << std::endl;
}, val1);
```

```javascript
// JavaScript에서 비슷한 처리
function printValue(value) {
  if (typeof value === 'number') {
    console.log(`숫자: ${value}`);
  } else if (typeof value === 'string') {
    console.log(`문자열: ${value}`);
  } else {
    console.log(`기타: ${value}`);
  }
}
```

### 2. 직렬화 (Serialization) = JSON.stringify()

**웹에서 데이터 전송할 때:**
```javascript
// 브라우저 → 서버 전송
const data = { id: 123, name: "김개발", score: 95.5 };
const jsonString = JSON.stringify(data);  // 직렬화
fetch('/api/users', {
  method: 'POST',
  body: jsonString  // 문자열로 전송
});

// 서버에서 받을 때
const receivedData = JSON.parse(jsonString);  // 역직렬화
```

**Record에서 바이너리 직렬화:**
```cpp
void Record::Serialize(char* data) const {
    // 1. 데이터 개수 저장
    size_t count = values_.size();
    std::memcpy(data + offset, &count, sizeof(count));

    // 2. 각 값마다 타입 정보 + 실제 데이터 저장
    for (const auto& value : values_) {
        // 타입별로 다르게 저장
        if (int인 경우) {
            uint8_t type = 0;  // int = 타입 0
            // 타입 저장 + 실제 값 저장
        }
        else if (double인 경우) {
            uint8_t type = 1;  // double = 타입 1
        }
        else if (string인 경우) {
            uint8_t type = 2;  // string = 타입 2
            // 문자열 길이 + 실제 문자열 저장
        }
    }
}
```

### 3. 바이너리 vs JSON 비교

| 방식 | JSON (웹) | Binary (Record) |
|------|-----------|-----------------|
| **가독성** | 사람이 읽기 쉬움 | 사람이 읽기 어려움 |
| **용량** | 큰 편 | 작은 편 |
| **속도** | 느림 | 빠름 |
| **호환성** | 거의 모든 언어 | C++ 특화 |

**예시 비교:**
```javascript
// JSON 방식 (약 50바이트)
'{"id": 123, "name": "김개발", "score": 95.5}'
```

```cpp
// Binary 방식 (약 30바이트)
[3][0][123][2][6][김개발][1][95.5]
//↑   ↑  ↑   ↑ ↑     ↑   ↑   ↑
//필드수 타입 값  타입 길이  문자열 타입 값
```

## 🎯 실제 동작 과정 분석

### 1. 데이터 저장 과정
```cpp
// 1. Record 생성
Record record;
record.AddValue(123);        // int
record.AddValue("김개발");   // string
record.AddValue(95.5);       // double
```

**메모리에서의 구조:**
```
values_ = [
  variant(int: 123),
  variant(string: "김개발"),
  variant(double: 95.5)
]
```

### 2. 직렬화 과정 (디스크 저장용)
```cpp
record.Serialize(buffer);
```

**실제 바이너리 데이터:**
```
[3]          // 필드 개수
[0][123]     // 타입0(int) + 값123
[2][6][김개발] // 타입2(string) + 길이6 + "김개발"
[1][95.5]    // 타입1(double) + 값95.5
```

### 3. 역직렬화 과정 (디스크에서 읽기)
```cpp
Record loaded = Record::Deserialize(buffer, offset);
```

**단계별 복원:**
1. `[3]` 읽기 → "3개 필드가 있구나"
2. `[0][123]` 읽기 → "첫번째는 int타입, 값은 123"
3. `[2][6][김개발]` 읽기 → "두번째는 string타입, 길이 6, 값은 '김개발'"
4. `[1][95.5]` 읽기 → "세번째는 double타입, 값은 95.5"

## 💡 웹 개발에서의 응용

### 1. LocalStorage 최적화
```javascript
// 기존 방식 (비효율적)
localStorage.setItem('userData', JSON.stringify(largeObject));

// Record 개념 응용 (가상의 최적화)
class BinaryStorage {
  serialize(data) {
    // Record처럼 바이너리로 압축
    return this.toBinary(data); // 더 작은 용량
  }

  deserialize(binary) {
    return this.fromBinary(binary);
  }
}
```

### 2. 상태 관리 최적화
```javascript
// Redux에서 Record 개념 응용
const initialState = {
  users: [], // Record들의 배열
  loading: false,
  error: null
};

// 각 user가 Record 형태
const user = {
  values: [123, "김개발", 95.5], // 고정된 순서
  schema: ["id", "name", "score"] // 컬럼 정보 별도 관리
};
```

### 3. 네트워크 전송 최적화
```javascript
// GraphQL이나 REST API 응답 최적화
class CompactResponse {
  constructor(schema, records) {
    this.schema = schema;   // ["id", "name", "score"] 한번만
    this.records = records; // [[123, "김개발", 95.5], ...] 데이터만
  }

  // 전송량 50% 절약 가능!
}
```

## 🔧 std::visit의 마법 - 타입 안전성

**C++의 std::visit:**
```cpp
std::visit([](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, int>) {
        // int 처리
    } else if constexpr (std::is_same_v<T, string>) {
        // string 처리
    }
}, variant_value);
```

**JavaScript/TypeScript에서 비슷한 패턴:**
```typescript
// TypeScript 타입 가드
function processValue(value: number | string | boolean) {
  if (typeof value === 'number') {
    // 여기서 value는 자동으로 number 타입
    return value * 2;
  } else if (typeof value === 'string') {
    // 여기서 value는 자동으로 string 타입
    return value.toUpperCase();
  } else {
    // 여기서 value는 자동으로 boolean 타입
    return value ? 'true' : 'false';
  }
}
```

## 🎯 핵심 개념 정리

### 1. 왜 Record가 필요한가?
**데이터베이스의 행(Row) = 다양한 타입의 컬럼들**
- 사용자 ID: 숫자
- 이름: 문자열
- 점수: 실수
- 등록일: 날짜

**모든 타입을 안전하게 하나의 구조체에 저장 필요**

### 2. 성능상 이점
- **메모리 효율**: 필요한 만큼만 사용
- **속도**: 바이너리 직렬화로 빠른 I/O
- **타입 안전**: 컴파일 타임에 에러 체크

### 3. 웹 개발에서 배울 점
- 데이터 압축 기법
- 바이너리 처리의 장점
- 타입 안전성의 중요성
- 직렬화/역직렬화 설계

## 📍 관련 파일 위치
- `src/record.h:9-26` - Record 클래스 정의
- `src/record.cpp:29-98` - 직렬화/역직렬화 구현
- `src/record.h:7` - Value variant 타입 정의

## ➡️ 다음 단계
BTree 클래스를 보면 이 Record들을 어떻게 효율적으로 검색하고 정렬하는지 알 수 있어요!