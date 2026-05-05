# Node.js 실습 시트

## 목차
1. [개요](#개요)
2. [핵심 개념](#핵심-개념)
3. [내장 모듈](#내장-모듈)
4. [Express.js](#expressjs)
5. [실용 패턴](#실용-패턴)
6. [실습 과제](#실습-과제)
7. [테스트 코드 설명](#테스트-코드-설명)

---

## 개요

Node.js는 **Chrome V8 엔진 기반의 JavaScript 런타임**으로, 서버 사이드 애플리케이션을 JavaScript로 만들 수 있게 해준다.

### 특징
- **비동기 I/O**: 파일 읽기, DB 쿼리, 네트워크 요청을 논블로킹으로 처리
- **이벤트 루프**: 단일 스레드지만 이벤트 루프로 높은 동시성 처리
- **npm 생태계**: 세계 최대 패키지 레지스트리
- **V8 엔진**: 빠른 JavaScript 실행

### 이벤트 루프 동작 순서
```
   ┌──────────────────────────┐
   │         timers           │  ← setTimeout, setInterval
   │         (Phase 1)        │
   ├──────────────────────────┤
   │     pending callbacks    │  ← I/O 콜백
   │         (Phase 2)        │
   ├──────────────────────────┤
   │       idle, prepare      │  ← 내부용
   │         (Phase 3)        │
   ├──────────────────────────┤
   │         poll              │  ← I/O 이벤트 대기
   │         (Phase 4)        │
   ├──────────────────────────┤
   │         check             │  ← setImmediate
   │         (Phase 5)        │
   ├──────────────────────────┤
   │    close callbacks       │  ← socket.on('close')
   │         (Phase 6)        │
   └──────────────────────────┘

   * process.nextTick()은 현재 Phase 종료 후 즉시 실행
   * Promise (.then)은 nextTick 다음에 실행 (microtask)
```

### 실행 순서 예시
```javascript
console.log('1. 동기');

setTimeout(() => console.log('2. setTimeout'), 0);
setImmediate(() => console.log('3. setImmediate'));
process.nextTick(() => console.log('4. nextTick'));
Promise.resolve().then(() => console.log('5. Promise'));

console.log('6. 동기');

// 출력 순서: 1 → 6 → 4 → 5 → 2 → 3
```

---

## 핵심 개념

### 모듈 시스템

#### CommonJS (require)
```javascript
// math.js - 내보내기
function add(a, b) { return a + b; }
function multiply(a, b) { return a * b; }

module.exports = { add, multiply };

// app.js - 가져오기
const { add, multiply } = require('./math');
```

#### ES Modules (import)
```javascript
// math.mjs 또는 package.json에 "type": "module"
export function add(a, b) { return a + b; }
export default function multiply(a, b) { return a * b; }

// app.mjs
import multiply, { add } from './math.mjs';
```

### 비동기 패턴

#### 1. 콜백 (레거시)
```javascript
fs.readFile('file.txt', 'utf8', (err, data) => {
    if (err) throw err;
    console.log(data);
});
```

#### 2. Promise
```javascript
const readFilePromise = (path) => {
    return new Promise((resolve, reject) => {
        fs.readFile(path, 'utf8', (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
};

readFilePromise('file.txt')
    .then(data => console.log(data))
    .catch(err => console.error(err));
```

#### 3. async/await (현재 표준)
```javascript
async function readFile() {
    try {
        const data = await fs.promises.readFile('file.txt', 'utf8');
        console.log(data);
    } catch (err) {
        console.error(err);
    }
}
```

#### 동시 실행 패턴
```javascript
// 병렬 실행 (모두 완료 대기)
const [users, products, orders] = await Promise.all([
    fetchUsers(),
    fetchProducts(),
    fetchOrders()
]);

// 병렬 실행 (가장 빠른 것만)
const fastest = await Promise.race([
    fetchFromServer1(),
    fetchFromServer2()
]);

// 병렬 실행 (실패해도 계속)
const results = await Promise.allSettled([
    fetchUsers(),     // 성공
    fetchProducts(),  // 실패해도 OK
    fetchOrders()     // 성공
]);
// results = [{ status: 'fulfilled', value: ... }, { status: 'rejected', reason: ... }, ...]
```

---

## 내장 모듈

### fs (파일 시스템)
```javascript
const fs = require('fs').promises;

// 파일 읽기
const content = await fs.readFile('file.txt', 'utf8');

// 파일 쓰기
await fs.writeFile('file.txt', 'Hello World');

// 파일 추가
await fs.appendFile('log.txt', '새 로그\n');

// 파일 정보
const stats = await fs.stat('file.txt');
console.log(stats.size, stats.isFile(), stats.isDirectory());

// 디렉토리 조작
await fs.mkdir('new-dir', { recursive: true });
const files = await fs.readdir('.');
await fs.rename('old.txt', 'new.txt');
await fs.unlink('delete-me.txt');  // 파일 삭제
```

### path (경로)
```javascript
const path = require('path');

path.join('/users', 'kim', 'docs');       // /users/kim/docs
path.resolve('src', 'index.js');           // 절대경로 반환
path.basename('/users/kim/file.txt');      // file.txt
path.extname('file.txt');                  // .txt
path.dirname('/users/kim/file.txt');       // /users/kim
```

### http (서버)
```javascript
const http = require('http');

const server = http.createServer((req, res) => {
    // req.method: GET, POST, PUT, DELETE 등
    // req.url: 요청 경로
    // req.headers: 요청 헤더

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ message: 'OK' }));
});

server.listen(3000, () => {
    console.log('서버 시작: http://localhost:3000');
});
```

### events (이벤트 에미터)
```javascript
const { EventEmitter } = require('events');

class OrderSystem extends EventEmitter {}
const orderSystem = new OrderSystem();

// 리스너 등록
orderSystem.on('order', (data) => {
    console.log('주문 접수:', data);
});

orderSystem.on('order', (data) => {
    console.log('이메일 발송:', data.email);
});

// 이벤트 발생
orderSystem.emit('order', { id: 1, email: 'user@test.com' });

// 일회성 리스너
orderSystem.once('special', () => console.log('한번만 실행'));
```

### stream (스트림)
```javascript
const fs = require('fs');

// 큰 파일을 스트림으로 읽기 (메모리 효율적)
const readStream = fs.createReadStream('big-file.txt');
const writeStream = fs.createWriteStream('output.txt');

// 파이프로 연결
readStream.pipe(writeStream);

// 이벤트 기반 처리
readStream.on('data', (chunk) => {
    console.log('받은 데이터:', chunk.length, 'bytes');
});

readStream.on('end', () => {
    console.log('읽기 완료');
});
```

### crypto (암호화)
```javascript
const crypto = require('crypto');

// 해시
const hash = crypto.createHash('sha256').update('password').digest('hex');

// HMAC (서명)
const hmac = crypto.createHmac('sha256', 'secret').update('data').digest('hex');

// 랜덤 문자열
const token = crypto.randomBytes(32).toString('hex');

// UUID
const uuid = crypto.randomUUID();

// 대칭 암호화/복호화
const key = crypto.randomBytes(32);
const iv = crypto.randomBytes(16);

const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
let encrypted = cipher.update('비밀 메시지', 'utf8', 'hex');
encrypted += cipher.final('hex');

const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
let decrypted = decipher.update(encrypted, 'hex', 'utf8');
decrypted += decipher.final('utf8');
```

### Buffer
```javascript
// 생성
const buf = Buffer.from('Hello');
const buf2 = Buffer.alloc(10);      // 10바이트 (0으로 초기화)

// 변환
buf.toString();                       // 'Hello'
buf.toString('base64');               // 'SGVsbG8='
buf.toString('hex');                  // '48656c6c6f'

// 연결
const combined = Buffer.concat([buf, Buffer.from(' World')]);

// 비교
buf.equals(Buffer.from('Hello'));     // true
```

---

## Express.js

### 기본 구조
```javascript
const express = require('express');
const app = express();

// 미들웨어
app.use(express.json());              // JSON 파싱
app.use(express.urlencoded({ extended: true })); // URL 인코딩 파싱

// 라우트
app.get('/', (req, res) => {
    res.json({ message: 'Hello' });
});

// 서버 시작
app.listen(3000, () => console.log('서버 시작'));
```

### 라우팅
```javascript
// 기본 CRUD
app.get('/users', getUsers);           // 목록 조회
app.get('/users/:id', getUser);        // 단건 조회
app.post('/users', createUser);        // 생성
app.put('/users/:id', updateUser);     // 전체 수정
app.patch('/users/:id', patchUser);    // 부분 수정
app.delete('/users/:id', deleteUser);  // 삭제

// 파라미터 접근
app.get('/users/:id', (req, res) => {
    req.params.id;        // URL 파라미터  /users/123 → '123'
    req.query.page;       // 쿼리 파라미터 /users?page=2 → '2'
    req.body.name;        // 요청 바디 (POST/PUT)
    req.headers['authorization']; // 헤더
});
```

### 미들웨어
```javascript
// 실행 순서: 위에서 아래로

// 1. 로깅 미들웨어
app.use((req, res, next) => {
    console.log(`${req.method} ${req.url}`);
    next(); // 다음 미들웨어로
});

// 2. 인증 미들웨어
function authMiddleware(req, res, next) {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: 'Unauthorized' });

    try {
        const user = jwt.verify(token.replace('Bearer ', ''), SECRET);
        req.user = user;
        next();
    } catch {
        res.status(401).json({ error: 'Invalid token' });
    }
}

// 특정 라우트에만 적용
app.get('/profile', authMiddleware, (req, res) => {
    res.json(req.user);
});

// 3. 에러 핸들링 미들웨어 (반드시 4개 인자)
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(err.status || 500).json({
        error: err.message || 'Internal Server Error'
    });
});
```

### 라우터 분리
```javascript
// routes/users.js
const router = require('express').Router();

router.get('/', (req, res) => { /* 사용자 목록 */ });
router.get('/:id', (req, res) => { /* 사용자 조회 */ });
router.post('/', (req, res) => { /* 사용자 생성 */ });

module.exports = router;

// app.js
const userRoutes = require('./routes/users');
app.use('/api/users', userRoutes);
```

### 유용한 미들웨어 패키지
```javascript
const helmet = require('helmet');       // 보안 헤더
const cors = require('cors');           // CORS 허용
const morgan = require('morgan');       // 로깅
const compression = require('compression'); // 응답 압축
const rateLimit = require('express-rate-limit'); // Rate Limiting

app.use(helmet());
app.use(cors());
app.use(morgan('dev'));
app.use(compression());
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
```

---

## 실용 패턴

### 에러 처리 래퍼
```javascript
// async 핸들러 에러를 자동으로 catch
const asyncHandler = (fn) => (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
};

app.get('/users', asyncHandler(async (req, res) => {
    const users = await User.findAll(); // 에러 발생 시 자동으로 에러 핸들러로
    res.json(users);
}));
```

### 환경 변수 관리
```javascript
// .env 파일
// PORT=3000
// DB_HOST=localhost
// JWT_SECRET=my-secret

require('dotenv').config();

const PORT = process.env.PORT || 3000;
const DB_HOST = process.env.DB_HOST;
```

### 프로젝트 구조 (MVC)
```
project/
├── src/
│   ├── controllers/      # 요청 처리 로직
│   │   └── userController.js
│   ├── models/           # 데이터 모델
│   │   └── User.js
│   ├── routes/           # 라우트 정의
│   │   └── userRoutes.js
│   ├── middlewares/       # 미들웨어
│   │   ├── auth.js
│   │   └── errorHandler.js
│   ├── services/         # 비즈니스 로직
│   │   └── userService.js
│   └── app.js            # Express 앱 설정
├── .env
└── package.json
```

---

## 실습 과제

### 과제 1: 기본 테스트 실행
```bash
# Node.js 내장 모듈 테스트
npm test

# Express 테스트
npm run test:express

# 전체 테스트
npm run test:all
```

### 과제 2: REST API 서버 만들기
Express로 간단한 TODO API를 구현해보기:

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | /todos | 전체 조회 |
| GET | /todos/:id | 단건 조회 |
| POST | /todos | 생성 |
| PUT | /todos/:id | 수정 |
| DELETE | /todos/:id | 삭제 |

요구사항:
- 데이터는 메모리(배열)에 저장
- 입력 검증 (title 필수)
- 적절한 HTTP 상태 코드 사용 (200, 201, 404, 400)

### 과제 3: 파일 기반 로거 만들기
Node.js 내장 모듈만 사용해서:
- 로그를 파일에 기록하는 Logger 클래스
- 로그 레벨: info, warn, error
- 날짜별 로그 파일 생성 (logs/2024-01-15.log)
- 스트림을 사용한 효율적 쓰기

### 과제 4: 미들웨어 직접 구현
다음 미들웨어를 직접 만들어보기:
1. **요청 로거**: 메서드, URL, 응답시간 기록
2. **인증 검사**: JWT 토큰 확인
3. **입력 검증**: req.body 필수 필드 확인

### 과제 5: 실시간 채팅 서버
Socket.IO를 사용해서:
- 사용자 접속/퇴장 알림
- 메시지 전송/수신
- 접속자 목록 표시

---

## 테스트 코드 설명

### test.js - Node.js 내장 모듈 테스트

| 테스트 함수 | 설명 | 핵심 개념 |
|------------|------|----------|
| `testFileSystem` | 파일/디렉토리 CRUD | fs.promises, mkdir, writeFile, readFile, stat |
| `testHTTPServer` | HTTP 서버 생성/요청 | http.createServer, request, response |
| `testEventEmitter` | 이벤트 시스템 | on, emit, once, removeListener |
| `testStreams` | 스트림 처리 | Readable, Writable, Transform, pipe |
| `testCrypto` | 암호화 | hash, hmac, randomBytes, cipher/decipher |
| `testProcess` | 프로세스 정보 | env, pid, version, memoryUsage, nextTick |
| `testChildProcess` | 자식 프로세스 | spawn, stdout, stderr |
| `testTimers` | 타이머 | setTimeout, setImmediate, setInterval |
| `testBuffer` | 바이너리 데이터 | Buffer.from, alloc, concat, toString |
| `testURL` | URL 파싱 | URL 클래스, searchParams |
| `testAsyncPatterns` | 비동기 패턴 | Promise.all, race, async/await |

### express-test.js - Express.js 테스트

| 테스트 영역 | 설명 | 핵심 개념 |
|------------|------|----------|
| `runExpressTests` | REST API 전체 테스트 | GET, POST, PUT, DELETE, 쿼리 파라미터, 에러 핸들링 |
| `testMiddleware` | 미들웨어 동작 확인 | app.use, next(), 커스텀 데이터 전달 |
| `testRouter` | 라우터 분리 | express.Router(), app.use('/prefix', router) |

### 실행 방법
```bash
# Node.js 내장 모듈 테스트 (외부 의존성 없음)
npm test

# Express 테스트 (npm install 필요)
npm run test:express

# 전체
npm run test:all
```

### 주의사항
- `test.js`는 외부 패키지 없이 Node.js 내장 모듈만 사용합니다
- `express-test.js`는 express 패키지가 필요합니다 (`npm install`)
- HTTP 테스트는 포트 3333, 3456, 3457, 3458을 사용합니다
- 해당 포트가 사용 중이면 테스트가 실패할 수 있습니다
