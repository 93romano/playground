# Node.js 실습 가이드

## 1. Node.js 기초

### 설치 확인
```bash
node --version
npm --version
```

### 프로젝트 초기화
```bash
npm init -y
```

## 2. 모듈 시스템

### CommonJS
```javascript
// math.js
exports.add = (a, b) => a + b;
exports.subtract = (a, b) => a - b;

// main.js
const math = require('./math');
console.log(math.add(5, 3));
```

### ES Modules
```javascript
// package.json에 "type": "module" 추가

// math.mjs
export const add = (a, b) => a + b;
export default class Calculator { }

// main.mjs
import { add } from './math.mjs';
import Calculator from './math.mjs';
```

## 3. 내장 모듈

### File System (fs)
```javascript
const fs = require('fs');
const fsPromises = require('fs').promises;

// 동기 방식
const data = fs.readFileSync('file.txt', 'utf-8');

// 비동기 콜백
fs.readFile('file.txt', 'utf-8', (err, data) => {
    if (err) throw err;
    console.log(data);
});

// Promise 방식
async function readFile() {
    const data = await fsPromises.readFile('file.txt', 'utf-8');
    return data;
}
```

### Path
```javascript
const path = require('path');

console.log(path.join(__dirname, 'files', 'test.txt'));
console.log(path.resolve('files', 'test.txt'));
console.log(path.basename('/users/file.txt'));
console.log(path.extname('file.txt'));
```

### HTTP
```javascript
const http = require('http');

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Hello World');
});

server.listen(3000);
```

### Events
```javascript
const EventEmitter = require('events');

class MyEmitter extends EventEmitter {}
const myEmitter = new MyEmitter();

myEmitter.on('event', (data) => {
    console.log('이벤트 발생:', data);
});

myEmitter.emit('event', { message: '안녕' });
```

### Stream
```javascript
const fs = require('fs');

// 읽기 스트림
const readStream = fs.createReadStream('input.txt');
const writeStream = fs.createWriteStream('output.txt');

// 파이프
readStream.pipe(writeStream);

// 이벤트 처리
readStream.on('data', (chunk) => {
    console.log('데이터 청크:', chunk.length);
});
```

### Process
```javascript
// 환경 변수
console.log(process.env.NODE_ENV);

// 명령줄 인자
console.log(process.argv);

// 종료
process.exit(0);

// 이벤트
process.on('exit', (code) => {
    console.log('프로세스 종료:', code);
});
```

### Child Process
```javascript
const { spawn, exec } = require('child_process');

// exec - 간단한 명령
exec('ls -la', (error, stdout, stderr) => {
    console.log(stdout);
});

// spawn - 스트리밍
const ls = spawn('ls', ['-la']);
ls.stdout.on('data', (data) => {
    console.log(data.toString());
});
```

### Crypto
```javascript
const crypto = require('crypto');

// 해시
const hash = crypto.createHash('sha256')
    .update('password')
    .digest('hex');

// 암호화
const algorithm = 'aes-256-cbc';
const key = crypto.randomBytes(32);
const iv = crypto.randomBytes(16);

const cipher = crypto.createCipheriv(algorithm, key, iv);
let encrypted = cipher.update('text', 'utf8', 'hex');
encrypted += cipher.final('hex');
```

## 4. Express.js 웹 서버

### 기본 설정
```javascript
const express = require('express');
const app = express();

// 미들웨어
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

// 라우팅
app.get('/', (req, res) => {
    res.send('Hello World');
});

app.post('/users', (req, res) => {
    const { name, email } = req.body;
    res.json({ id: 1, name, email });
});

app.listen(3000);
```

### 라우터
```javascript
// routes/users.js
const router = express.Router();

router.get('/', (req, res) => {
    res.json({ users: [] });
});

router.get('/:id', (req, res) => {
    res.json({ id: req.params.id });
});

module.exports = router;

// main.js
app.use('/users', require('./routes/users'));
```

### 미들웨어
```javascript
// 커스텀 미들웨어
const logger = (req, res, next) => {
    console.log(`${req.method} ${req.url}`);
    next();
};

app.use(logger);

// 에러 핸들링
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).send('Something broke!');
});
```

## 5. 비동기 프로그래밍

### Callback
```javascript
function fetchData(callback) {
    setTimeout(() => {
        callback(null, { data: 'result' });
    }, 1000);
}

fetchData((err, data) => {
    if (err) return console.error(err);
    console.log(data);
});
```

### Promise
```javascript
function fetchData() {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve({ data: 'result' });
        }, 1000);
    });
}

fetchData()
    .then(data => console.log(data))
    .catch(err => console.error(err));
```

### Async/Await
```javascript
async function getData() {
    try {
        const data = await fetchData();
        console.log(data);
    } catch (error) {
        console.error(error);
    }
}

// 병렬 처리
async function parallel() {
    const [result1, result2] = await Promise.all([
        fetchData1(),
        fetchData2()
    ]);
}
```

## 6. 데이터베이스 연동

### MongoDB (Mongoose)
```javascript
const mongoose = require('mongoose');

// 연결
mongoose.connect('mongodb://localhost/mydb');

// 스키마
const userSchema = new mongoose.Schema({
    name: String,
    email: { type: String, unique: true },
    age: Number
});

// 모델
const User = mongoose.model('User', userSchema);

// CRUD
const user = new User({ name: '홍길동', email: 'hong@example.com' });
await user.save();

const users = await User.find();
await User.findByIdAndUpdate(id, { age: 30 });
await User.findByIdAndDelete(id);
```

### MySQL
```javascript
const mysql = require('mysql2/promise');

const pool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'mydb'
});

async function query() {
    const [rows] = await pool.execute(
        'SELECT * FROM users WHERE age > ?',
        [25]
    );
    return rows;
}
```

## 7. 테스팅

### Jest
```javascript
// math.test.js
const { add, subtract } = require('./math');

describe('Math functions', () => {
    test('adds 1 + 2 to equal 3', () => {
        expect(add(1, 2)).toBe(3);
    });

    test('subtracts 5 - 3 to equal 2', () => {
        expect(subtract(5, 3)).toBe(2);
    });
});
```

### Supertest (API 테스팅)
```javascript
const request = require('supertest');
const app = require('./app');

describe('GET /users', () => {
    it('responds with json', async () => {
        const response = await request(app)
            .get('/users')
            .expect('Content-Type', /json/)
            .expect(200);

        expect(response.body).toHaveProperty('users');
    });
});
```

## 8. 환경 설정

### dotenv
```javascript
require('dotenv').config();

// .env 파일
// PORT=3000
// DB_HOST=localhost

console.log(process.env.PORT);
console.log(process.env.DB_HOST);
```

### 설정 파일
```javascript
// config.js
module.exports = {
    development: {
        port: 3000,
        db: 'mongodb://localhost/dev'
    },
    production: {
        port: process.env.PORT,
        db: process.env.DB_URL
    }
};

// 사용
const config = require('./config')[process.env.NODE_ENV || 'development'];
```

## 9. 보안

### Helmet
```javascript
const helmet = require('helmet');
app.use(helmet());
```

### CORS
```javascript
const cors = require('cors');

app.use(cors({
    origin: 'https://example.com',
    credentials: true
}));
```

### Rate Limiting
```javascript
const rateLimit = require('express-rate-limit');

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15분
    max: 100 // 요청 제한
});

app.use('/api', limiter);
```

### JWT
```javascript
const jwt = require('jsonwebtoken');

// 토큰 생성
const token = jwt.sign(
    { userId: user.id },
    process.env.JWT_SECRET,
    { expiresIn: '1h' }
);

// 토큰 검증
const decoded = jwt.verify(token, process.env.JWT_SECRET);
```

## 10. 성능 최적화

### Clustering
```javascript
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
} else {
    // 워커 프로세스
    require('./app');
}
```

### 캐싱
```javascript
const NodeCache = require('node-cache');
const cache = new NodeCache({ stdTTL: 600 });

function getCachedData(key) {
    let data = cache.get(key);
    if (data === undefined) {
        data = fetchFromDB();
        cache.set(key, data);
    }
    return data;
}
```

### Compression
```javascript
const compression = require('compression');
app.use(compression());
```

## 11. WebSocket

### Socket.io
```javascript
const io = require('socket.io')(server);

io.on('connection', (socket) => {
    console.log('사용자 연결');

    socket.on('message', (data) => {
        // 모든 클라이언트에게 전송
        io.emit('message', data);
    });

    socket.on('disconnect', () => {
        console.log('사용자 연결 해제');
    });
});
```

## 12. 실용적인 유틸리티

### Lodash
```javascript
const _ = require('lodash');

_.chunk(['a', 'b', 'c', 'd'], 2);
_.debounce(func, 1000);
_.cloneDeep(object);
```

### Moment.js / Day.js
```javascript
const dayjs = require('dayjs');

dayjs().format('YYYY-MM-DD');
dayjs().add(7, 'day');
dayjs().diff(date, 'days');
```

### Nodemailer
```javascript
const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: 'your-email@gmail.com',
        pass: 'your-password'
    }
});

await transporter.sendMail({
    from: 'sender@example.com',
    to: 'receiver@example.com',
    subject: 'Hello',
    text: 'Hello world'
});
```