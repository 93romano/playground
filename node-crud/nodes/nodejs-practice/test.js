const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { spawn } = require('child_process');

// 테스트 결과 추적
const testResults = {
    passed: 0,
    failed: 0,
    tests: []
};

// 테스트 헬퍼 함수
function logTest(name, passed, details = '') {
    const status = passed ? '✅' : '❌';
    console.log(`${status} ${name} ${details}`);
    testResults.tests.push({ name, passed, details });
    if (passed) testResults.passed++;
    else testResults.failed++;
}

// 1. 파일 시스템 테스트
async function testFileSystem() {
    console.log('\n📁 파일 시스템 테스트');
    const testDir = './test-files';
    const testFile = path.join(testDir, 'test.txt');

    try {
        // 디렉토리 생성
        await fs.mkdir(testDir, { recursive: true });
        logTest('디렉토리 생성', true);

        // 파일 쓰기
        const content = 'Hello, Node.js!';
        await fs.writeFile(testFile, content);
        logTest('파일 쓰기', true);

        // 파일 읽기
        const readContent = await fs.readFile(testFile, 'utf-8');
        logTest('파일 읽기', readContent === content, `내용: ${readContent}`);

        // 파일 정보
        const stats = await fs.stat(testFile);
        logTest('파일 정보 조회', stats.isFile());

        // 파일 목록
        const files = await fs.readdir(testDir);
        logTest('디렉토리 목록', files.includes('test.txt'));

        // 정리
        await fs.unlink(testFile);
        await fs.rmdir(testDir);
        logTest('파일/디렉토리 삭제', true);

    } catch (error) {
        logTest('파일 시스템 테스트', false, error.message);
    }
}

// 2. HTTP 서버 테스트
async function testHTTPServer() {
    console.log('\n🌐 HTTP 서버 테스트');

    return new Promise((resolve) => {
        const server = http.createServer((req, res) => {
            if (req.url === '/') {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Hello World' }));
            } else if (req.url === '/users') {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ users: ['Alice', 'Bob'] }));
            } else {
                res.writeHead(404);
                res.end('Not Found');
            }
        });

        const port = 3333;
        server.listen(port, () => {
            logTest('HTTP 서버 시작', true, `포트: ${port}`);

            // 클라이언트 요청 테스트
            http.get(`http://localhost:${port}/`, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    const json = JSON.parse(data);
                    logTest('GET / 요청', json.message === 'Hello World');

                    // 두 번째 요청
                    http.get(`http://localhost:${port}/users`, (res2) => {
                        let data2 = '';
                        res2.on('data', chunk => data2 += chunk);
                        res2.on('end', () => {
                            const json2 = JSON.parse(data2);
                            logTest('GET /users 요청', Array.isArray(json2.users));

                            server.close();
                            logTest('서버 종료', true);
                            resolve();
                        });
                    });
                });
            }).on('error', (err) => {
                logTest('HTTP 요청', false, err.message);
                server.close();
                resolve();
            });
        });
    });
}

// 3. 이벤트 에미터 테스트
async function testEventEmitter() {
    console.log('\n📢 이벤트 에미터 테스트');

    class CustomEmitter extends EventEmitter {}
    const emitter = new CustomEmitter();

    try {
        let eventFired = false;
        let eventData = null;

        // 이벤트 리스너 등록
        emitter.on('test', (data) => {
            eventFired = true;
            eventData = data;
        });

        // 이벤트 발생
        emitter.emit('test', { message: 'Hello Events' });
        logTest('이벤트 발생/수신', eventFired && eventData.message === 'Hello Events');

        // 일회성 이벤트
        let onceCount = 0;
        emitter.once('once-test', () => {
            onceCount++;
        });

        emitter.emit('once-test');
        emitter.emit('once-test'); // 두 번째는 무시됨
        logTest('once 이벤트', onceCount === 1);

        // 리스너 제거
        const listener = () => {};
        emitter.on('remove-test', listener);
        emitter.removeListener('remove-test', listener);
        logTest('리스너 제거', emitter.listenerCount('remove-test') === 0);

    } catch (error) {
        logTest('이벤트 에미터 테스트', false, error.message);
    }
}

// 4. 스트림 테스트
async function testStreams() {
    console.log('\n💧 스트림 테스트');
    const { Readable, Writable, Transform } = require('stream');

    try {
        // 읽기 스트림
        const readable = new Readable({
            read() {
                this.push('Hello ');
                this.push('Streams!');
                this.push(null); // 종료
            }
        });

        let readData = '';
        readable.on('data', chunk => readData += chunk);
        await new Promise(resolve => {
            readable.on('end', () => {
                logTest('읽기 스트림', readData === 'Hello Streams!');
                resolve();
            });
        });

        // Transform 스트림
        const upperCase = new Transform({
            transform(chunk, encoding, callback) {
                this.push(chunk.toString().toUpperCase());
                callback();
            }
        });

        const input = new Readable({
            read() {
                this.push('hello world');
                this.push(null);
            }
        });

        let transformedData = '';
        input.pipe(upperCase).on('data', chunk => {
            transformedData += chunk;
        });

        await new Promise(resolve => {
            upperCase.on('end', () => {
                logTest('Transform 스트림', transformedData === 'HELLO WORLD');
                resolve();
            });
        });

    } catch (error) {
        logTest('스트림 테스트', false, error.message);
    }
}

// 5. 암호화 테스트
async function testCrypto() {
    console.log('\n🔐 암호화 테스트');

    try {
        // 해시
        const password = 'myPassword123';
        const hash = crypto.createHash('sha256').update(password).digest('hex');
        logTest('SHA256 해시', hash.length === 64);

        // HMAC
        const secret = 'my-secret-key';
        const hmac = crypto.createHmac('sha256', secret).update('data').digest('hex');
        logTest('HMAC 생성', hmac.length === 64);

        // 랜덤 바이트
        const randomBytes = crypto.randomBytes(16);
        logTest('랜덤 바이트 생성', randomBytes.length === 16);

        // 대칭 암호화
        const algorithm = 'aes-256-cbc';
        const key = crypto.randomBytes(32);
        const iv = crypto.randomBytes(16);
        const text = 'Secret Message';

        const cipher = crypto.createCipheriv(algorithm, key, iv);
        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');

        const decipher = crypto.createDecipheriv(algorithm, key, iv);
        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');

        logTest('대칭 암호화/복호화', decrypted === text);

    } catch (error) {
        logTest('암호화 테스트', false, error.message);
    }
}

// 6. 프로세스 테스트
async function testProcess() {
    console.log('\n⚙️ 프로세스 테스트');

    try {
        // 환경 변수
        process.env.TEST_VAR = 'test_value';
        logTest('환경 변수', process.env.TEST_VAR === 'test_value');

        // 프로세스 정보
        logTest('프로세스 ID', typeof process.pid === 'number');
        logTest('Node 버전', process.version.startsWith('v'));
        logTest('플랫폼', ['linux', 'darwin', 'win32'].includes(process.platform));

        // 메모리 사용량
        const memUsage = process.memoryUsage();
        logTest('메모리 사용량', memUsage.heapUsed > 0);

        // nextTick
        let tickExecuted = false;
        process.nextTick(() => {
            tickExecuted = true;
        });
        await new Promise(resolve => setImmediate(() => {
            logTest('nextTick', tickExecuted);
            resolve();
        }));

    } catch (error) {
        logTest('프로세스 테스트', false, error.message);
    }
}

// 7. 자식 프로세스 테스트
async function testChildProcess() {
    console.log('\n👶 자식 프로세스 테스트');

    return new Promise((resolve) => {
        try {
            // 간단한 명령 실행
            const echo = spawn('echo', ['Hello from child process']);
            let output = '';

            echo.stdout.on('data', (data) => {
                output += data.toString();
            });

            echo.on('close', (code) => {
                logTest('자식 프로세스 실행', code === 0 && output.includes('Hello'));

                // Node.js 스크립트 실행
                const nodeScript = spawn('node', ['-e', 'console.log("Node child")']);
                let nodeOutput = '';

                nodeScript.stdout.on('data', (data) => {
                    nodeOutput += data.toString();
                });

                nodeScript.on('close', (code) => {
                    logTest('Node 스크립트 실행', code === 0 && nodeOutput.includes('Node child'));
                    resolve();
                });
            });

        } catch (error) {
            logTest('자식 프로세스 테스트', false, error.message);
            resolve();
        }
    });
}

// 8. 타이머 테스트
async function testTimers() {
    console.log('\n⏰ 타이머 테스트');

    try {
        // setTimeout
        const start = Date.now();
        await new Promise(resolve => {
            setTimeout(() => {
                const elapsed = Date.now() - start;
                logTest('setTimeout', elapsed >= 100 && elapsed < 200);
                resolve();
            }, 100);
        });

        // setImmediate
        let immediateExecuted = false;
        setImmediate(() => {
            immediateExecuted = true;
        });
        await new Promise(resolve => {
            setTimeout(() => {
                logTest('setImmediate', immediateExecuted);
                resolve();
            }, 10);
        });

        // setInterval과 clearInterval
        let intervalCount = 0;
        const interval = setInterval(() => {
            intervalCount++;
        }, 50);

        await new Promise(resolve => {
            setTimeout(() => {
                clearInterval(interval);
                logTest('setInterval', intervalCount >= 2 && intervalCount <= 4);
                resolve();
            }, 150);
        });

    } catch (error) {
        logTest('타이머 테스트', false, error.message);
    }
}

// 9. 버퍼 테스트
async function testBuffer() {
    console.log('\n📦 버퍼 테스트');

    try {
        // 버퍼 생성
        const buf1 = Buffer.from('Hello');
        logTest('문자열에서 버퍼 생성', buf1.toString() === 'Hello');

        // 버퍼 할당
        const buf2 = Buffer.alloc(10);
        logTest('버퍼 할당', buf2.length === 10);

        // 버퍼 쓰기
        buf2.write('Node.js');
        logTest('버퍼 쓰기', buf2.toString('utf8', 0, 7) === 'Node.js');

        // 버퍼 연결
        const buf3 = Buffer.concat([buf1, Buffer.from(' '), Buffer.from('World')]);
        logTest('버퍼 연결', buf3.toString() === 'Hello World');

        // Base64 인코딩
        const base64 = buf1.toString('base64');
        const decoded = Buffer.from(base64, 'base64');
        logTest('Base64 인코딩/디코딩', decoded.toString() === 'Hello');

    } catch (error) {
        logTest('버퍼 테스트', false, error.message);
    }
}

// 10. URL 파싱 테스트
async function testURL() {
    console.log('\n🔗 URL 테스트');
    const { URL } = require('url');

    try {
        const myURL = new URL('https://example.com:8080/path?name=test&age=25#section');

        logTest('프로토콜', myURL.protocol === 'https:');
        logTest('호스트', myURL.host === 'example.com:8080');
        logTest('경로', myURL.pathname === '/path');
        logTest('쿼리 파라미터', myURL.searchParams.get('name') === 'test');
        logTest('해시', myURL.hash === '#section');

        // URL 수정
        myURL.searchParams.set('name', 'updated');
        logTest('파라미터 수정', myURL.searchParams.get('name') === 'updated');

    } catch (error) {
        logTest('URL 테스트', false, error.message);
    }
}

// 11. Promise 및 async/await 테스트
async function testAsyncPatterns() {
    console.log('\n⏳ 비동기 패턴 테스트');

    try {
        // Promise 체이닝
        const result = await Promise.resolve(1)
            .then(x => x + 1)
            .then(x => x * 2);
        logTest('Promise 체이닝', result === 4);

        // Promise.all
        const [a, b, c] = await Promise.all([
            Promise.resolve(1),
            Promise.resolve(2),
            Promise.resolve(3)
        ]);
        logTest('Promise.all', a === 1 && b === 2 && c === 3);

        // Promise.race
        const winner = await Promise.race([
            new Promise(resolve => setTimeout(() => resolve('slow'), 100)),
            Promise.resolve('fast')
        ]);
        logTest('Promise.race', winner === 'fast');

        // async/await 에러 처리
        let errorCaught = false;
        try {
            await Promise.reject(new Error('Test error'));
        } catch (error) {
            errorCaught = true;
        }
        logTest('async/await 에러 처리', errorCaught);

    } catch (error) {
        logTest('비동기 패턴 테스트', false, error.message);
    }
}

// 메인 실행 함수
async function runAllTests() {
    console.log('🚀 Node.js 기능 테스트 시작\n');
    console.log('Node.js 버전:', process.version);
    console.log('플랫폼:', process.platform);
    console.log('='.repeat(50));

    const tests = [
        testFileSystem,
        testHTTPServer,
        testEventEmitter,
        testStreams,
        testCrypto,
        testProcess,
        testChildProcess,
        testTimers,
        testBuffer,
        testURL,
        testAsyncPatterns
    ];

    for (const test of tests) {
        await test();
    }

    // 결과 출력
    console.log('\n' + '='.repeat(50));
    console.log('📊 테스트 결과 요약');
    console.log('='.repeat(50));
    console.log(`✅ 성공: ${testResults.passed}`);
    console.log(`❌ 실패: ${testResults.failed}`);
    console.log(`📈 성공률: ${(testResults.passed / (testResults.passed + testResults.failed) * 100).toFixed(2)}%`);

    // 실패한 테스트 목록
    const failedTests = testResults.tests.filter(t => !t.passed);
    if (failedTests.length > 0) {
        console.log('\n실패한 테스트:');
        failedTests.forEach(t => {
            console.log(`  - ${t.name}: ${t.details}`);
        });
    }
}

// 실행
runAllTests().catch(console.error);