const express = require('express');
const http = require('http');

// Express 테스트 서버
function createTestApp() {
    const app = express();

    // 미들웨어
    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));

    // 로깅 미들웨어
    app.use((req, res, next) => {
        console.log(`📝 ${new Date().toISOString()} - ${req.method} ${req.url}`);
        next();
    });

    // 라우트
    app.get('/', (req, res) => {
        res.json({ message: 'Express 서버 테스트' });
    });

    app.get('/users', (req, res) => {
        res.json({
            users: [
                { id: 1, name: '홍길동', email: 'hong@example.com' },
                { id: 2, name: '김철수', email: 'kim@example.com' }
            ]
        });
    });

    app.get('/users/:id', (req, res) => {
        const { id } = req.params;
        res.json({
            id: parseInt(id),
            name: `사용자${id}`,
            email: `user${id}@example.com`
        });
    });

    app.post('/users', (req, res) => {
        const { name, email } = req.body;
        res.status(201).json({
            id: Date.now(),
            name,
            email,
            created: new Date().toISOString()
        });
    });

    app.put('/users/:id', (req, res) => {
        const { id } = req.params;
        const { name, email } = req.body;
        res.json({
            id: parseInt(id),
            name,
            email,
            updated: new Date().toISOString()
        });
    });

    app.delete('/users/:id', (req, res) => {
        const { id } = req.params;
        res.json({
            message: `사용자 ${id} 삭제됨`,
            deleted: true
        });
    });

    // 쿼리 파라미터 테스트
    app.get('/search', (req, res) => {
        const { q, page = 1, limit = 10 } = req.query;
        res.json({
            query: q,
            page: parseInt(page),
            limit: parseInt(limit),
            results: []
        });
    });

    // 에러 핸들링
    app.get('/error', (req, res, next) => {
        const error = new Error('테스트 에러');
        error.status = 500;
        next(error);
    });

    // 404 핸들러
    app.use((req, res) => {
        res.status(404).json({ error: 'Not Found' });
    });

    // 에러 핸들러
    app.use((err, req, res, next) => {
        const status = err.status || 500;
        res.status(status).json({
            error: err.message,
            status
        });
    });

    return app;
}

// HTTP 요청 테스트 유틸리티
function makeRequest(options) {
    return new Promise((resolve, reject) => {
        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    resolve({ status: res.statusCode, data: json });
                } catch (e) {
                    resolve({ status: res.statusCode, data });
                }
            });
        });

        req.on('error', reject);

        if (options.body) {
            req.write(JSON.stringify(options.body));
        }

        req.end();
    });
}

// 테스트 실행
async function runExpressTests() {
    console.log('\n🚀 Express.js 테스트 시작\n');
    console.log('='.repeat(50));

    const app = createTestApp();
    const port = 3456;
    const server = http.createServer(app);

    return new Promise((resolve) => {
        server.listen(port, async () => {
            console.log(`✅ Express 서버 시작 (포트: ${port})\n`);

            const tests = [];
            let passed = 0;
            let failed = 0;

            try {
                // GET 요청 테스트
                console.log('📌 GET 요청 테스트');
                let result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/',
                    method: 'GET'
                });
                const homeTest = result.status === 200 && result.data.message;
                console.log(homeTest ? '✅' : '❌', 'GET /');
                if (homeTest) passed++; else failed++;

                // 사용자 목록
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/users',
                    method: 'GET'
                });
                const usersTest = result.status === 200 && Array.isArray(result.data.users);
                console.log(usersTest ? '✅' : '❌', 'GET /users');
                if (usersTest) passed++; else failed++;

                // 특정 사용자
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/users/123',
                    method: 'GET'
                });
                const userTest = result.status === 200 && result.data.id === 123;
                console.log(userTest ? '✅' : '❌', 'GET /users/:id');
                if (userTest) passed++; else failed++;

                // POST 요청 테스트
                console.log('\n📌 POST 요청 테스트');
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/users',
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: {
                        name: '새사용자',
                        email: 'new@example.com'
                    }
                });
                const postTest = result.status === 201 && result.data.name === '새사용자';
                console.log(postTest ? '✅' : '❌', 'POST /users');
                if (postTest) passed++; else failed++;

                // PUT 요청 테스트
                console.log('\n📌 PUT 요청 테스트');
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/users/456',
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: {
                        name: '수정된사용자',
                        email: 'updated@example.com'
                    }
                });
                const putTest = result.status === 200 && result.data.updated;
                console.log(putTest ? '✅' : '❌', 'PUT /users/:id');
                if (putTest) passed++; else failed++;

                // DELETE 요청 테스트
                console.log('\n📌 DELETE 요청 테스트');
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/users/789',
                    method: 'DELETE'
                });
                const deleteTest = result.status === 200 && result.data.deleted === true;
                console.log(deleteTest ? '✅' : '❌', 'DELETE /users/:id');
                if (deleteTest) passed++; else failed++;

                // 쿼리 파라미터 테스트
                console.log('\n📌 쿼리 파라미터 테스트');
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/search?q=test&page=2&limit=20',
                    method: 'GET'
                });
                const queryTest = result.status === 200 &&
                    result.data.query === 'test' &&
                    result.data.page === 2 &&
                    result.data.limit === 20;
                console.log(queryTest ? '✅' : '❌', 'GET /search with query params');
                if (queryTest) passed++; else failed++;

                // 404 테스트
                console.log('\n📌 에러 핸들링 테스트');
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/nonexistent',
                    method: 'GET'
                });
                const notFoundTest = result.status === 404;
                console.log(notFoundTest ? '✅' : '❌', '404 Not Found');
                if (notFoundTest) passed++; else failed++;

                // 500 에러 테스트
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/error',
                    method: 'GET'
                });
                const errorTest = result.status === 500;
                console.log(errorTest ? '✅' : '❌', '500 Error');
                if (errorTest) passed++; else failed++;

            } catch (error) {
                console.error('❌ 테스트 중 오류:', error.message);
                failed++;
            }

            // 결과 요약
            console.log('\n' + '='.repeat(50));
            console.log('📊 Express 테스트 결과');
            console.log('='.repeat(50));
            console.log(`✅ 성공: ${passed}`);
            console.log(`❌ 실패: ${failed}`);
            console.log(`📈 성공률: ${(passed / (passed + failed) * 100).toFixed(2)}%`);

            // 서버 종료
            server.close(() => {
                console.log('\n✅ Express 서버 종료');
                resolve();
            });
        });
    });
}

// 미들웨어 테스트
async function testMiddleware() {
    console.log('\n🔧 미들웨어 테스트\n');
    console.log('='.repeat(50));

    const app = express();
    let middlewareExecuted = false;

    // 커스텀 미들웨어
    app.use((req, res, next) => {
        middlewareExecuted = true;
        req.customData = 'middleware-data';
        next();
    });

    app.get('/test', (req, res) => {
        res.json({
            middlewareExecuted,
            customData: req.customData
        });
    });

    const server = http.createServer(app);
    const port = 3457;

    return new Promise((resolve) => {
        server.listen(port, async () => {
            console.log(`✅ 미들웨어 테스트 서버 시작 (포트: ${port})`);

            try {
                const result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/test',
                    method: 'GET'
                });

                const success = result.data.middlewareExecuted &&
                              result.data.customData === 'middleware-data';
                console.log(success ? '✅' : '❌', '커스텀 미들웨어 실행');

            } catch (error) {
                console.error('❌ 미들웨어 테스트 실패:', error.message);
            }

            server.close(() => {
                console.log('✅ 미들웨어 테스트 서버 종료');
                resolve();
            });
        });
    });
}

// 라우터 테스트
async function testRouter() {
    console.log('\n🛣️ 라우터 테스트\n');
    console.log('='.repeat(50));

    const app = express();

    // 라우터 생성
    const apiRouter = express.Router();
    apiRouter.get('/status', (req, res) => {
        res.json({ status: 'OK', version: '1.0.0' });
    });

    const adminRouter = express.Router();
    adminRouter.get('/dashboard', (req, res) => {
        res.json({ dashboard: 'admin data' });
    });

    // 라우터 마운트
    app.use('/api', apiRouter);
    app.use('/admin', adminRouter);

    const server = http.createServer(app);
    const port = 3458;

    return new Promise((resolve) => {
        server.listen(port, async () => {
            console.log(`✅ 라우터 테스트 서버 시작 (포트: ${port})`);

            try {
                // API 라우터 테스트
                let result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/api/status',
                    method: 'GET'
                });
                console.log(result.data.status === 'OK' ? '✅' : '❌', '/api/status');

                // Admin 라우터 테스트
                result = await makeRequest({
                    hostname: 'localhost',
                    port,
                    path: '/admin/dashboard',
                    method: 'GET'
                });
                console.log(result.data.dashboard ? '✅' : '❌', '/admin/dashboard');

            } catch (error) {
                console.error('❌ 라우터 테스트 실패:', error.message);
            }

            server.close(() => {
                console.log('✅ 라우터 테스트 서버 종료');
                resolve();
            });
        });
    });
}

// 메인 실행
async function main() {
    console.log('🎯 Express.js 종합 테스트 시작');
    console.log('Express 버전:', require('express/package.json').version);

    await runExpressTests();
    await testMiddleware();
    await testRouter();

    console.log('\n✨ 모든 Express 테스트 완료!');
}

main().catch(console.error);