const redis = require('redis');

// Redis 클라이언트 생성
const client = redis.createClient({
    host: 'localhost',
    port: 6379,
    // password: 'your_password' // 필요시 추가
});

// 에러 핸들링
client.on('error', (err) => {
    console.error('Redis 에러:', err);
});

// 연결 이벤트
client.on('connect', () => {
    console.log('✅ Redis 연결 성공!');
});

// Promise 래퍼 (Redis v3 호환)
const { promisify } = require('util');
const getAsync = promisify(client.get).bind(client);
const setAsync = promisify(client.set).bind(client);
const delAsync = promisify(client.del).bind(client);
const existsAsync = promisify(client.exists).bind(client);
const incrAsync = promisify(client.incr).bind(client);
const decrAsync = promisify(client.decr).bind(client);
const expireAsync = promisify(client.expire).bind(client);
const ttlAsync = promisify(client.ttl).bind(client);
const lpushAsync = promisify(client.lpush).bind(client);
const rpushAsync = promisify(client.rpush).bind(client);
const lrangeAsync = promisify(client.lrange).bind(client);
const lpopAsync = promisify(client.lpop).bind(client);
const rpopAsync = promisify(client.rpop).bind(client);
const saddAsync = promisify(client.sadd).bind(client);
const smembersAsync = promisify(client.smembers).bind(client);
const sismemberAsync = promisify(client.sismember).bind(client);
const hsetAsync = promisify(client.hset).bind(client);
const hgetAsync = promisify(client.hget).bind(client);
const hgetallAsync = promisify(client.hgetall).bind(client);
const zaddAsync = promisify(client.zadd).bind(client);
const zrangeAsync = promisify(client.zrange).bind(client);
const zrevrangeAsync = promisify(client.zrevrange).bind(client);

// 테스트 함수들

async function testString() {
    console.log('\n🔄 String 타입 테스트...');
    try {
        // SET & GET
        await setAsync('name', '홍길동');
        const name = await getAsync('name');
        console.log('✅ SET/GET:', name);

        // 카운터
        await setAsync('counter', '0');
        await incrAsync('counter');
        await incrAsync('counter');
        const counter = await getAsync('counter');
        console.log('✅ 카운터 증가:', counter);

        // 만료 시간
        await setAsync('temp', 'temporary');
        await expireAsync('temp', 2);
        const ttl = await ttlAsync('temp');
        console.log('✅ TTL:', ttl, '초');

        // 2초 후 확인
        await new Promise(resolve => setTimeout(resolve, 2100));
        const expired = await getAsync('temp');
        console.log('✅ 만료 후 값:', expired || '(삭제됨)');

        return true;
    } catch (error) {
        console.error('❌ String 테스트 실패:', error.message);
        return false;
    }
}

async function testList() {
    console.log('\n🔄 List 타입 테스트...');
    try {
        const listKey = 'tasks';

        // 리스트에 추가
        await lpushAsync(listKey, '작업3');
        await lpushAsync(listKey, '작업2');
        await lpushAsync(listKey, '작업1');
        await rpushAsync(listKey, '작업4');

        // 조회
        const tasks = await lrangeAsync(listKey, 0, -1);
        console.log('✅ 전체 작업:', tasks);

        // 제거
        const first = await lpopAsync(listKey);
        const last = await rpopAsync(listKey);
        console.log('✅ 제거된 작업:', { 첫번째: first, 마지막: last });

        const remaining = await lrangeAsync(listKey, 0, -1);
        console.log('✅ 남은 작업:', remaining);

        return true;
    } catch (error) {
        console.error('❌ List 테스트 실패:', error.message);
        return false;
    }
}

async function testSet() {
    console.log('\n🔄 Set 타입 테스트...');
    try {
        const setKey = 'languages';

        // 집합에 추가
        await saddAsync(setKey, 'JavaScript');
        await saddAsync(setKey, 'Python');
        await saddAsync(setKey, 'Java');
        await saddAsync(setKey, 'JavaScript'); // 중복

        // 조회
        const members = await smembersAsync(setKey);
        console.log('✅ 모든 언어:', members);

        // 멤버 확인
        const hasJS = await sismemberAsync(setKey, 'JavaScript');
        const hasRuby = await sismemberAsync(setKey, 'Ruby');
        console.log('✅ JavaScript 있음?:', hasJS === 1);
        console.log('✅ Ruby 있음?:', hasRuby === 1);

        return true;
    } catch (error) {
        console.error('❌ Set 테스트 실패:', error.message);
        return false;
    }
}

async function testHash() {
    console.log('\n🔄 Hash 타입 테스트...');
    try {
        const userKey = 'user:1001';

        // 해시 필드 설정
        await hsetAsync(userKey, 'name', '김철수');
        await hsetAsync(userKey, 'email', 'kim@example.com');
        await hsetAsync(userKey, 'age', '30');

        // 개별 필드 조회
        const userName = await hgetAsync(userKey, 'name');
        console.log('✅ 사용자 이름:', userName);

        // 전체 조회
        const user = await hgetallAsync(userKey);
        console.log('✅ 전체 사용자 정보:', user);

        return true;
    } catch (error) {
        console.error('❌ Hash 테스트 실패:', error.message);
        return false;
    }
}

async function testSortedSet() {
    console.log('\n🔄 Sorted Set 타입 테스트...');
    try {
        const leaderboard = 'leaderboard';

        // 점수와 함께 추가
        await zaddAsync(leaderboard, 100, 'player1');
        await zaddAsync(leaderboard, 150, 'player2');
        await zaddAsync(leaderboard, 120, 'player3');
        await zaddAsync(leaderboard, 200, 'player4');

        // 오름차순 조회
        const ascending = await zrangeAsync(leaderboard, 0, -1);
        console.log('✅ 오름차순:', ascending);

        // 내림차순 조회 (상위 3명)
        const top3 = await zrevrangeAsync(leaderboard, 0, 2, 'WITHSCORES');
        console.log('✅ 상위 3명 (점수 포함):');
        for (let i = 0; i < top3.length; i += 2) {
            console.log(`  ${i/2 + 1}위: ${top3[i]} - ${top3[i+1]}점`);
        }

        return true;
    } catch (error) {
        console.error('❌ Sorted Set 테스트 실패:', error.message);
        return false;
    }
}

async function testCaching() {
    console.log('\n🔄 캐싱 패턴 테스트...');
    try {
        // 캐시 키
        const cacheKey = 'product:123';

        // 캐시 확인
        let cached = await getAsync(cacheKey);
        if (!cached) {
            console.log('⚠️ 캐시 미스 - DB에서 조회');
            // DB 조회 시뮬레이션
            const data = {
                id: 123,
                name: '노트북',
                price: 1500000
            };

            // 캐시 저장 (1시간)
            await setAsync(cacheKey, JSON.stringify(data));
            await expireAsync(cacheKey, 3600);
            cached = JSON.stringify(data);
        } else {
            console.log('✅ 캐시 히트!');
        }

        const product = JSON.parse(cached);
        console.log('✅ 제품 정보:', product);

        return true;
    } catch (error) {
        console.error('❌ 캐싱 테스트 실패:', error.message);
        return false;
    }
}

async function testSession() {
    console.log('\n🔄 세션 관리 테스트...');
    try {
        const sessionId = 'session:' + Math.random().toString(36).substr(2, 9);
        const sessionData = {
            userId: 1001,
            username: '홍길동',
            loginTime: new Date().toISOString()
        };

        // 세션 생성 (30분)
        await setAsync(sessionId, JSON.stringify(sessionData));
        await expireAsync(sessionId, 1800);

        // 세션 조회
        const session = await getAsync(sessionId);
        console.log('✅ 세션 생성:', JSON.parse(session));

        // TTL 확인
        const ttl = await ttlAsync(sessionId);
        console.log('✅ 세션 만료까지:', ttl, '초');

        // 세션 연장
        await expireAsync(sessionId, 3600);
        const newTtl = await ttlAsync(sessionId);
        console.log('✅ 연장 후 만료까지:', newTtl, '초');

        // 세션 삭제
        await delAsync(sessionId);
        const deleted = await getAsync(sessionId);
        console.log('✅ 세션 삭제:', deleted === null ? '성공' : '실패');

        return true;
    } catch (error) {
        console.error('❌ 세션 테스트 실패:', error.message);
        return false;
    }
}

async function testRateLimiting() {
    console.log('\n🔄 Rate Limiting 테스트...');
    try {
        const userId = 1001;
        const limitKey = `rate:${userId}:${Date.now()}`;
        const maxRequests = 5;

        console.log(`⚡ ${maxRequests}회 요청 제한 테스트`);

        // 요청 시뮬레이션
        for (let i = 1; i <= 7; i++) {
            const count = await incrAsync(limitKey);

            if (i === 1) {
                // 첫 요청시 만료시간 설정 (1분)
                await expireAsync(limitKey, 60);
            }

            if (count <= maxRequests) {
                console.log(`  ✅ 요청 ${i}: 허용 (${count}/${maxRequests})`);
            } else {
                console.log(`  ❌ 요청 ${i}: 거부 (제한 초과)`);
            }
        }

        return true;
    } catch (error) {
        console.error('❌ Rate Limiting 테스트 실패:', error.message);
        return false;
    }
}

async function testTransaction() {
    console.log('\n🔄 트랜잭션 테스트...');
    try {
        // 계좌 초기화
        await setAsync('account:A', '1000');
        await setAsync('account:B', '500');

        console.log('💰 이체 전:');
        console.log('  계좌 A:', await getAsync('account:A'));
        console.log('  계좌 B:', await getAsync('account:B'));

        // 트랜잭션 실행
        const multi = client.multi();
        multi.decrby('account:A', 100);
        multi.incrby('account:B', 100);

        await new Promise((resolve, reject) => {
            multi.exec((err, results) => {
                if (err) reject(err);
                else resolve(results);
            });
        });

        console.log('💰 이체 후:');
        console.log('  계좌 A:', await getAsync('account:A'));
        console.log('  계좌 B:', await getAsync('account:B'));
        console.log('✅ 트랜잭션 성공!');

        return true;
    } catch (error) {
        console.error('❌ 트랜잭션 테스트 실패:', error.message);
        return false;
    }
}

async function testPubSub() {
    console.log('\n🔄 Pub/Sub 테스트...');
    try {
        // 구독자 클라이언트
        const subscriber = redis.createClient({
            host: 'localhost',
            port: 6379
        });

        // 발행자 클라이언트
        const publisher = redis.createClient({
            host: 'localhost',
            port: 6379
        });

        let messageCount = 0;
        const maxMessages = 3;

        return new Promise((resolve) => {
            // 메시지 수신 핸들러
            subscriber.on('message', (channel, message) => {
                console.log(`📨 [${channel}] 메시지 수신:`, message);
                messageCount++;

                if (messageCount >= maxMessages) {
                    console.log('✅ Pub/Sub 테스트 완료!');
                    subscriber.unsubscribe();
                    subscriber.quit();
                    publisher.quit();
                    resolve(true);
                }
            });

            // 채널 구독
            subscriber.subscribe('news', 'sports');

            // 메시지 발행 (약간의 지연 후)
            setTimeout(() => {
                publisher.publish('news', '속보: Redis 테스트 중');
                publisher.publish('sports', '축구: 한국 승리!');
                publisher.publish('news', '날씨: 오늘은 맑음');
            }, 100);
        });
    } catch (error) {
        console.error('❌ Pub/Sub 테스트 실패:', error.message);
        return false;
    }
}

// 메인 실행 함수
async function runAllTests() {
    console.log('===== Redis 테스트 시작 =====\n');

    const tests = [
        testString,
        testList,
        testSet,
        testHash,
        testSortedSet,
        testCaching,
        testSession,
        testRateLimiting,
        testTransaction,
        testPubSub
    ];

    let passedTests = 0;
    let failedTests = 0;

    for (const test of tests) {
        const result = await test();
        if (result) {
            passedTests++;
        } else {
            failedTests++;
        }
    }

    // 정리
    console.log('\n🧹 테스트 데이터 정리 중...');
    await new Promise(resolve => {
        client.flushdb((err) => {
            if (err) console.error('정리 실패:', err);
            else console.log('✅ 정리 완료!');
            resolve();
        });
    });

    // 연결 종료
    client.quit();

    console.log('\n===== 테스트 결과 =====');
    console.log(`✅ 성공: ${passedTests}`);
    console.log(`❌ 실패: ${failedTests}`);
    console.log(`📊 성공률: ${(passedTests / (passedTests + failedTests) * 100).toFixed(2)}%`);
}

// 실행
runAllTests().catch(console.error);