const { Client } = require('pg');

// PostgreSQL 연결 설정
const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'your_password', // 실제 비밀번호로 변경하세요
    database: 'practice_db'
});

// 테스트 함수들
async function testConnection() {
    console.log('🔄 데이터베이스 연결 테스트...');
    try {
        await client.connect();
        console.log('✅ 데이터베이스 연결 성공!');
        return true;
    } catch (error) {
        console.error('❌ 연결 실패:', error.message);
        return false;
    }
}

async function testCreateTable() {
    console.log('\n🔄 테이블 생성 테스트...');
    try {
        // 기존 테이블 삭제 (있을 경우)
        await client.query('DROP TABLE IF EXISTS test_users CASCADE');

        // 테이블 생성
        await client.query(`
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INTEGER,
                email VARCHAR(100) UNIQUE
            )
        `);
        console.log('✅ 테이블 생성 성공!');
        return true;
    } catch (error) {
        console.error('❌ 테이블 생성 실패:', error.message);
        return false;
    }
}

async function testInsert() {
    console.log('\n🔄 데이터 삽입 테스트...');
    try {
        const result = await client.query(`
            INSERT INTO test_users (name, age, email)
            VALUES ($1, $2, $3)
            RETURNING *
        `, ['홍길동', 25, 'hong@example.com']);

        console.log('✅ 데이터 삽입 성공!');
        console.log('삽입된 데이터:', result.rows[0]);

        // 여러 개 삽입
        await client.query(`
            INSERT INTO test_users (name, age, email)
            VALUES
                ($1, $2, $3),
                ($4, $5, $6),
                ($7, $8, $9)
        `, [
            '김철수', 30, 'kim@example.com',
            '이영희', 28, 'lee@example.com',
            '박민수', 35, 'park@example.com'
        ]);
        console.log('✅ 여러 데이터 삽입 성공!');
        return true;
    } catch (error) {
        console.error('❌ 데이터 삽입 실패:', error.message);
        return false;
    }
}

async function testSelect() {
    console.log('\n🔄 데이터 조회 테스트...');
    try {
        // 전체 조회
        const allUsers = await client.query('SELECT * FROM test_users');
        console.log('✅ 전체 사용자:', allUsers.rows);

        // 조건부 조회
        const youngUsers = await client.query(
            'SELECT * FROM test_users WHERE age < $1',
            [30]
        );
        console.log('✅ 30세 미만 사용자:', youngUsers.rows);

        // 집계 함수
        const avgAge = await client.query(
            'SELECT AVG(age) as average_age FROM test_users'
        );
        console.log('✅ 평균 나이:', avgAge.rows[0].average_age);

        return true;
    } catch (error) {
        console.error('❌ 데이터 조회 실패:', error.message);
        return false;
    }
}

async function testUpdate() {
    console.log('\n🔄 데이터 수정 테스트...');
    try {
        const result = await client.query(`
            UPDATE test_users
            SET age = age + 1
            WHERE name = $1
            RETURNING *
        `, ['홍길동']);

        console.log('✅ 데이터 수정 성공!');
        console.log('수정된 데이터:', result.rows[0]);
        return true;
    } catch (error) {
        console.error('❌ 데이터 수정 실패:', error.message);
        return false;
    }
}

async function testDelete() {
    console.log('\n🔄 데이터 삭제 테스트...');
    try {
        const result = await client.query(`
            DELETE FROM test_users
            WHERE age > $1
            RETURNING name
        `, [30]);

        console.log('✅ 데이터 삭제 성공!');
        console.log('삭제된 사용자:', result.rows.map(r => r.name));
        return true;
    } catch (error) {
        console.error('❌ 데이터 삭제 실패:', error.message);
        return false;
    }
}

async function testTransaction() {
    console.log('\n🔄 트랜잭션 테스트...');
    try {
        await client.query('BEGIN');

        // 첫 번째 작업
        await client.query(
            'INSERT INTO test_users (name, age, email) VALUES ($1, $2, $3)',
            ['트랜잭션테스트', 40, 'trans@example.com']
        );

        // 두 번째 작업
        await client.query(
            'UPDATE test_users SET age = age + 10 WHERE name = $1',
            ['트랜잭션테스트']
        );

        // 커밋
        await client.query('COMMIT');
        console.log('✅ 트랜잭션 성공!');

        // 결과 확인
        const result = await client.query(
            'SELECT * FROM test_users WHERE name = $1',
            ['트랜잭션테스트']
        );
        console.log('트랜잭션 결과:', result.rows[0]);

        return true;
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ 트랜잭션 실패 (롤백됨):', error.message);
        return false;
    }
}

async function testJoin() {
    console.log('\n🔄 JOIN 테스트...');
    try {
        // 주문 테이블 생성
        await client.query('DROP TABLE IF EXISTS test_orders CASCADE');
        await client.query(`
            CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                product VARCHAR(100),
                price DECIMAL(10, 2),
                FOREIGN KEY (user_id) REFERENCES test_users(id)
            )
        `);

        // 주문 데이터 삽입
        await client.query(`
            INSERT INTO test_orders (user_id, product, price)
            SELECT
                id,
                CASE WHEN id % 2 = 0 THEN '노트북' ELSE '마우스' END,
                CASE WHEN id % 2 = 0 THEN 1500000 ELSE 30000 END
            FROM test_users
        `);

        // JOIN 쿼리
        const result = await client.query(`
            SELECT
                u.name,
                u.email,
                o.product,
                o.price
            FROM test_users u
            LEFT JOIN test_orders o ON u.id = o.user_id
            ORDER BY u.name
        `);

        console.log('✅ JOIN 결과:');
        result.rows.forEach(row => {
            console.log(`  - ${row.name}: ${row.product || '주문없음'} (${row.price || 0}원)`);
        });

        return true;
    } catch (error) {
        console.error('❌ JOIN 테스트 실패:', error.message);
        return false;
    }
}

async function testIndexPerformance() {
    console.log('\n🔄 인덱스 성능 테스트...');
    try {
        // 많은 데이터 삽입
        console.log('대량 데이터 생성 중...');
        await client.query(`
            INSERT INTO test_users (name, age, email)
            SELECT
                'User_' || generate_series,
                (random() * 50 + 20)::integer,
                'user' || generate_series || '@example.com'
            FROM generate_series(1, 1000)
        `);

        // 인덱스 없이 조회
        console.time('인덱스 없음');
        await client.query(`
            SELECT * FROM test_users WHERE email = 'user500@example.com'
        `);
        console.timeEnd('인덱스 없음');

        // 인덱스 생성
        await client.query('CREATE INDEX idx_email ON test_users(email)');
        console.log('✅ 인덱스 생성 완료');

        // 인덱스 있을 때 조회
        console.time('인덱스 있음');
        await client.query(`
            SELECT * FROM test_users WHERE email = 'user500@example.com'
        `);
        console.timeEnd('인덱스 있음');

        // EXPLAIN 분석
        const explain = await client.query(`
            EXPLAIN ANALYZE
            SELECT * FROM test_users WHERE email = 'user500@example.com'
        `);
        console.log('✅ 실행 계획:', explain.rows.map(r => r['QUERY PLAN']).join('\n'));

        return true;
    } catch (error) {
        console.error('❌ 인덱스 테스트 실패:', error.message);
        return false;
    }
}

// 메인 실행 함수
async function runAllTests() {
    console.log('===== PostgreSQL 테스트 시작 =====\n');

    const tests = [
        testConnection,
        testCreateTable,
        testInsert,
        testSelect,
        testUpdate,
        testDelete,
        testTransaction,
        testJoin,
        testIndexPerformance
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
    try {
        await client.query('DROP TABLE IF EXISTS test_orders CASCADE');
        await client.query('DROP TABLE IF EXISTS test_users CASCADE');
        await client.end();
    } catch (error) {
        console.error('정리 중 오류:', error.message);
    }

    console.log('\n===== 테스트 결과 =====');
    console.log(`✅ 성공: ${passedTests}`);
    console.log(`❌ 실패: ${failedTests}`);
    console.log(`📊 성공률: ${(passedTests / (passedTests + failedTests) * 100).toFixed(2)}%`);
}

// 실행
runAllTests().catch(console.error);