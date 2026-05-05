import { Pool } from 'pg';
import { createClient, RedisClientType } from 'redis';
import { config } from './index';
import logger from '@/utils/logger';

// PostgreSQL 연결 풀
export const pool = new Pool({
  host: config.database.host,
  port: config.database.port,
  database: config.database.name,
  user: config.database.user,
  password: config.database.password,
  max: 20, // 최대 연결 수
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Redis 클라이언트
export const redisClient: RedisClientType = createClient({
  socket: {
    host: config.redis.host,
    port: config.redis.port,
  },
  password: config.redis.password,
});

// 데이터베이스 연결 초기화
export const initializeDatabase = async (): Promise<void> => {
  try {
    // PostgreSQL 연결 테스트
    const client = await pool.connect();
    await client.query('SELECT NOW()');
    client.release();
    logger.info('PostgreSQL connected successfully');

    // Redis 연결
    await redisClient.connect();
    logger.info('Redis connected successfully');

    // 데이터베이스 테이블 생성
    await createTables();
    
  } catch (error) {
    logger.error('Database initialization failed:', error);
    throw error;
  }
};

// 테이블 생성 함수
const createTables = async (): Promise<void> => {
  const client = await pool.connect();
  
  try {
    // Users 테이블
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Rooms 테이블
    await client.query(`
      CREATE TABLE IF NOT EXISTS rooms (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(100) NOT NULL,
        description TEXT,
        created_by UUID NOT NULL REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Messages 테이블
    await client.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        content TEXT NOT NULL,
        user_id UUID NOT NULL REFERENCES users(id),
        room_id UUID NOT NULL REFERENCES rooms(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Room memberships 테이블 (사용자-채팅방 관계)
    await client.query(`
      CREATE TABLE IF NOT EXISTS room_members (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id),
        room_id UUID NOT NULL REFERENCES rooms(id),
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, room_id)
      )
    `);

    // 인덱스 생성
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages(room_id);
      CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
      CREATE INDEX IF NOT EXISTS idx_room_members_user_id ON room_members(user_id);
      CREATE INDEX IF NOT EXISTS idx_room_members_room_id ON room_members(room_id);
    `);

    logger.info('Database tables created successfully');
    
  } finally {
    client.release();
  }
};

// 정리 함수
export const closeDatabase = async (): Promise<void> => {
  try {
    await pool.end();
    await redisClient.quit();
    logger.info('Database connections closed');
  } catch (error) {
    logger.error('Error closing database connections:', error);
  }
};

