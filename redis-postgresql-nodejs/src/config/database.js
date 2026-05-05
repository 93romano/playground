import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

// PostgreSQL connection pool - CIA Triad: Availability (connection pooling for high availability)
export const pgPool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  max: 20, // Maximum pool connections
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Health check
pgPool.on('connect', () => {
  console.log('✅ PostgreSQL connected');
});

pgPool.on('error', (err) => {
  console.error('❌ PostgreSQL error:', err);
});

export default pgPool;
