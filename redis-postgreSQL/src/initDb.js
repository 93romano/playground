import pg from 'pg';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
import { hashPassword } from './auth.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config({ path: path.join(__dirname, '../.env') });

const { Pool } = pg;

const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
});

const initializeDb = async () => {
  try {
    // Create tables
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id          SERIAL       PRIMARY KEY,
        username    VARCHAR(50)  NOT NULL UNIQUE,
        email       VARCHAR(100),
        password    VARCHAR(100) NOT NULL,
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        last_login  TIMESTAMPTZ
      );

      CREATE TABLE IF NOT EXISTS matches (
        id         SERIAL       PRIMARY KEY,
        start_time TIMESTAMPTZ  NOT NULL,
        end_time   TIMESTAMPTZ,
        status     VARCHAR(20)  NOT NULL
      );

      CREATE TABLE IF NOT EXISTS match_players (
        id        SERIAL       PRIMARY KEY,
        match_id  INT          NOT NULL REFERENCES matches(id)  ON DELETE CASCADE,
        user_id   INT          NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
        team      VARCHAR(10)  NOT NULL,
        score     INT          NOT NULL DEFAULT 0,
        kills     INT          NOT NULL DEFAULT 0,
        deaths    INT          NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS rankings (
        user_id      INT     PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        total_score  BIGINT  NOT NULL DEFAULT 0,
        total_kills  BIGINT  NOT NULL DEFAULT 0,
        total_deaths BIGINT  NOT NULL DEFAULT 0
      );
    `);

    // Insert sample data with hashed passwords
    const hashedPassword = await hashPassword('password123');
    await pool.query(`
      INSERT INTO users (username, email, password) VALUES
        ('player1', 'player1@example.com', $1),
        ('player2', 'player2@example.com', $1),
        ('player3', 'player3@example.com', $1)
      ON CONFLICT (username) DO NOTHING;
    `, [hashedPassword]);

    await pool.query(`
      INSERT INTO matches (start_time, status) VALUES
        (NOW(), 'waiting'),
        (NOW() - INTERVAL '1 hour', 'in_progress'),
        (NOW() - INTERVAL '2 hours', 'done')
      RETURNING id;
    `);

    console.log('Database initialized successfully!');
  } catch (error) {
    console.error('Error initializing database:', error);
  } finally {
    await pool.end();
  }
};

initializeDb(); 