import express from 'express';
import pg from 'pg';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
import { authenticateToken, generateToken, storeSession, comparePassword } from './auth.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config({ path: path.join(__dirname, '../.env') });

const app = express();
app.use(express.json());

// PostgreSQL setup
const pool = new pg.Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
});

// Redis setup
const redisClient = createClient({
  url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`
});

redisClient.on('error', err => console.log('Redis Client Error', err));
await redisClient.connect();

// Authentication Routes
app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;

  try {
    const result = await pool.query(
      'SELECT id, password FROM users WHERE username = $1',
      [username]
    );

    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const user = result.rows[0];
    const validPassword = await comparePassword(password, user.password);

    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = generateToken(user.id);
    await storeSession(user.id, token);

    // Update last login
    await pool.query(
      'UPDATE users SET last_login = NOW() WHERE id = $1',
      [user.id]
    );

    res.json({ token });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Protected Routes - Add authenticateToken middleware
app.post('/api/matches/start', authenticateToken, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    const matchResult = await client.query(
      'INSERT INTO matches (start_time, status) VALUES (NOW(), $1) RETURNING id',
      ['waiting']
    );
    const matchId = matchResult.rows[0].id;

    const { players } = req.body;
    for (const player of players) {
      await client.query(
        'INSERT INTO match_players (match_id, user_id, team) VALUES ($1, $2, $3)',
        [matchId, player.userId, player.team]
      );
    }

    await client.query('COMMIT');
    res.json({ matchId });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error starting match:', error);
    res.status(500).json({ error: 'Failed to start match' });
  } finally {
    client.release();
  }
});

app.post('/api/matches/:matchId/position', authenticateToken, async (req, res) => {
  const { matchId } = req.params;
  const { userId, position } = req.body;
  
  await redisClient.hSet(
    `match:${matchId}:positions`,
    userId.toString(),
    JSON.stringify(position)
  );
  
  res.json({ success: true });
});

app.post('/api/matches/:matchId/hit', authenticateToken, async (req, res) => {
  const { matchId } = req.params;
  const { attackerId, victimId, damage } = req.body;
  
  await redisClient.xAdd(
    `match:${matchId}:hitEvents`,
    '*',
    {
      'attackerId': attackerId.toString(),
      'victimId': victimId.toString(),
      'damage': damage.toString(),
      'timestamp': Date.now().toString()
    }
  );
  
  res.json({ success: true });
});

app.post('/api/matches/:matchId/score', authenticateToken, async (req, res) => {
  const { matchId } = req.params;
  const { userId, score, kills, deaths } = req.body;
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    await client.query(
      'UPDATE match_players SET score = score + $1, kills = kills + $2, deaths = deaths + $3 WHERE match_id = $4 AND user_id = $5',
      [score, kills, deaths, matchId, userId]
    );
    
    await client.query(
      `INSERT INTO rankings (user_id, total_score, total_kills, total_deaths)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (user_id) DO UPDATE SET
         total_score = rankings.total_score + $2,
         total_kills = rankings.total_kills + $3,
         total_deaths = rankings.total_deaths + $4`,
      [userId, score, kills, deaths]
    );
    
    await redisClient.zIncrBy('ranking:total_score', score, userId.toString());
    
    await client.query('COMMIT');
    res.json({ success: true });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error updating score:', error);
    res.status(500).json({ error: 'Failed to update score' });
  } finally {
    client.release();
  }
});

app.get('/api/rankings', async (req, res) => {
  const rankings = await redisClient.zRangeWithScores('ranking:total_score', 0, 9, {
    REV: true
  });
  res.json(rankings);
});

// Get current match positions
app.get('/api/matches/:matchId/positions', authenticateToken, async (req, res) => {
  const { matchId } = req.params;
  const positions = await redisClient.hGetAll(`match:${matchId}:positions`);
  
  // Convert string values to objects
  const formattedPositions = Object.entries(positions).reduce((acc, [userId, pos]) => {
    acc[userId] = JSON.parse(pos);
    return acc;
  }, {});
  
  res.json(formattedPositions);
});

// Get match hit events
app.get('/api/matches/:matchId/hits', authenticateToken, async (req, res) => {
  const { matchId } = req.params;
  const { count = 100 } = req.query;
  
  const hits = await redisClient.xRange(
    `match:${matchId}:hitEvents`,
    '-',
    '+',
    { COUNT: parseInt(count) }
  );
  
  res.json(hits);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
}); 