import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { createClient } from 'redis';
import dotenv from 'dotenv';

dotenv.config();

const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key';
const SESSION_TTL = 86400; // 24 hours in seconds

// Redis client setup
const redisClient = createClient({
  url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`
});

redisClient.on('error', err => console.log('Redis Client Error', err));
await redisClient.connect();

// Generate JWT token
export const generateToken = (userId) => {
  return jwt.sign({ userId }, JWT_SECRET, { expiresIn: '24h' });
};

// Store session in Redis
export const storeSession = async (userId, token) => {
  const key = `session:${userId}`;
  await redisClient.set(key, token);
  await redisClient.expire(key, SESSION_TTL);
};

// Verify token middleware
export const authenticateToken = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Authentication token required' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const storedToken = await redisClient.get(`session:${decoded.userId}`);

    if (!storedToken || storedToken !== token) {
      return res.status(401).json({ error: 'Invalid or expired session' });
    }

    req.user = decoded;
    next();
  } catch (error) {
    return res.status(403).json({ error: 'Invalid token' });
  }
};

// Hash password
export const hashPassword = async (password) => {
  const salt = await bcrypt.genSalt(10);
  return bcrypt.hash(password, salt);
};

// Compare password
export const comparePassword = async (password, hash) => {
  return bcrypt.compare(password, hash);
};

export const redisAuthClient = redisClient; 