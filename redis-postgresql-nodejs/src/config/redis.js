import { createClient } from 'redis';
import dotenv from 'dotenv';

dotenv.config();

// Redis client for caching - CIA Triad: Availability (fast data access, reduces DB load)
// Big Data 3V: Velocity (handles high-speed read/write operations)
const redisClient = createClient({
  socket: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
  },
  password: process.env.REDIS_PASSWORD || undefined,
});

redisClient.on('connect', () => {
  console.log('✅ Redis connected');
});

redisClient.on('error', (err) => {
  console.error('❌ Redis error:', err);
});

await redisClient.connect();

// Cache utilities
export const cache = {
  // Set cache with TTL
  async set(key, value, ttl = 3600) {
    try {
      await redisClient.setEx(key, ttl, JSON.stringify(value));
    } catch (error) {
      console.error('Cache set error:', error);
    }
  },

  // Get cache
  async get(key) {
    try {
      const data = await redisClient.get(key);
      return data ? JSON.parse(data) : null;
    } catch (error) {
      console.error('Cache get error:', error);
      return null;
    }
  },

  // Delete cache
  async del(key) {
    try {
      await redisClient.del(key);
    } catch (error) {
      console.error('Cache delete error:', error);
    }
  },

  // Delete multiple keys by pattern
  async delPattern(pattern) {
    try {
      const keys = await redisClient.keys(pattern);
      if (keys.length > 0) {
        await redisClient.del(keys);
      }
    } catch (error) {
      console.error('Cache delete pattern error:', error);
    }
  },

  // Increment counter (for real-time analytics - Big Data 3V: Velocity)
  async incr(key) {
    try {
      return await redisClient.incr(key);
    } catch (error) {
      console.error('Cache increment error:', error);
      return 0;
    }
  },

  // Set with expiration
  async setWithExpiry(key, value, expiryInSeconds) {
    try {
      await redisClient.setEx(key, expiryInSeconds, JSON.stringify(value));
    } catch (error) {
      console.error('Cache set with expiry error:', error);
    }
  },

  // Add to sorted set (for leaderboards, trending posts)
  async zAdd(key, score, member) {
    try {
      await redisClient.zAdd(key, { score, value: member });
    } catch (error) {
      console.error('Cache zAdd error:', error);
    }
  },

  // Get top N from sorted set
  async zRevRange(key, start, stop) {
    try {
      return await redisClient.zRevRange(key, start, stop);
    } catch (error) {
      console.error('Cache zRevRange error:', error);
      return [];
    }
  },

  // Get sorted set with scores
  async zRevRangeWithScores(key, start, stop) {
    try {
      return await redisClient.zRevRangeWithScores(key, start, stop);
    } catch (error) {
      console.error('Cache zRevRangeWithScores error:', error);
      return [];
    }
  },

  // Add to list (for activity streams - Big Data 3V: Velocity)
  async lPush(key, ...values) {
    try {
      await redisClient.lPush(key, values.map(v => JSON.stringify(v)));
    } catch (error) {
      console.error('Cache lPush error:', error);
    }
  },

  // Get list range
  async lRange(key, start, stop) {
    try {
      const data = await redisClient.lRange(key, start, stop);
      return data.map(item => JSON.parse(item));
    } catch (error) {
      console.error('Cache lRange error:', error);
      return [];
    }
  },

  // Set hash field
  async hSet(key, field, value) {
    try {
      await redisClient.hSet(key, field, JSON.stringify(value));
    } catch (error) {
      console.error('Cache hSet error:', error);
    }
  },

  // Get hash field
  async hGet(key, field) {
    try {
      const data = await redisClient.hGet(key, field);
      return data ? JSON.parse(data) : null;
    } catch (error) {
      console.error('Cache hGet error:', error);
      return null;
    }
  },

  // Get all hash fields
  async hGetAll(key) {
    try {
      const data = await redisClient.hGetAll(key);
      const result = {};
      for (const [field, value] of Object.entries(data)) {
        result[field] = JSON.parse(value);
      }
      return result;
    } catch (error) {
      console.error('Cache hGetAll error:', error);
      return {};
    }
  },

  // Rate limiting helper - CIA Triad: Availability (prevent abuse)
  async checkRateLimit(identifier, maxRequests, windowSeconds) {
    try {
      const key = `rate_limit:${identifier}`;
      const current = await redisClient.incr(key);

      if (current === 1) {
        await redisClient.expire(key, windowSeconds);
      }

      return {
        allowed: current <= maxRequests,
        remaining: Math.max(0, maxRequests - current),
        current,
      };
    } catch (error) {
      console.error('Rate limit check error:', error);
      return { allowed: true, remaining: maxRequests, current: 0 };
    }
  },
};

export default redisClient;
