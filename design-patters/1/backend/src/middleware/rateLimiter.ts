import rateLimit from 'express-rate-limit';
import { config } from '@/config';
import { redisClient } from '@/config/database';
import logger from '@/utils/logger';

/**
 * 기본 API 율제한
 */
export const apiLimiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  max: config.rateLimit.maxRequests,
  message: {
    error: 'Too many requests',
    message: 'Please try again later'
  },
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    logger.warn('Rate limit exceeded:', {
      ip: req.ip,
      userAgent: req.get('User-Agent'),
      url: req.url
    });
    
    res.status(429).json({
      error: 'Too many requests',
      message: 'Please try again later',
      retryAfter: Math.ceil(config.rateLimit.windowMs / 1000)
    });
  }
});

/**
 * 로그인 시도 율제한 (더 엄격)
 */
export const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15분
  max: 5, // 15분당 5번 시도
  message: {
    error: 'Too many login attempts',
    message: 'Please try again after 15 minutes'
  },
  standardHeaders: true,
  legacyHeaders: false,
  skipSuccessfulRequests: true, // 성공한 요청은 카운트하지 않음
  handler: (req, res) => {
    logger.warn('Login rate limit exceeded:', {
      ip: req.ip,
      email: req.body?.email,
      userAgent: req.get('User-Agent')
    });
    
    res.status(429).json({
      error: 'Too many login attempts',
      message: 'Please try again after 15 minutes',
      retryAfter: 15 * 60
    });
  }
});

/**
 * 메시지 전송 율제한
 */
export const messageLimiter = rateLimit({
  windowMs: 1000, // 1초
  max: 5, // 초당 5개 메시지
  message: {
    error: 'Too many messages',
    message: 'Please slow down'
  },
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    logger.warn('Message rate limit exceeded:', {
      ip: req.ip,
      userId: req.user?.userId,
      userAgent: req.get('User-Agent')
    });
    
    res.status(429).json({
      error: 'Too many messages',
      message: 'Please slow down',
      retryAfter: 1
    });
  }
});

/**
 * Redis 기반 사용자별 율제한
 */
export const createUserRateLimit = (windowMs: number, maxRequests: number) => {
  return async (req: any, res: any, next: any) => {
    if (!req.user?.userId) {
      return next();
    }

    const key = `rate_limit:${req.user.userId}:${req.route?.path || req.path}`;
    
    try {
      const current = await redisClient.incr(key);
      
      if (current === 1) {
        await redisClient.expire(key, Math.ceil(windowMs / 1000));
      }
      
      if (current > maxRequests) {
        logger.warn('User rate limit exceeded:', {
          userId: req.user.userId,
          key,
          current,
          max: maxRequests
        });
        
        return res.status(429).json({
          error: 'Rate limit exceeded',
          message: 'Too many requests from this user'
        });
      }
      
      next();
    } catch (error) {
      logger.error('Redis rate limiting error:', error);
      // Redis 에러 시 통과
      next();
    }
  };
};

