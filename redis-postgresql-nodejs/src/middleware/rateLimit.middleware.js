import { cache } from '../config/redis.js';

// CIA Triad: Availability - Rate limiting to prevent abuse and ensure service availability
export const rateLimitMiddleware = (maxRequests = 100, windowSeconds = 900) => {
  return async (req, res, next) => {
    try {
      // Use IP address or user ID as identifier
      const identifier = req.user?.id || req.ip;
      const key = `rate_limit:${identifier}`;

      const result = await cache.checkRateLimit(identifier, maxRequests, windowSeconds);

      // Add rate limit headers
      res.setHeader('X-RateLimit-Limit', maxRequests);
      res.setHeader('X-RateLimit-Remaining', result.remaining);
      res.setHeader('X-RateLimit-Reset', Date.now() + windowSeconds * 1000);

      if (!result.allowed) {
        return res.status(429).json({
          success: false,
          error: 'Too many requests. Please try again later.',
          retryAfter: windowSeconds,
        });
      }

      next();
    } catch (error) {
      console.error('Rate limit error:', error);
      // Fail open - allow request if rate limiting fails
      next();
    }
  };
};

// Stricter rate limit for sensitive operations
export const strictRateLimit = rateLimitMiddleware(10, 60); // 10 requests per minute

// Normal rate limit for general API
export const normalRateLimit = rateLimitMiddleware(100, 900); // 100 requests per 15 minutes

// Lenient rate limit for read operations
export const lenientRateLimit = rateLimitMiddleware(500, 900); // 500 requests per 15 minutes
