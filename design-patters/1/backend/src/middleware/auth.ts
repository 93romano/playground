import { Request, Response, NextFunction } from 'express';
import { JWTUtil } from '@/utils/jwt';
import { JWTPayload } from '@/types';
import logger from '@/utils/logger';

// Request 인터페이스 확장
declare global {
  namespace Express {
    interface Request {
      user?: JWTPayload;
    }
  }
}

/**
 * JWT 인증 미들웨어
 */
export const authenticateToken = (
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  try {
    const authHeader = req.headers.authorization;
    const token = JWTUtil.extractToken(authHeader);
    
    const payload = JWTUtil.verifyAccessToken(token);
    req.user = payload;
    
    next();
  } catch (error) {
    logger.warn('Authentication failed:', error);
    res.status(401).json({ 
      error: 'Unauthorized',
      message: 'Invalid or missing authentication token'
    });
  }
};

/**
 * 선택적 인증 미들웨어 (토큰이 있으면 검증, 없어도 통과)
 */
export const optionalAuth = (
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  try {
    const authHeader = req.headers.authorization;
    
    if (authHeader) {
      const token = JWTUtil.extractToken(authHeader);
      const payload = JWTUtil.verifyAccessToken(token);
      req.user = payload;
    }
    
    next();
  } catch (error) {
    // 토큰이 잘못되었어도 계속 진행
    logger.warn('Optional auth failed:', error);
    next();
  }
};

