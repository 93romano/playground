import { Request, Response, NextFunction } from 'express';
import { ApiError } from '@/types';
import logger from '@/utils/logger';

/**
 * API 에러 클래스
 */
export class AppError extends Error implements ApiError {
  public status: number;
  public code?: string;

  constructor(message: string, status: number = 500, code?: string) {
    super(message);
    this.status = status;
    this.code = code;
    this.name = 'AppError';
  }
}

/**
 * 에러 핸들러 미들웨어
 */
export const errorHandler = (
  error: Error | AppError,
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  // 이미 응답이 전송된 경우 기본 에러 핸들러로 전달
  if (res.headersSent) {
    return next(error);
  }

  // 로깅
  logger.error('Error occurred:', {
    error: error.message,
    stack: error.stack,
    url: req.url,
    method: req.method,
    ip: req.ip,
    userAgent: req.get('User-Agent')
  });

  // AppError인 경우
  if (error instanceof AppError) {
    res.status(error.status).json({
      error: error.message,
      code: error.code,
      timestamp: new Date().toISOString()
    });
    return;
  }

  // 기본 에러 응답
  res.status(500).json({
    error: 'Internal Server Error',
    message: 'Something went wrong',
    timestamp: new Date().toISOString()
  });
};

/**
 * 404 핸들러
 */
export const notFoundHandler = (
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  const error = new AppError(`Route ${req.originalUrl} not found`, 404, 'NOT_FOUND');
  next(error);
};

/**
 * 비동기 함수 래퍼 (에러 자동 캐치)
 */
export const asyncHandler = (
  fn: (req: Request, res: Response, next: NextFunction) => Promise<any>
) => {
  return (req: Request, res: Response, next: NextFunction): void => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
};

