import express from 'express';
import { createServer } from 'http';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import { config, validateConfig } from '@/config';
import { initializeDatabase, closeDatabase } from '@/config/database';
import { SocketService } from '@/services/SocketService';
import routes from '@/routes';
import { errorHandler, notFoundHandler } from '@/middleware/errorHandler';
import { apiLimiter } from '@/middleware/rateLimiter';
import logger from '@/utils/logger';

class Application {
  private app: express.Application;
  private server: any;
  private socketService: SocketService;

  constructor() {
    this.app = express();
    this.server = createServer(this.app);
    
    this.validateEnvironment();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
    this.setupSocketIO();
  }

  /**
   * 환경변수 검증
   */
  private validateEnvironment(): void {
    try {
      validateConfig();
      logger.info('Environment configuration validated');
    } catch (error) {
      logger.error('Environment validation failed:', error);
      process.exit(1);
    }
  }

  /**
   * 미들웨어 설정
   */
  private setupMiddleware(): void {
    // 보안 헤더
    this.app.use(helmet({
      contentSecurityPolicy: {
        directives: {
          defaultSrc: ["'self'"],
          scriptSrc: ["'self'", "'unsafe-inline'"],
          styleSrc: ["'self'", "'unsafe-inline'"],
          imgSrc: ["'self'", "data:", "https:"],
        },
      },
      crossOriginEmbedderPolicy: false
    }));

    // CORS 설정
    this.app.use(cors({
      origin: config.corsOrigin,
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization'],
      credentials: true
    }));

    // 압축
    this.app.use(compression());

    // 요청 파싱
    this.app.use(express.json({ limit: '10mb' }));
    this.app.use(express.urlencoded({ extended: true, limit: '10mb' }));

    // 요청 로깅
    this.app.use((req, res, next) => {
      logger.info('Request received:', {
        method: req.method,
        url: req.url,
        ip: req.ip,
        userAgent: req.get('User-Agent')
      });
      next();
    });

    // 글로벌 rate limiting
    this.app.use('/api', apiLimiter);

    logger.info('Middleware setup complete');
  }

  /**
   * 라우트 설정
   */
  private setupRoutes(): void {
    // API 라우트
    this.app.use('/api', routes);

    // 정적 파일 (프론트엔드 빌드 파일)
    if (config.nodeEnv === 'production') {
      this.app.use(express.static('public'));
      
      // SPA를 위한 fallback
      this.app.get('*', (req, res) => {
        res.sendFile('index.html', { root: 'public' });
      });
    } else {
      // 개발 환경에서는 간단한 웰컴 페이지
      this.app.get('/', (req, res) => {
        res.json({
          success: true,
          data: {
            name: 'Chat Application API',
            version: '1.0.0',
            environment: config.nodeEnv,
            docs: '/api'
          },
          message: 'Welcome to Chat API Server'
        });
      });
    }

    logger.info('Routes setup complete');
  }

  /**
   * 에러 핸들링 설정
   */
  private setupErrorHandling(): void {
    // 404 핸들러
    this.app.use(notFoundHandler);

    // 글로벌 에러 핸들러
    this.app.use(errorHandler);

    // 프로세스 레벨 에러 핸들링
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught Exception:', error);
      this.gracefulShutdown(1);
    });

    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
      this.gracefulShutdown(1);
    });

    // 종료 시그널 처리
    process.on('SIGTERM', () => {
      logger.info('SIGTERM signal received');
      this.gracefulShutdown(0);
    });

    process.on('SIGINT', () => {
      logger.info('SIGINT signal received');
      this.gracefulShutdown(0);
    });

    logger.info('Error handling setup complete');
  }

  /**
   * Socket.IO 설정
   */
  private setupSocketIO(): void {
    this.socketService = new SocketService(this.server);
    logger.info('Socket.IO setup complete');
  }

  /**
   * 서버 시작
   */
  public async start(): Promise<void> {
    try {
      // 데이터베이스 연결
      await initializeDatabase();
      
      // 서버 시작
      this.server.listen(config.port, () => {
        logger.info(`Server started on port ${config.port}`, {
          port: config.port,
          environment: config.nodeEnv,
          nodeVersion: process.version,
          platform: process.platform
        });
      });

    } catch (error) {
      logger.error('Failed to start server:', error);
      process.exit(1);
    }
  }

  /**
   * 우아한 종료
   */
  private gracefulShutdown(exitCode: number): void {
    logger.info('Starting graceful shutdown...');

    // 새로운 연결 거부
    this.server.close(async () => {
      logger.info('HTTP server closed');

      try {
        // Socket.IO 서버 종료
        if (this.socketService) {
          this.socketService.getIO().close();
          logger.info('Socket.IO server closed');
        }

        // 데이터베이스 연결 종료
        await closeDatabase();

        logger.info('Graceful shutdown completed');
        process.exit(exitCode);

      } catch (error) {
        logger.error('Error during graceful shutdown:', error);
        process.exit(1);
      }
    });

    // 강제 종료 타이머 (30초)
    setTimeout(() => {
      logger.error('Could not close connections in time, forcefully shutting down');
      process.exit(1);
    }, 30000);
  }

  /**
   * Express 앱 인스턴스 반환
   */
  public getApp(): express.Application {
    return this.app;
  }

  /**
   * HTTP 서버 인스턴스 반환
   */
  public getServer(): any {
    return this.server;
  }
}

// 애플리케이션 시작
const app = new Application();

if (require.main === module) {
  app.start().catch((error) => {
    logger.error('Failed to start application:', error);
    process.exit(1);
  });
}

export default app;
