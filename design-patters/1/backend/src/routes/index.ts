import { Router } from 'express';
import authRoutes from './auth';
import chatRoutes from './chat';

const router = Router();

// API 버전 관리
router.use('/auth', authRoutes);
router.use('/chat', chatRoutes);

// 헬스 체크
router.get('/health', (req, res) => {
  res.json({
    success: true,
    data: {
      status: 'OK',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      environment: process.env.NODE_ENV || 'development'
    },
    message: 'Server is healthy'
  });
});

// API 정보
router.get('/', (req, res) => {
  res.json({
    success: true,
    data: {
      name: 'Chat API',
      version: '1.0.0',
      description: 'Real-time chat application API',
      endpoints: {
        auth: '/api/auth',
        chat: '/api/chat',
        health: '/api/health'
      }
    },
    message: 'Welcome to Chat API'
  });
});

export default router;
