import { Router } from 'express';
import { AuthController } from '@/controllers/AuthController';
import { authenticateToken } from '@/middleware/auth';
import { loginLimiter, apiLimiter } from '@/middleware/rateLimiter';

const router = Router();
const authController = new AuthController();

// 공개 라우트 (인증 불필요)
router.post('/register', apiLimiter, authController.register);
router.post('/login', loginLimiter, authController.login);
router.post('/refresh', apiLimiter, authController.refreshToken);

// 보호된 라우트 (인증 필요)
router.post('/logout', authenticateToken, authController.logout);
router.get('/profile', authenticateToken, authController.getProfile);
router.get('/online-users', authenticateToken, authController.getOnlineUsers);
router.get('/online-status/:userId', authenticateToken, authController.checkOnlineStatus);

export default router;
