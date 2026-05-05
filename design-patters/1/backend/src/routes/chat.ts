import { Router } from 'express';
import { ChatController } from '@/controllers/ChatController';
import { authenticateToken } from '@/middleware/auth';
import { apiLimiter, messageLimiter, createUserRateLimit } from '@/middleware/rateLimiter';

const router = Router();
const chatController = new ChatController();

// 모든 채팅 라우트는 인증 필요
router.use(authenticateToken);

// 채팅방 관련
router.get('/rooms', apiLimiter, chatController.getRooms);
router.post('/rooms', apiLimiter, createUserRateLimit(60000, 10), chatController.createRoom); // 분당 10개 방 생성 제한
router.get('/rooms/:roomId', apiLimiter, chatController.getRoomById);
router.post('/rooms/:roomId/join', apiLimiter, chatController.joinRoom);
router.post('/rooms/:roomId/leave', apiLimiter, chatController.leaveRoom);
router.delete('/rooms/:roomId', apiLimiter, chatController.deleteRoom);

// 메시지 관련
router.get('/rooms/:roomId/messages', apiLimiter, chatController.getMessages);
router.post('/messages', messageLimiter, chatController.sendMessage); // HTTP로도 메시지 전송 가능
router.get('/messages/search', apiLimiter, chatController.searchMessages);

// 온라인 사용자
router.get('/rooms/:roomId/online-users', apiLimiter, chatController.getOnlineUsersInRoom);

export default router;
