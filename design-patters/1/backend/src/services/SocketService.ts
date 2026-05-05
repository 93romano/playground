import { Server as SocketIOServer, Socket } from 'socket.io';
import { Server as HTTPServer } from 'http';
import { JWTUtil } from '@/utils/jwt';
import { ChatService } from '@/services/ChatService';
import { AuthService } from '@/services/AuthService';
import { redisClient } from '@/config/database';
import { config } from '@/config';
import { 
  ServerToClientEvents, 
  ClientToServerEvents, 
  InterServerEvents, 
  SocketData,
  SendMessageRequest 
} from '@/types';
import { validate, sendMessageSchema, uuidSchema } from '@/utils/validation';
import logger from '@/utils/logger';

export class SocketService {
  private io: SocketIOServer<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>;
  private chatService: ChatService;
  private authService: AuthService;

  constructor(server: HTTPServer) {
    this.io = new SocketIOServer(server, {
      cors: {
        origin: config.corsOrigin,
        methods: ['GET', 'POST'],
        credentials: true
      },
      transports: ['websocket', 'polling']
    });

    this.chatService = new ChatService();
    this.authService = new AuthService();

    this.setupMiddleware();
    this.setupEventHandlers();
    this.setupRedisSubscriptions();

    logger.info('Socket.io server initialized');
  }

  /**
   * 미들웨어 설정
   */
  private setupMiddleware(): void {
    // 인증 미들웨어
    this.io.use(async (socket, next) => {
      try {
        const token = socket.handshake.auth.token;
        
        if (!token) {
          throw new Error('No token provided');
        }

        const payload = JWTUtil.verifyAccessToken(token);
        
        // 사용자 정보 확인
        const user = await this.authService.getProfile(payload.userId);
        if (!user) {
          throw new Error('User not found');
        }

        // Socket 데이터에 사용자 정보 저장
        socket.data.userId = user.id;
        socket.data.username = user.username;

        logger.info('Socket authenticated:', { 
          socketId: socket.id, 
          userId: user.id,
          username: user.username 
        });

        next();
      } catch (error) {
        logger.warn('Socket authentication failed:', error);
        next(new Error('Authentication failed'));
      }
    });

    // 연결 로깅
    this.io.use((socket, next) => {
      logger.info('Socket connection attempt:', {
        socketId: socket.id,
        userId: socket.data.userId,
        userAgent: socket.handshake.headers['user-agent'],
        ip: socket.handshake.address
      });
      next();
    });
  }

  /**
   * 이벤트 핸들러 설정
   */
  private setupEventHandlers(): void {
    this.io.on('connection', (socket) => {
      this.handleConnection(socket);
    });
  }

  /**
   * 연결 핸들러
   */
  private handleConnection(socket: Socket<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>): void {
    const { userId, username } = socket.data;

    logger.info('User connected:', { socketId: socket.id, userId, username });

    // 온라인 상태 설정
    this.authService.setOnlineStatus(userId, true);

    // 채팅방 참여
    socket.on('joinRoom', async (roomId) => {
      await this.handleJoinRoom(socket, roomId);
    });

    // 채팅방 나가기
    socket.on('leaveRoom', async (roomId) => {
      await this.handleLeaveRoom(socket, roomId);
    });

    // 메시지 전송
    socket.on('sendMessage', async (data) => {
      await this.handleSendMessage(socket, data);
    });

    // 연결 해제
    socket.on('disconnect', async (reason) => {
      await this.handleDisconnect(socket, reason);
    });

    // 에러 처리
    socket.on('error', (error) => {
      logger.error('Socket error:', { socketId: socket.id, userId, error });
      socket.emit('error', 'An error occurred');
    });
  }

  /**
   * 채팅방 참여 처리
   */
  private async handleJoinRoom(
    socket: Socket<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>, 
    roomId: string
  ): Promise<void> {
    try {
      const { userId, username } = socket.data;

      // 룸 ID 검증
      validate(uuidSchema, roomId);

      // 채팅방 참여
      const result = await this.chatService.joinRoom(userId, roomId);

      // Socket.io 룸에 참여
      await socket.join(`room:${roomId}`);

      // 사용자에게 성공 응답
      socket.emit('message', {
        id: `system-${Date.now()}`,
        content: `Welcome to ${result.room.name}!`,
        user_id: 'system',
        room_id: roomId,
        created_at: new Date(),
        user: {
          id: 'system',
          username: 'System',
          email: '',
          created_at: new Date(),
          updated_at: new Date()
        }
      });

      // 다른 사용자들에게 참여 알림
      socket.to(`room:${roomId}`).emit('userJoined', {
        id: userId,
        username,
        email: '',
        created_at: new Date(),
        updated_at: new Date()
      });

      // 온라인 사용자 목록 업데이트
      const onlineUsers = await this.chatService.getOnlineUsersInRoom(roomId);
      this.io.to(`room:${roomId}`).emit('onlineUsers', onlineUsers);

      logger.info('User joined room via socket:', { userId, roomId, socketId: socket.id });

    } catch (error) {
      logger.error('Failed to join room:', error);
      socket.emit('error', 'Failed to join room');
    }
  }

  /**
   * 채팅방 나가기 처리
   */
  private async handleLeaveRoom(
    socket: Socket<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>, 
    roomId: string
  ): Promise<void> {
    try {
      const { userId, username } = socket.data;

      // 룸 ID 검증
      validate(uuidSchema, roomId);

      // 채팅방 나가기
      await this.chatService.leaveRoom(userId, roomId);

      // Socket.io 룸에서 나가기
      await socket.leave(`room:${roomId}`);

      // 다른 사용자들에게 나가기 알림
      socket.to(`room:${roomId}`).emit('userLeft', {
        id: userId,
        username,
        email: '',
        created_at: new Date(),
        updated_at: new Date()
      });

      // 온라인 사용자 목록 업데이트
      const onlineUsers = await this.chatService.getOnlineUsersInRoom(roomId);
      this.io.to(`room:${roomId}`).emit('onlineUsers', onlineUsers);

      logger.info('User left room via socket:', { userId, roomId, socketId: socket.id });

    } catch (error) {
      logger.error('Failed to leave room:', error);
      socket.emit('error', 'Failed to leave room');
    }
  }

  /**
   * 메시지 전송 처리
   */
  private async handleSendMessage(
    socket: Socket<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>, 
    data: SendMessageRequest
  ): Promise<void> {
    try {
      const { userId } = socket.data;

      // 데이터 검증
      const validatedData = validate(sendMessageSchema, data);

      // 메시지 저장
      const message = await this.chatService.sendMessage(userId, validatedData);

      // 해당 채팅방의 모든 사용자에게 메시지 전송
      this.io.to(`room:${validatedData.room_id}`).emit('message', message);

      logger.info('Message sent via socket:', { 
        messageId: message.id, 
        userId, 
        roomId: validatedData.room_id,
        socketId: socket.id 
      });

    } catch (error) {
      logger.error('Failed to send message:', error);
      socket.emit('error', 'Failed to send message');
    }
  }

  /**
   * 연결 해제 처리
   */
  private async handleDisconnect(
    socket: Socket<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData>, 
    reason: string
  ): Promise<void> {
    const { userId, username } = socket.data;

    logger.info('User disconnected:', { 
      socketId: socket.id, 
      userId, 
      username, 
      reason 
    });

    // 다른 활성 소켓이 있는지 확인
    const userSockets = await this.io.in(`user:${userId}`).fetchSockets();
    
    if (userSockets.length === 0) {
      // 마지막 소켓이면 오프라인 상태로 변경
      await this.authService.setOnlineStatus(userId, false);
      
      // 참여 중인 모든 채팅방에서 오프라인 상태 업데이트
      await this.updateUserOfflineStatus(userId);
    }
  }

  /**
   * Redis Pub/Sub 설정 (수평 확장용)
   */
  private setupRedisSubscriptions(): void {
    // Redis 구독 클라이언트 생성
    const subscriber = redisClient.duplicate();
    
    subscriber.connect().then(() => {
      // 메시지 채널 구독
      subscriber.pSubscribe('room:*:messages', (message, channel) => {
        try {
          const messageData = JSON.parse(message);
          const roomId = channel.split(':')[1];
          
          // 해당 룸의 모든 클라이언트에게 메시지 전송
          this.io.to(`room:${roomId}`).emit('message', messageData);
          
        } catch (error) {
          logger.error('Failed to process Redis message:', error);
        }
      });

      logger.info('Redis subscriptions setup complete');
    }).catch((error) => {
      logger.error('Failed to setup Redis subscriptions:', error);
    });
  }

  /**
   * 사용자 오프라인 상태 업데이트
   */
  private async updateUserOfflineStatus(userId: string): Promise<void> {
    try {
      // 사용자가 참여한 모든 채팅방 조회
      const rooms = await this.chatService.getRooms(userId);
      
      // 각 채팅방의 온라인 사용자 목록 업데이트
      for (const room of rooms) {
        const onlineUsers = await this.chatService.getOnlineUsersInRoom(room.id);
        this.io.to(`room:${room.id}`).emit('onlineUsers', onlineUsers);
      }
      
    } catch (error) {
      logger.error('Failed to update user offline status:', error);
    }
  }

  /**
   * Socket.io 인스턴스 반환
   */
  public getIO(): SocketIOServer<ClientToServerEvents, ServerToClientEvents, InterServerEvents, SocketData> {
    return this.io;
  }
}
