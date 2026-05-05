import { MessageRepository, MessageWithUser } from '@/models/Message';
import { RoomRepository } from '@/models/Room';
import { UserRepository } from '@/models/User';
import { redisClient } from '@/config/database';
import { Room, User, SendMessageRequest, CreateRoomRequest } from '@/types';
import { AppError } from '@/middleware/errorHandler';
import logger from '@/utils/logger';

export class ChatService {
  private messageRepository: MessageRepository;
  private roomRepository: RoomRepository;
  private userRepository: UserRepository;

  constructor() {
    this.messageRepository = new MessageRepository();
    this.roomRepository = new RoomRepository();
    this.userRepository = new UserRepository();
  }

  /**
   * 채팅방 생성
   */
  async createRoom(userId: string, data: CreateRoomRequest): Promise<Room> {
    const { name, description } = data;

    // 사용자 존재 확인
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    // 채팅방 생성
    const room = await this.roomRepository.create(name, userId, description);

    // Redis에 채팅방 캐시
    await this.cacheRoom(room);

    logger.info('Room created:', { roomId: room.id, userId, name });

    return room;
  }

  /**
   * 채팅방 목록 조회
   */
  async getRooms(userId?: string, limit: number = 50, offset: number = 0): Promise<Room[]> {
    let rooms: Room[];

    if (userId) {
      // 사용자가 참여한 채팅방만
      rooms = await this.roomRepository.findByUserId(userId);
    } else {
      // 모든 공개 채팅방
      rooms = await this.roomRepository.findAll(limit, offset);
    }

    return rooms;
  }

  /**
   * 채팅방 상세 정보
   */
  async getRoomById(roomId: string): Promise<Room | null> {
    // Redis 캐시 확인
    const cached = await this.getCachedRoom(roomId);
    if (cached) {
      return cached;
    }

    // DB에서 조회
    const room = await this.roomRepository.findById(roomId);
    
    if (room) {
      await this.cacheRoom(room);
    }

    return room;
  }

  /**
   * 채팅방 참여
   */
  async joinRoom(userId: string, roomId: string): Promise<{ room: Room; members: User[] }> {
    // 채팅방 존재 확인
    const room = await this.getRoomById(roomId);
    if (!room) {
      throw new AppError('Room not found', 404, 'ROOM_NOT_FOUND');
    }

    // 사용자 존재 확인
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    // 이미 멤버인지 확인
    const isMember = await this.roomRepository.isMember(roomId, userId);
    if (!isMember) {
      // 채팅방에 사용자 추가
      await this.roomRepository.addMember(roomId, userId);
    }

    // Redis에 온라인 사용자 추가
    await redisClient.sAdd(`room:${roomId}:online`, userId);

    // 멤버 목록 조회
    const members = await this.roomRepository.getMembers(roomId);

    logger.info('User joined room:', { userId, roomId });

    return { room, members };
  }

  /**
   * 채팅방 나가기
   */
  async leaveRoom(userId: string, roomId: string): Promise<void> {
    // 채팅방 멤버에서 제거
    await this.roomRepository.removeMember(roomId, userId);

    // Redis에서 온라인 사용자 제거
    await redisClient.sRem(`room:${roomId}:online`, userId);

    logger.info('User left room:', { userId, roomId });
  }

  /**
   * 메시지 전송
   */
  async sendMessage(userId: string, data: SendMessageRequest): Promise<MessageWithUser> {
    const { content, room_id } = data;

    // 채팅방 존재 확인
    const room = await this.getRoomById(room_id);
    if (!room) {
      throw new AppError('Room not found', 404, 'ROOM_NOT_FOUND');
    }

    // 사용자가 채팅방 멤버인지 확인
    const isMember = await this.roomRepository.isMember(room_id, userId);
    if (!isMember) {
      throw new AppError('You are not a member of this room', 403, 'NOT_ROOM_MEMBER');
    }

    // 메시지 생성
    const message = await this.messageRepository.create(content, userId, room_id);

    // Redis에 실시간 메시지 발행
    await this.publishMessage(message);

    // 최근 메시지 캐시 업데이트
    await this.cacheRecentMessage(room_id, message);

    logger.info('Message sent:', { messageId: message.id, userId, roomId: room_id });

    return message;
  }

  /**
   * 채팅방 메시지 히스토리 조회
   */
  async getMessages(
    roomId: string, 
    userId: string,
    limit: number = 50, 
    offset: number = 0
  ): Promise<MessageWithUser[]> {
    // 사용자가 채팅방 멤버인지 확인
    const isMember = await this.roomRepository.isMember(roomId, userId);
    if (!isMember) {
      throw new AppError('You are not a member of this room', 403, 'NOT_ROOM_MEMBER');
    }

    // 첫 페이지는 캐시에서 확인
    if (offset === 0 && limit <= 20) {
      const cached = await this.getCachedRecentMessages(roomId);
      if (cached && cached.length > 0) {
        return cached.slice(0, limit);
      }
    }

    // DB에서 조회
    const messages = await this.messageRepository.findByRoomId(roomId, limit, offset);

    // 첫 페이지는 캐시에 저장
    if (offset === 0) {
      await this.cacheRecentMessages(roomId, messages);
    }

    return messages;
  }

  /**
   * 채팅방 온라인 사용자 목록
   */
  async getOnlineUsersInRoom(roomId: string): Promise<User[]> {
    try {
      // Redis에서 온라인 사용자 ID 조회
      const onlineUserIds = await redisClient.sMembers(`room:${roomId}:online`);
      
      if (onlineUserIds.length === 0) {
        return [];
      }

      // 사용자 정보 조회
      const users: User[] = [];
      for (const userId of onlineUserIds) {
        const user = await this.userRepository.findById(userId);
        if (user) {
          users.push(user);
        }
      }

      return users;
      
    } catch (error) {
      logger.error('Failed to get online users in room:', error);
      return [];
    }
  }

  /**
   * 메시지 검색
   */
  async searchMessages(
    query: string,
    roomId?: string,
    userId?: string,
    limit: number = 20
  ): Promise<MessageWithUser[]> {
    // 특정 채팅방에서 검색하는 경우 멤버 확인
    if (roomId && userId) {
      const isMember = await this.roomRepository.isMember(roomId, userId);
      if (!isMember) {
        throw new AppError('You are not a member of this room', 403, 'NOT_ROOM_MEMBER');
      }
    }

    return await this.messageRepository.search(query, roomId, limit);
  }

  /**
   * 채팅방 삭제 (방장만 가능)
   */
  async deleteRoom(roomId: string, userId: string): Promise<void> {
    const room = await this.getRoomById(roomId);
    if (!room) {
      throw new AppError('Room not found', 404, 'ROOM_NOT_FOUND');
    }

    // 방장 확인
    if (room.created_by !== userId) {
      throw new AppError('Only room creator can delete the room', 403, 'NOT_ROOM_CREATOR');
    }

    // 채팅방 삭제
    await this.roomRepository.delete(roomId);

    // Redis 캐시 삭제
    await this.deleteCachedRoom(roomId);

    logger.info('Room deleted:', { roomId, userId });
  }

  // === 캐시 관련 private 메서드들 ===

  private async cacheRoom(room: Room): Promise<void> {
    try {
      const key = `room:${room.id}`;
      await redisClient.setEx(key, 3600, JSON.stringify(room)); // 1시간 캐시
    } catch (error) {
      logger.error('Failed to cache room:', error);
    }
  }

  private async getCachedRoom(roomId: string): Promise<Room | null> {
    try {
      const cached = await redisClient.get(`room:${roomId}`);
      return cached ? JSON.parse(cached) : null;
    } catch (error) {
      logger.error('Failed to get cached room:', error);
      return null;
    }
  }

  private async deleteCachedRoom(roomId: string): Promise<void> {
    try {
      await redisClient.del(`room:${roomId}`);
      await redisClient.del(`room:${roomId}:messages`);
      await redisClient.del(`room:${roomId}:online`);
    } catch (error) {
      logger.error('Failed to delete cached room:', error);
    }
  }

  private async publishMessage(message: MessageWithUser): Promise<void> {
    try {
      const channel = `room:${message.room_id}:messages`;
      await redisClient.publish(channel, JSON.stringify(message));
    } catch (error) {
      logger.error('Failed to publish message:', error);
    }
  }

  private async cacheRecentMessage(roomId: string, message: MessageWithUser): Promise<void> {
    try {
      const key = `room:${roomId}:messages`;
      await redisClient.lPush(key, JSON.stringify(message));
      await redisClient.lTrim(key, 0, 49); // 최근 50개만 유지
      await redisClient.expire(key, 3600); // 1시간 TTL
    } catch (error) {
      logger.error('Failed to cache recent message:', error);
    }
  }

  private async cacheRecentMessages(roomId: string, messages: MessageWithUser[]): Promise<void> {
    try {
      const key = `room:${roomId}:messages`;
      const pipeline = redisClient.multi();
      
      // 기존 캐시 삭제
      pipeline.del(key);
      
      // 새 메시지들 추가 (최신순)
      for (const message of messages) {
        pipeline.rPush(key, JSON.stringify(message));
      }
      
      pipeline.expire(key, 3600); // 1시간 TTL
      await pipeline.exec();
    } catch (error) {
      logger.error('Failed to cache recent messages:', error);
    }
  }

  private async getCachedRecentMessages(roomId: string): Promise<MessageWithUser[] | null> {
    try {
      const cached = await redisClient.lRange(`room:${roomId}:messages`, 0, -1);
      return cached.map(msg => JSON.parse(msg));
    } catch (error) {
      logger.error('Failed to get cached recent messages:', error);
      return null;
    }
  }
}

