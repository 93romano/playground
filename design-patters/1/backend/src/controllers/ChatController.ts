import { Request, Response } from 'express';
import { ChatService } from '@/services/ChatService';
import { validate, createRoomSchema, sendMessageSchema, uuidSchema } from '@/utils/validation';
import { asyncHandler } from '@/middleware/errorHandler';
import logger from '@/utils/logger';

export class ChatController {
  private chatService: ChatService;

  constructor() {
    this.chatService = new ChatService();
  }

  /**
   * 채팅방 생성
   */
  createRoom = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    const data = validate(createRoomSchema, req.body);
    
    const room = await this.chatService.createRoom(userId, data);
    
    res.status(201).json({
      success: true,
      data: { room },
      message: 'Room created successfully'
    });
  });

  /**
   * 채팅방 목록 조회
   */
  getRooms = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { limit = '50', offset = '0', my } = req.query;
    
    const limitNum = parseInt(limit as string, 10);
    const offsetNum = parseInt(offset as string, 10);
    
    // 내 채팅방만 조회할지 결정
    const queryUserId = my === 'true' ? userId : undefined;
    
    const rooms = await this.chatService.getRooms(queryUserId, limitNum, offsetNum);
    
    res.json({
      success: true,
      data: { 
        rooms,
        pagination: {
          limit: limitNum,
          offset: offsetNum,
          count: rooms.length
        }
      },
      message: 'Rooms retrieved successfully'
    });
  });

  /**
   * 채팅방 상세 정보
   */
  getRoomById = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { roomId } = req.params;
    
    validate(uuidSchema, roomId);
    
    const room = await this.chatService.getRoomById(roomId);
    
    if (!room) {
      res.status(404).json({
        success: false,
        error: 'Room not found'
      });
      return;
    }
    
    res.json({
      success: true,
      data: { room },
      message: 'Room retrieved successfully'
    });
  });

  /**
   * 채팅방 참여
   */
  joinRoom = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { roomId } = req.params;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    validate(uuidSchema, roomId);
    
    const result = await this.chatService.joinRoom(userId, roomId);
    
    res.json({
      success: true,
      data: result,
      message: 'Joined room successfully'
    });
  });

  /**
   * 채팅방 나가기
   */
  leaveRoom = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { roomId } = req.params;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    validate(uuidSchema, roomId);
    
    await this.chatService.leaveRoom(userId, roomId);
    
    res.json({
      success: true,
      message: 'Left room successfully'
    });
  });

  /**
   * 메시지 전송 (HTTP API 버전)
   */
  sendMessage = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    const data = validate(sendMessageSchema, req.body);
    
    const message = await this.chatService.sendMessage(userId, data);
    
    res.status(201).json({
      success: true,
      data: { message },
      message: 'Message sent successfully'
    });
  });

  /**
   * 채팅방 메시지 히스토리 조회
   */
  getMessages = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { roomId } = req.params;
    const { limit = '50', offset = '0' } = req.query;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    validate(uuidSchema, roomId);
    
    const limitNum = parseInt(limit as string, 10);
    const offsetNum = parseInt(offset as string, 10);
    
    const messages = await this.chatService.getMessages(roomId, userId, limitNum, offsetNum);
    
    res.json({
      success: true,
      data: { 
        messages,
        pagination: {
          limit: limitNum,
          offset: offsetNum,
          count: messages.length
        }
      },
      message: 'Messages retrieved successfully'
    });
  });

  /**
   * 채팅방 온라인 사용자 목록
   */
  getOnlineUsersInRoom = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { roomId } = req.params;
    
    validate(uuidSchema, roomId);
    
    const users = await this.chatService.getOnlineUsersInRoom(roomId);
    
    res.json({
      success: true,
      data: { 
        users,
        count: users.length 
      },
      message: 'Online users retrieved successfully'
    });
  });

  /**
   * 메시지 검색
   */
  searchMessages = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { q: query, roomId, limit = '20' } = req.query;
    
    if (!query || typeof query !== 'string') {
      res.status(400).json({
        success: false,
        error: 'Search query is required'
      });
      return;
    }
    
    if (roomId && typeof roomId === 'string') {
      validate(uuidSchema, roomId);
    }
    
    const limitNum = parseInt(limit as string, 10);
    
    const messages = await this.chatService.searchMessages(
      query,
      roomId as string | undefined,
      userId,
      limitNum
    );
    
    res.json({
      success: true,
      data: { 
        messages,
        query,
        count: messages.length 
      },
      message: 'Search completed successfully'
    });
  });

  /**
   * 채팅방 삭제
   */
  deleteRoom = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    const { roomId } = req.params;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    validate(uuidSchema, roomId);
    
    await this.chatService.deleteRoom(roomId, userId);
    
    res.json({
      success: true,
      message: 'Room deleted successfully'
    });
  });
}
