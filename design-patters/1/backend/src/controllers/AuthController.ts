import { Request, Response } from 'express';
import { AuthService } from '@/services/AuthService';
import { validate, registerSchema, loginSchema } from '@/utils/validation';
import { asyncHandler } from '@/middleware/errorHandler';
import logger from '@/utils/logger';

export class AuthController {
  private authService: AuthService;

  constructor() {
    this.authService = new AuthService();
  }

  /**
   * 사용자 등록
   */
  register = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const data = validate(registerSchema, req.body);
    
    const result = await this.authService.register(data);
    
    res.status(201).json({
      success: true,
      data: {
        user: result.user,
        tokens: result.tokens
      },
      message: 'User registered successfully'
    });
  });

  /**
   * 로그인
   */
  login = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const data = validate(loginSchema, req.body);
    
    const result = await this.authService.login(data);
    
    // 온라인 상태 설정
    await this.authService.setOnlineStatus(result.user.id, true);
    
    res.json({
      success: true,
      data: {
        user: result.user,
        tokens: result.tokens
      },
      message: 'Login successful'
    });
  });

  /**
   * 토큰 갱신
   */
  refreshToken = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { refreshToken } = req.body;
    
    if (!refreshToken) {
      res.status(400).json({
        success: false,
        error: 'Refresh token is required'
      });
      return;
    }
    
    const tokens = await this.authService.refreshToken(refreshToken);
    
    res.json({
      success: true,
      data: { tokens },
      message: 'Token refreshed successfully'
    });
  });

  /**
   * 로그아웃
   */
  logout = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    // 오프라인 상태 설정
    await this.authService.setOnlineStatus(userId, false);
    
    await this.authService.logout(userId);
    
    res.json({
      success: true,
      message: 'Logout successful'
    });
  });

  /**
   * 프로필 조회
   */
  getProfile = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user?.userId;
    
    if (!userId) {
      res.status(401).json({
        success: false,
        error: 'Unauthorized'
      });
      return;
    }
    
    const user = await this.authService.getProfile(userId);
    
    res.json({
      success: true,
      data: { user },
      message: 'Profile retrieved successfully'
    });
  });

  /**
   * 온라인 사용자 목록
   */
  getOnlineUsers = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const onlineUserIds = await this.authService.getOnlineUsers();
    
    res.json({
      success: true,
      data: { 
        onlineUsers: onlineUserIds,
        count: onlineUserIds.length 
      },
      message: 'Online users retrieved successfully'
    });
  });

  /**
   * 사용자 온라인 상태 확인
   */
  checkOnlineStatus = asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { userId } = req.params;
    
    if (!userId) {
      res.status(400).json({
        success: false,
        error: 'User ID is required'
      });
      return;
    }
    
    const isOnline = await this.authService.isUserOnline(userId);
    
    res.json({
      success: true,
      data: { 
        userId,
        isOnline 
      }
    });
  });
}

