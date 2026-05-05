import { UserRepository } from '@/models/User';
import { JWTUtil } from '@/utils/jwt';
import { redisClient } from '@/config/database';
import { User, AuthTokens, LoginRequest, RegisterRequest } from '@/types';
import { AppError } from '@/middleware/errorHandler';
import logger from '@/utils/logger';

export class AuthService {
  private userRepository: UserRepository;

  constructor() {
    this.userRepository = new UserRepository();
  }

  /**
   * 사용자 등록
   */
  async register(data: RegisterRequest): Promise<{ user: User; tokens: AuthTokens }> {
    const { username, email, password } = data;

    // 이메일 중복 확인
    if (await this.userRepository.existsByEmail(email)) {
      throw new AppError('Email already exists', 409, 'EMAIL_EXISTS');
    }

    // 사용자명 중복 확인
    if (await this.userRepository.existsByUsername(username)) {
      throw new AppError('Username already exists', 409, 'USERNAME_EXISTS');
    }

    // 사용자 생성
    const user = await this.userRepository.create(username, email, password);

    // JWT 토큰 생성
    const tokens = JWTUtil.generateTokens({
      userId: user.id,
      username: user.username
    });

    // Redis에 refresh token 저장
    await this.storeRefreshToken(user.id, tokens.refreshToken);

    logger.info('User registered successfully:', { userId: user.id, username });

    return { user, tokens };
  }

  /**
   * 로그인
   */
  async login(data: LoginRequest): Promise<{ user: User; tokens: AuthTokens }> {
    const { email, password } = data;

    // 사용자 조회 (비밀번호 포함)
    const userWithPassword = await this.userRepository.findByEmailWithPassword(email);
    
    if (!userWithPassword) {
      throw new AppError('Invalid email or password', 401, 'INVALID_CREDENTIALS');
    }

    // 비밀번호 확인
    const isValidPassword = await this.userRepository.verifyPassword(
      password, 
      userWithPassword.password
    );

    if (!isValidPassword) {
      throw new AppError('Invalid email or password', 401, 'INVALID_CREDENTIALS');
    }

    // 비밀번호 제거
    const { password: _, ...user } = userWithPassword;

    // JWT 토큰 생성
    const tokens = JWTUtil.generateTokens({
      userId: user.id,
      username: user.username
    });

    // Redis에 refresh token 저장
    await this.storeRefreshToken(user.id, tokens.refreshToken);

    logger.info('User logged in successfully:', { userId: user.id, username: user.username });

    return { user, tokens };
  }

  /**
   * 토큰 갱신
   */
  async refreshToken(refreshToken: string): Promise<AuthTokens> {
    try {
      // Refresh token 검증
      const payload = JWTUtil.verifyRefreshToken(refreshToken);

      // Redis에서 저장된 refresh token 확인
      const storedToken = await redisClient.get(`refresh_token:${payload.userId}`);
      
      if (!storedToken || storedToken !== refreshToken) {
        throw new AppError('Invalid refresh token', 401, 'INVALID_REFRESH_TOKEN');
      }

      // 사용자 존재 확인
      const user = await this.userRepository.findById(payload.userId);
      
      if (!user) {
        throw new AppError('User not found', 404, 'USER_NOT_FOUND');
      }

      // 새 토큰 생성
      const tokens = JWTUtil.generateTokens({
        userId: user.id,
        username: user.username
      });

      // 새 refresh token 저장
      await this.storeRefreshToken(user.id, tokens.refreshToken);

      logger.info('Token refreshed:', { userId: user.id });

      return tokens;
      
    } catch (error) {
      logger.warn('Token refresh failed:', error);
      throw new AppError('Invalid refresh token', 401, 'INVALID_REFRESH_TOKEN');
    }
  }

  /**
   * 로그아웃
   */
  async logout(userId: string): Promise<void> {
    try {
      // Redis에서 refresh token 삭제
      await redisClient.del(`refresh_token:${userId}`);
      
      // 온라인 상태 제거
      await redisClient.sRem('online_users', userId);

      logger.info('User logged out:', { userId });
      
    } catch (error) {
      logger.error('Logout error:', error);
      // 로그아웃은 실패해도 계속 진행
    }
  }

  /**
   * 사용자 정보 조회
   */
  async getProfile(userId: string): Promise<User> {
    const user = await this.userRepository.findById(userId);
    
    if (!user) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    return user;
  }

  /**
   * 비밀번호 변경
   */
  async changePassword(userId: string, currentPassword: string, newPassword: string): Promise<void> {
    // 현재 사용자 조회 (비밀번호 포함)
    const user = await this.userRepository.findById(userId);
    
    if (!user) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    const userWithPassword = await this.userRepository.findByEmailWithPassword(user.email);
    
    if (!userWithPassword) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    // 현재 비밀번호 확인
    const isValidPassword = await this.userRepository.verifyPassword(
      currentPassword, 
      userWithPassword.password
    );

    if (!isValidPassword) {
      throw new AppError('Invalid current password', 401, 'INVALID_PASSWORD');
    }

    // TODO: 비밀번호 업데이트 메서드 구현 필요
    // await this.userRepository.updatePassword(userId, newPassword);

    logger.info('Password changed:', { userId });
  }

  /**
   * Redis에 refresh token 저장
   */
  private async storeRefreshToken(userId: string, refreshToken: string): Promise<void> {
    const key = `refresh_token:${userId}`;
    
    // 30일 TTL
    await redisClient.setEx(key, 30 * 24 * 60 * 60, refreshToken);
  }

  /**
   * 온라인 상태 설정
   */
  async setOnlineStatus(userId: string, isOnline: boolean): Promise<void> {
    try {
      if (isOnline) {
        await redisClient.sAdd('online_users', userId);
        await redisClient.setEx(`user_last_seen:${userId}`, 300, Date.now().toString());
      } else {
        await redisClient.sRem('online_users', userId);
        await redisClient.del(`user_last_seen:${userId}`);
      }
      
      logger.debug('Online status updated:', { userId, isOnline });
      
    } catch (error) {
      logger.error('Failed to update online status:', error);
    }
  }

  /**
   * 온라인 사용자 목록 조회
   */
  async getOnlineUsers(): Promise<string[]> {
    try {
      return await redisClient.sMembers('online_users');
    } catch (error) {
      logger.error('Failed to get online users:', error);
      return [];
    }
  }

  /**
   * 사용자 온라인 상태 확인
   */
  async isUserOnline(userId: string): Promise<boolean> {
    try {
      return await redisClient.sIsMember('online_users', userId);
    } catch (error) {
      logger.error('Failed to check online status:', error);
      return false;
    }
  }
}

