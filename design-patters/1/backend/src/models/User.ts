import { Pool } from 'pg';
import bcrypt from 'bcryptjs';
import { User } from '@/types';
import { pool } from '@/config/database';
import logger from '@/utils/logger';

export class UserRepository {
  private db: Pool;

  constructor() {
    this.db = pool;
  }

  /**
   * 사용자 생성
   */
  async create(username: string, email: string, password: string): Promise<User> {
    const client = await this.db.connect();
    
    try {
      // 비밀번호 해시화
      const saltRounds = 12;
      const hashedPassword = await bcrypt.hash(password, saltRounds);

      const result = await client.query(
        `INSERT INTO users (username, email, password) 
         VALUES ($1, $2, $3) 
         RETURNING id, username, email, created_at, updated_at`,
        [username, email, hashedPassword]
      );

      logger.info('User created:', { userId: result.rows[0]?.id, username });
      return result.rows[0] as User;
      
    } finally {
      client.release();
    }
  }

  /**
   * 이메일로 사용자 찾기 (비밀번호 포함)
   */
  async findByEmailWithPassword(email: string): Promise<(User & { password: string }) | null> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT id, username, email, password, created_at, updated_at FROM users WHERE email = $1',
        [email]
      );

      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * ID로 사용자 찾기
   */
  async findById(id: string): Promise<User | null> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT id, username, email, created_at, updated_at FROM users WHERE id = $1',
        [id]
      );

      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자명으로 사용자 찾기
   */
  async findByUsername(username: string): Promise<User | null> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT id, username, email, created_at, updated_at FROM users WHERE username = $1',
        [username]
      );

      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * 이메일 중복 확인
   */
  async existsByEmail(email: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT 1 FROM users WHERE email = $1',
        [email]
      );

      return result.rows.length > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자명 중복 확인
   */
  async existsByUsername(username: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT 1 FROM users WHERE username = $1',
        [username]
      );

      return result.rows.length > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 비밀번호 검증
   */
  async verifyPassword(plainPassword: string, hashedPassword: string): Promise<boolean> {
    return bcrypt.compare(plainPassword, hashedPassword);
  }

  /**
   * 사용자 업데이트
   */
  async update(id: string, updates: Partial<Pick<User, 'username' | 'email'>>): Promise<User | null> {
    const client = await this.db.connect();
    
    try {
      const updateFields: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      if (updates.username) {
        updateFields.push(`username = $${paramIndex++}`);
        values.push(updates.username);
      }

      if (updates.email) {
        updateFields.push(`email = $${paramIndex++}`);
        values.push(updates.email);
      }

      if (updateFields.length === 0) {
        return this.findById(id);
      }

      updateFields.push(`updated_at = CURRENT_TIMESTAMP`);
      values.push(id);

      const result = await client.query(
        `UPDATE users SET ${updateFields.join(', ')} 
         WHERE id = $${paramIndex} 
         RETURNING id, username, email, created_at, updated_at`,
        values
      );

      logger.info('User updated:', { userId: id, updates });
      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자 삭제
   */
  async delete(id: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'DELETE FROM users WHERE id = $1',
        [id]
      );

      logger.info('User deleted:', { userId: id });
      return result.rowCount > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 온라인 사용자 목록 조회 (특정 채팅방)
   */
  async getOnlineUsersInRoom(roomId: string): Promise<User[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT DISTINCT u.id, u.username, u.email, u.created_at, u.updated_at
         FROM users u
         INNER JOIN room_members rm ON u.id = rm.user_id
         WHERE rm.room_id = $1`,
        [roomId]
      );

      return result.rows;
      
    } finally {
      client.release();
    }
  }
}

