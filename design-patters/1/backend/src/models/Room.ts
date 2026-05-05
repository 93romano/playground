import { Pool } from 'pg';
import { Room, User } from '@/types';
import { pool } from '@/config/database';
import logger from '@/utils/logger';

export class RoomRepository {
  private db: Pool;

  constructor() {
    this.db = pool;
  }

  /**
   * 채팅방 생성
   */
  async create(name: string, createdBy: string, description?: string): Promise<Room> {
    const client = await this.db.connect();
    
    try {
      await client.query('BEGIN');

      // 채팅방 생성
      const roomResult = await client.query(
        `INSERT INTO rooms (name, description, created_by) 
         VALUES ($1, $2, $3) 
         RETURNING id, name, description, created_by, created_at, updated_at`,
        [name, description || null, createdBy]
      );

      const room = roomResult.rows[0] as Room;

      // 생성자를 자동으로 멤버로 추가
      await client.query(
        `INSERT INTO room_members (user_id, room_id) VALUES ($1, $2)`,
        [createdBy, room.id]
      );

      await client.query('COMMIT');

      logger.info('Room created:', { roomId: room.id, name, createdBy });
      return room;
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * ID로 채팅방 찾기
   */
  async findById(id: string): Promise<Room | null> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT id, name, description, created_by, created_at, updated_at FROM rooms WHERE id = $1',
        [id]
      );

      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * 모든 채팅방 목록 조회
   */
  async findAll(limit: number = 50, offset: number = 0): Promise<Room[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT r.id, r.name, r.description, r.created_by, r.created_at, r.updated_at,
                COUNT(rm.user_id) as member_count
         FROM rooms r
         LEFT JOIN room_members rm ON r.id = rm.room_id
         GROUP BY r.id, r.name, r.description, r.created_by, r.created_at, r.updated_at
         ORDER BY r.created_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
      );

      return result.rows;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자가 참여한 채팅방 목록
   */
  async findByUserId(userId: string): Promise<Room[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT r.id, r.name, r.description, r.created_by, r.created_at, r.updated_at
         FROM rooms r
         INNER JOIN room_members rm ON r.id = rm.room_id
         WHERE rm.user_id = $1
         ORDER BY rm.joined_at DESC`,
        [userId]
      );

      return result.rows;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자를 채팅방에 추가
   */
  async addMember(roomId: string, userId: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      await client.query(
        `INSERT INTO room_members (user_id, room_id) 
         VALUES ($1, $2) 
         ON CONFLICT (user_id, room_id) DO NOTHING`,
        [userId, roomId]
      );

      logger.info('User added to room:', { roomId, userId });
      return true;
      
    } catch (error) {
      logger.error('Failed to add user to room:', error);
      return false;
    } finally {
      client.release();
    }
  }

  /**
   * 사용자를 채팅방에서 제거
   */
  async removeMember(roomId: string, userId: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'DELETE FROM room_members WHERE user_id = $1 AND room_id = $2',
        [userId, roomId]
      );

      logger.info('User removed from room:', { roomId, userId });
      return result.rowCount > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 채팅방 멤버 목록 조회
   */
  async getMembers(roomId: string): Promise<User[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT u.id, u.username, u.email, u.created_at, u.updated_at, rm.joined_at
         FROM users u
         INNER JOIN room_members rm ON u.id = rm.user_id
         WHERE rm.room_id = $1
         ORDER BY rm.joined_at ASC`,
        [roomId]
      );

      return result.rows;
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자가 채팅방 멤버인지 확인
   */
  async isMember(roomId: string, userId: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT 1 FROM room_members WHERE user_id = $1 AND room_id = $2',
        [userId, roomId]
      );

      return result.rows.length > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 채팅방 업데이트
   */
  async update(id: string, updates: Partial<Pick<Room, 'name' | 'description'>>): Promise<Room | null> {
    const client = await this.db.connect();
    
    try {
      const updateFields: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      if (updates.name) {
        updateFields.push(`name = $${paramIndex++}`);
        values.push(updates.name);
      }

      if (updates.description !== undefined) {
        updateFields.push(`description = $${paramIndex++}`);
        values.push(updates.description);
      }

      if (updateFields.length === 0) {
        return this.findById(id);
      }

      updateFields.push(`updated_at = CURRENT_TIMESTAMP`);
      values.push(id);

      const result = await client.query(
        `UPDATE rooms SET ${updateFields.join(', ')} 
         WHERE id = $${paramIndex} 
         RETURNING id, name, description, created_by, created_at, updated_at`,
        values
      );

      logger.info('Room updated:', { roomId: id, updates });
      return result.rows[0] || null;
      
    } finally {
      client.release();
    }
  }

  /**
   * 채팅방 삭제
   */
  async delete(id: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      await client.query('BEGIN');

      // 관련 데이터 삭제 (외래키 제약으로 인해 순서 중요)
      await client.query('DELETE FROM room_members WHERE room_id = $1', [id]);
      await client.query('DELETE FROM messages WHERE room_id = $1', [id]);
      
      const result = await client.query('DELETE FROM rooms WHERE id = $1', [id]);

      await client.query('COMMIT');

      logger.info('Room deleted:', { roomId: id });
      return result.rowCount > 0;
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * 이름으로 채팅방 검색
   */
  async searchByName(query: string, limit: number = 20): Promise<Room[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT r.id, r.name, r.description, r.created_by, r.created_at, r.updated_at,
                COUNT(rm.user_id) as member_count
         FROM rooms r
         LEFT JOIN room_members rm ON r.id = rm.room_id
         WHERE r.name ILIKE $1
         GROUP BY r.id, r.name, r.description, r.created_by, r.created_at, r.updated_at
         ORDER BY member_count DESC, r.created_at DESC
         LIMIT $2`,
        [`%${query}%`, limit]
      );

      return result.rows;
      
    } finally {
      client.release();
    }
  }
}

