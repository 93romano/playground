import { Pool } from 'pg';
import { Message, User } from '@/types';
import { pool } from '@/config/database';
import logger from '@/utils/logger';

export interface MessageWithUser extends Message {
  user: User;
}

export class MessageRepository {
  private db: Pool;

  constructor() {
    this.db = pool;
  }

  /**
   * 메시지 생성
   */
  async create(content: string, userId: string, roomId: string): Promise<MessageWithUser> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `INSERT INTO messages (content, user_id, room_id) 
         VALUES ($1, $2, $3) 
         RETURNING id, content, user_id, room_id, created_at`,
        [content, userId, roomId]
      );

      const message = result.rows[0];

      // 사용자 정보 조회
      const userResult = await client.query(
        'SELECT id, username, email, created_at, updated_at FROM users WHERE id = $1',
        [userId]
      );

      const messageWithUser: MessageWithUser = {
        ...message,
        user: userResult.rows[0]
      };

      logger.info('Message created:', { messageId: message.id, userId, roomId });
      return messageWithUser;
      
    } finally {
      client.release();
    }
  }

  /**
   * ID로 메시지 찾기 (사용자 정보 포함)
   */
  async findById(id: string): Promise<MessageWithUser | null> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
                u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at
         FROM messages m
         INNER JOIN users u ON m.user_id = u.id
         WHERE m.id = $1`,
        [id]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return {
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      };
      
    } finally {
      client.release();
    }
  }

  /**
   * 채팅방의 메시지 목록 조회 (페이지네이션)
   */
  async findByRoomId(
    roomId: string, 
    limit: number = 50, 
    offset: number = 0
  ): Promise<MessageWithUser[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
                u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at
         FROM messages m
         INNER JOIN users u ON m.user_id = u.id
         WHERE m.room_id = $1
         ORDER BY m.created_at DESC
         LIMIT $2 OFFSET $3`,
        [roomId, limit, offset]
      );

      return result.rows.map(row => ({
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      }));
      
    } finally {
      client.release();
    }
  }

  /**
   * 특정 시간 이후의 메시지 조회 (실시간 동기화용)
   */
  async findByRoomIdAfter(roomId: string, after: Date): Promise<MessageWithUser[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
                u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at
         FROM messages m
         INNER JOIN users u ON m.user_id = u.id
         WHERE m.room_id = $1 AND m.created_at > $2
         ORDER BY m.created_at ASC`,
        [roomId, after]
      );

      return result.rows.map(row => ({
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      }));
      
    } finally {
      client.release();
    }
  }

  /**
   * 사용자의 메시지 목록 조회
   */
  async findByUserId(userId: string, limit: number = 50, offset: number = 0): Promise<MessageWithUser[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
                u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at,
                r.name as room_name
         FROM messages m
         INNER JOIN users u ON m.user_id = u.id
         INNER JOIN rooms r ON m.room_id = r.id
         WHERE m.user_id = $1
         ORDER BY m.created_at DESC
         LIMIT $2 OFFSET $3`,
        [userId, limit, offset]
      );

      return result.rows.map(row => ({
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      }));
      
    } finally {
      client.release();
    }
  }

  /**
   * 메시지 검색 (내용 기반)
   */
  async search(query: string, roomId?: string, limit: number = 20): Promise<MessageWithUser[]> {
    const client = await this.db.connect();
    
    try {
      let sql = `
        SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
               u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at,
               r.name as room_name
        FROM messages m
        INNER JOIN users u ON m.user_id = u.id
        INNER JOIN rooms r ON m.room_id = r.id
        WHERE m.content ILIKE $1
      `;
      
      const params: any[] = [`%${query}%`];
      
      if (roomId) {
        sql += ' AND m.room_id = $2';
        params.push(roomId);
      }
      
      sql += ' ORDER BY m.created_at DESC LIMIT $' + (params.length + 1);
      params.push(limit);

      const result = await client.query(sql, params);

      return result.rows.map(row => ({
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      }));
      
    } finally {
      client.release();
    }
  }

  /**
   * 메시지 삭제
   */
  async delete(id: string, userId: string): Promise<boolean> {
    const client = await this.db.connect();
    
    try {
      // 자신의 메시지만 삭제 가능
      const result = await client.query(
        'DELETE FROM messages WHERE id = $1 AND user_id = $2',
        [id, userId]
      );

      logger.info('Message deleted:', { messageId: id, userId });
      return result.rowCount > 0;
      
    } finally {
      client.release();
    }
  }

  /**
   * 채팅방의 메시지 수 조회
   */
  async countByRoomId(roomId: string): Promise<number> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        'SELECT COUNT(*) as count FROM messages WHERE room_id = $1',
        [roomId]
      );

      return parseInt(result.rows[0]?.count || '0', 10);
      
    } finally {
      client.release();
    }
  }

  /**
   * 특정 날짜 범위의 메시지 조회
   */
  async findByDateRange(
    roomId: string,
    startDate: Date,
    endDate: Date,
    limit: number = 100
  ): Promise<MessageWithUser[]> {
    const client = await this.db.connect();
    
    try {
      const result = await client.query(
        `SELECT m.id, m.content, m.user_id, m.room_id, m.created_at,
                u.id as user_id, u.username, u.email, u.created_at as user_created_at, u.updated_at as user_updated_at
         FROM messages m
         INNER JOIN users u ON m.user_id = u.id
         WHERE m.room_id = $1 AND m.created_at BETWEEN $2 AND $3
         ORDER BY m.created_at ASC
         LIMIT $4`,
        [roomId, startDate, endDate, limit]
      );

      return result.rows.map(row => ({
        id: row.id,
        content: row.content,
        user_id: row.user_id,
        room_id: row.room_id,
        created_at: row.created_at,
        user: {
          id: row.user_id,
          username: row.username,
          email: row.email,
          created_at: row.user_created_at,
          updated_at: row.user_updated_at
        }
      }));
      
    } finally {
      client.release();
    }
  }
}

