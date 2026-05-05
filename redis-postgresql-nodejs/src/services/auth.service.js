import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import crypto from 'crypto';
import pgPool from '../config/database.js';
import { cache } from '../config/redis.js';

// CIA Triad: Confidentiality - Secure authentication and authorization
export class AuthService {
  constructor() {
    this.bcryptRounds = parseInt(process.env.BCRYPT_ROUNDS) || 12;
    this.jwtSecret = process.env.JWT_SECRET;
    this.jwtExpiresIn = process.env.JWT_EXPIRES_IN || '24h';
  }

  // Register new user - CREATE operation
  async register(username, email, password, role = 'user') {
    const client = await pgPool.connect();

    try {
      // CIA Triad: Confidentiality - Hash password
      const passwordHash = await bcrypt.hash(password, this.bcryptRounds);

      const result = await client.query(
        `INSERT INTO users (username, email, password_hash, role)
         VALUES ($1, $2, $3, $4)
         RETURNING id, username, email, role, created_at`,
        [username, email, passwordHash, role]
      );

      // CIA Triad: Integrity - Audit log
      await this.logAudit(client, 'users', result.rows[0].id, 'INSERT', null, {
        username,
        email,
        role,
      });

      return result.rows[0];
    } catch (error) {
      if (error.code === '23505') {
        // Unique constraint violation
        throw new Error('Username or email already exists');
      }
      throw error;
    } finally {
      client.release();
    }
  }

  // Login user - READ operation with security features
  async login(email, password, ipAddress) {
    const client = await pgPool.connect();

    try {
      // CIA Triad: Availability - Check rate limiting
      const rateLimit = await cache.checkRateLimit(`login:${ipAddress}`, 5, 300);
      if (!rateLimit.allowed) {
        throw new Error('Too many login attempts. Please try again later.');
      }

      // Fetch user
      const result = await client.query(
        `SELECT id, username, email, password_hash, role, is_active,
                failed_login_attempts, account_locked_until
         FROM users WHERE email = $1`,
        [email]
      );

      if (result.rows.length === 0) {
        throw new Error('Invalid credentials');
      }

      const user = result.rows[0];

      // Check if account is locked
      if (user.account_locked_until && new Date(user.account_locked_until) > new Date()) {
        throw new Error('Account is temporarily locked. Please try again later.');
      }

      // Check if account is active
      if (!user.is_active) {
        throw new Error('Account is deactivated');
      }

      // Verify password - CIA Triad: Confidentiality
      const isValidPassword = await bcrypt.compare(password, user.password_hash);

      if (!isValidPassword) {
        // Increment failed login attempts
        const newFailedAttempts = user.failed_login_attempts + 1;
        const lockUntil = newFailedAttempts >= 5
          ? new Date(Date.now() + 15 * 60 * 1000) // Lock for 15 minutes
          : null;

        await client.query(
          `UPDATE users
           SET failed_login_attempts = $1,
               account_locked_until = $2
           WHERE id = $3`,
          [newFailedAttempts, lockUntil, user.id]
        );

        throw new Error('Invalid credentials');
      }

      // Reset failed attempts and update last login
      await client.query(
        `UPDATE users
         SET failed_login_attempts = 0,
             account_locked_until = NULL,
             last_login = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [user.id]
      );

      // Generate JWT token - CIA Triad: Confidentiality
      const token = jwt.sign(
        {
          userId: user.id,
          username: user.username,
          email: user.email,
          role: user.role,
        },
        this.jwtSecret,
        { expiresIn: this.jwtExpiresIn }
      );

      // Cache user session - Big Data 3V: Velocity
      await cache.set(`session:${user.id}`, {
        userId: user.id,
        username: user.username,
        email: user.email,
        role: user.role,
      }, 86400); // 24 hours

      // Audit log
      await this.logAudit(client, 'users', user.id, 'LOGIN', null, {
        ipAddress,
        timestamp: new Date(),
      });

      return {
        token,
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          role: user.role,
        },
      };
    } finally {
      client.release();
    }
  }

  // Verify JWT token - CIA Triad: Confidentiality & Integrity
  async verifyToken(token) {
    try {
      const decoded = jwt.verify(token, this.jwtSecret);

      // Check cache for session - Big Data 3V: Velocity
      const cachedSession = await cache.get(`session:${decoded.userId}`);
      if (cachedSession) {
        return cachedSession;
      }

      // Fallback to database
      const client = await pgPool.connect();
      try {
        const result = await client.query(
          `SELECT id, username, email, role, is_active
           FROM users WHERE id = $1`,
          [decoded.userId]
        );

        if (result.rows.length === 0 || !result.rows[0].is_active) {
          throw new Error('Invalid token');
        }

        return result.rows[0];
      } finally {
        client.release();
      }
    } catch (error) {
      throw new Error('Invalid or expired token');
    }
  }

  // Logout - DELETE session
  async logout(userId) {
    await cache.del(`session:${userId}`);
  }

  // Change password - UPDATE operation
  async changePassword(userId, oldPassword, newPassword) {
    const client = await pgPool.connect();

    try {
      // Verify old password
      const result = await client.query(
        `SELECT password_hash FROM users WHERE id = $1`,
        [userId]
      );

      if (result.rows.length === 0) {
        throw new Error('User not found');
      }

      const isValidPassword = await bcrypt.compare(
        oldPassword,
        result.rows[0].password_hash
      );

      if (!isValidPassword) {
        throw new Error('Invalid current password');
      }

      // Hash new password - CIA Triad: Confidentiality
      const newPasswordHash = await bcrypt.hash(newPassword, this.bcryptRounds);

      await client.query(
        `UPDATE users SET password_hash = $1 WHERE id = $2`,
        [newPasswordHash, userId]
      );

      // Audit log - CIA Triad: Integrity
      await this.logAudit(client, 'users', userId, 'PASSWORD_CHANGE', null, {
        timestamp: new Date(),
      });

      // Invalidate session cache
      await cache.del(`session:${userId}`);

      return { success: true };
    } finally {
      client.release();
    }
  }

  // Audit log helper - CIA Triad: Integrity & Accountability
  async logAudit(client, tableName, recordId, operation, oldData, newData, userId = null) {
    try {
      await client.query(
        `INSERT INTO audit_logs (table_name, record_id, operation, user_id, old_data, new_data)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [tableName, recordId, operation, userId, JSON.stringify(oldData), JSON.stringify(newData)]
      );
    } catch (error) {
      console.error('Audit log error:', error);
    }
  }
}

export default new AuthService();
