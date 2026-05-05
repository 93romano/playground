import crypto from 'crypto';
import pgPool from '../config/database.js';
import { cache } from '../config/redis.js';

// CRUD operations for posts with CIA Triad and Big Data 3V principles
export class PostService {
  // CREATE - Create new post
  async createPost(userId, { title, content, mediaType, mediaUrl }) {
    const client = await pgPool.connect();

    try {
      // CIA Triad: Integrity - Generate checksum for content integrity
      const checksum = crypto
        .createHash('sha256')
        .update(content)
        .digest('hex');

      const result = await client.query(
        `INSERT INTO posts (user_id, title, content, media_type, media_url, checksum, published_at)
         VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
         RETURNING *`,
        [userId, title, content, mediaType, mediaUrl, checksum]
      );

      const post = result.rows[0];

      // Audit log - CIA Triad: Integrity
      await this.logAudit(client, 'posts', post.id, 'INSERT', null, post, userId);

      // Cache the new post - Big Data 3V: Velocity
      await cache.set(`post:${post.id}`, post, 3600);

      // Add to trending posts (sorted set) - Big Data 3V: Velocity
      await cache.zAdd('trending_posts', Date.now(), post.id);

      // Invalidate user posts cache
      await cache.del(`user_posts:${userId}`);

      // Publish event to activity stream - Big Data 3V: Velocity
      await cache.lPush('activity_stream', {
        type: 'post_created',
        userId,
        postId: post.id,
        timestamp: new Date(),
      });

      return post;
    } finally {
      client.release();
    }
  }

  // READ - Get post by ID with caching
  async getPostById(postId, userId = null) {
    // Try cache first - Big Data 3V: Velocity (fast retrieval)
    const cached = await cache.get(`post:${postId}`);
    if (cached) {
      // Track cache hit for analytics
      await cache.incr(`cache_hits:posts`);

      // Increment view count asynchronously - Big Data 3V: Velocity
      this.incrementViewCount(postId, userId).catch(console.error);

      return cached;
    }

    // Cache miss - fetch from database
    const client = await pgPool.connect();

    try {
      const result = await client.query(
        `SELECT p.*,
                u.username,
                u.email,
                COUNT(DISTINCT l.id) as like_count,
                COUNT(DISTINCT c.id) as comment_count
         FROM posts p
         JOIN users u ON p.user_id = u.id
         LEFT JOIN likes l ON p.id = l.post_id
         LEFT JOIN comments c ON p.id = c.post_id
         WHERE p.id = $1
         GROUP BY p.id, u.username, u.email`,
        [postId]
      );

      if (result.rows.length === 0) {
        throw new Error('Post not found');
      }

      const post = result.rows[0];

      // CIA Triad: Integrity - Verify checksum
      const calculatedChecksum = crypto
        .createHash('sha256')
        .update(post.content)
        .digest('hex');

      if (calculatedChecksum !== post.checksum) {
        console.error(`Integrity check failed for post ${postId}`);
        // You could implement recovery mechanisms here
      }

      // Cache for future requests - Big Data 3V: Velocity
      await cache.set(`post:${postId}`, post, 3600);
      await cache.incr(`cache_misses:posts`);

      // Increment view count - Big Data 3V: Velocity
      this.incrementViewCount(postId, userId).catch(console.error);

      return post;
    } finally {
      client.release();
    }
  }

  // READ - Get posts with pagination - Big Data 3V: Volume
  async getPosts({ page = 1, limit = 20, userId = null, sortBy = 'created_at' }) {
    const offset = (page - 1) * limit;
    const cacheKey = `posts:page_${page}:limit_${limit}:sort_${sortBy}`;

    // Try cache - Big Data 3V: Velocity
    const cached = await cache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const client = await pgPool.connect();

    try {
      let orderClause = 'p.created_at DESC';
      if (sortBy === 'popular') {
        orderClause = 'like_count DESC, view_count DESC';
      } else if (sortBy === 'views') {
        orderClause = 'p.view_count DESC';
      }

      const result = await client.query(
        `SELECT p.*,
                u.username,
                COUNT(DISTINCT l.id) as like_count,
                COUNT(DISTINCT c.id) as comment_count
         FROM posts p
         JOIN users u ON p.user_id = u.id
         LEFT JOIN likes l ON p.id = l.post_id
         LEFT JOIN comments c ON p.id = c.post_id
         ${userId ? 'WHERE p.user_id = $3' : ''}
         GROUP BY p.id, u.username
         ORDER BY ${orderClause}
         LIMIT $1 OFFSET $2`,
        userId ? [limit, offset, userId] : [limit, offset]
      );

      // Get total count - Big Data 3V: Volume
      const countResult = await client.query(
        `SELECT COUNT(*) as total FROM posts ${userId ? 'WHERE user_id = $1' : ''}`,
        userId ? [userId] : []
      );

      const response = {
        posts: result.rows,
        pagination: {
          page,
          limit,
          total: parseInt(countResult.rows[0].total),
          totalPages: Math.ceil(countResult.rows[0].total / limit),
        },
      };

      // Cache results - Big Data 3V: Velocity
      await cache.set(cacheKey, response, 300); // 5 minutes

      return response;
    } finally {
      client.release();
    }
  }

  // UPDATE - Update post
  async updatePost(postId, userId, updates) {
    const client = await pgPool.connect();

    try {
      // Get current post for audit
      const current = await client.query(
        `SELECT * FROM posts WHERE id = $1 AND user_id = $2`,
        [postId, userId]
      );

      if (current.rows.length === 0) {
        throw new Error('Post not found or unauthorized');
      }

      const oldPost = current.rows[0];

      // Build update query dynamically
      const allowedFields = ['title', 'content', 'media_type', 'media_url', 'status'];
      const updateFields = [];
      const values = [];
      let paramCount = 1;

      for (const [key, value] of Object.entries(updates)) {
        if (allowedFields.includes(key)) {
          updateFields.push(`${key} = $${paramCount}`);
          values.push(value);
          paramCount++;
        }
      }

      // CIA Triad: Integrity - Recalculate checksum if content changed
      if (updates.content) {
        const newChecksum = crypto
          .createHash('sha256')
          .update(updates.content)
          .digest('hex');
        updateFields.push(`checksum = $${paramCount}`);
        values.push(newChecksum);
        paramCount++;
      }

      values.push(postId, userId);

      const result = await client.query(
        `UPDATE posts
         SET ${updateFields.join(', ')}
         WHERE id = $${paramCount} AND user_id = $${paramCount + 1}
         RETURNING *`,
        values
      );

      const updatedPost = result.rows[0];

      // Audit log - CIA Triad: Integrity
      await this.logAudit(client, 'posts', postId, 'UPDATE', oldPost, updatedPost, userId);

      // Invalidate caches - Big Data 3V: Velocity
      await cache.del(`post:${postId}`);
      await cache.delPattern(`posts:*`);
      await cache.del(`user_posts:${userId}`);

      return updatedPost;
    } finally {
      client.release();
    }
  }

  // DELETE - Delete post
  async deletePost(postId, userId) {
    const client = await pgPool.connect();

    try {
      // Get post for audit
      const result = await client.query(
        `SELECT * FROM posts WHERE id = $1 AND user_id = $2`,
        [postId, userId]
      );

      if (result.rows.length === 0) {
        throw new Error('Post not found or unauthorized');
      }

      const post = result.rows[0];

      // Soft delete or hard delete - here we do hard delete
      await client.query(`DELETE FROM posts WHERE id = $1 AND user_id = $2`, [
        postId,
        userId,
      ]);

      // Audit log - CIA Triad: Integrity
      await this.logAudit(client, 'posts', postId, 'DELETE', post, null, userId);

      // Invalidate caches - Big Data 3V: Velocity
      await cache.del(`post:${postId}`);
      await cache.delPattern(`posts:*`);
      await cache.del(`user_posts:${userId}`);

      return { success: true, deletedPost: post };
    } finally {
      client.release();
    }
  }

  // Search posts - Big Data 3V: Variety (full-text search)
  async searchPosts(query, { page = 1, limit = 20 }) {
    const offset = (page - 1) * limit;
    const client = await pgPool.connect();

    try {
      // Using PostgreSQL full-text search - Big Data 3V: Variety
      const result = await client.query(
        `SELECT p.*,
                u.username,
                COUNT(DISTINCT l.id) as like_count,
                COUNT(DISTINCT c.id) as comment_count,
                ts_rank(to_tsvector('english', p.title || ' ' || p.content), plainto_tsquery('english', $1)) as rank
         FROM posts p
         JOIN users u ON p.user_id = u.id
         LEFT JOIN likes l ON p.id = l.post_id
         LEFT JOIN comments c ON p.id = c.post_id
         WHERE to_tsvector('english', p.title || ' ' || p.content) @@ plainto_tsquery('english', $1)
         GROUP BY p.id, u.username
         ORDER BY rank DESC, p.created_at DESC
         LIMIT $2 OFFSET $3`,
        [query, limit, offset]
      );

      return result.rows;
    } finally {
      client.release();
    }
  }

  // Get trending posts - Big Data 3V: Velocity
  async getTrendingPosts(limit = 10) {
    const cacheKey = 'trending_posts_list';

    // Check cache
    const cached = await cache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const client = await pgPool.connect();

    try {
      // Calculate trending score based on recent activity
      const result = await client.query(
        `SELECT p.*,
                u.username,
                COUNT(DISTINCT l.id) as like_count,
                COUNT(DISTINCT c.id) as comment_count,
                (COUNT(DISTINCT l.id) * 2 + COUNT(DISTINCT c.id) * 3 + p.view_count * 0.1) as trending_score
         FROM posts p
         JOIN users u ON p.user_id = u.id
         LEFT JOIN likes l ON p.id = l.post_id AND l.created_at > NOW() - INTERVAL '24 hours'
         LEFT JOIN comments c ON p.id = c.post_id AND c.created_at > NOW() - INTERVAL '24 hours'
         WHERE p.created_at > NOW() - INTERVAL '7 days'
         GROUP BY p.id, u.username
         ORDER BY trending_score DESC
         LIMIT $1`,
        [limit]
      );

      // Cache for 5 minutes
      await cache.set(cacheKey, result.rows, 300);

      return result.rows;
    } finally {
      client.release();
    }
  }

  // Increment view count - Big Data 3V: Velocity (high-frequency operation)
  async incrementViewCount(postId, userId = null) {
    // Use Redis for high-velocity counting
    const countKey = `post_views:${postId}`;
    const viewerKey = `post_viewers:${postId}`;

    // Increment view count
    await cache.incr(countKey);

    // Track unique viewers if userId provided
    if (userId) {
      await cache.hSet(viewerKey, userId, Date.now());
    }

    // Periodically sync to PostgreSQL (every 100 views)
    const currentCount = await cache.get(countKey);
    if (currentCount && parseInt(currentCount) % 100 === 0) {
      const client = await pgPool.connect();
      try {
        await client.query(
          `UPDATE posts SET view_count = view_count + 100 WHERE id = $1`,
          [postId]
        );
      } finally {
        client.release();
      }
    }
  }

  // Audit log helper
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

export default new PostService();
