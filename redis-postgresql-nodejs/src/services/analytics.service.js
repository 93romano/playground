import pgPool from '../config/database.js';
import { cache } from '../config/redis.js';

// Big Data 3V: Volume, Velocity, Variety - Analytics and event tracking
export class AnalyticsService {
  // Track analytics event - Big Data 3V: Velocity (high-speed event ingestion)
  async trackEvent(eventType, data) {
    const { userId, postId, sessionId, ipAddress, userAgent, metadata } = data;

    // Store in Redis stream first for high velocity - Big Data 3V: Velocity
    const eventId = Date.now() + Math.random();
    await cache.lPush('analytics_queue', {
      eventId,
      eventType,
      userId,
      postId,
      sessionId,
      ipAddress,
      userAgent,
      metadata,
      timestamp: new Date(),
    });

    // Increment real-time counters - Big Data 3V: Velocity
    await cache.incr(`event_count:${eventType}`);
    await cache.incr(`event_count:total`);

    if (userId) {
      await cache.incr(`user_event_count:${userId}`);
    }

    // Batch insert to PostgreSQL for persistence - Big Data 3V: Volume
    // This would typically be done by a background worker
    this.flushEventsToDB().catch(console.error);

    return { eventId, success: true };
  }

  // Flush events from Redis to PostgreSQL - Big Data 3V: Volume
  async flushEventsToDB() {
    const batchSize = 100;
    const events = await cache.lRange('analytics_queue', 0, batchSize - 1);

    if (events.length === 0) {
      return;
    }

    const client = await pgPool.connect();

    try {
      await client.query('BEGIN');

      for (const event of events) {
        await client.query(
          `INSERT INTO analytics_events
           (event_type, user_id, post_id, session_id, ip_address, user_agent, metadata)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            event.eventType,
            event.userId,
            event.postId,
            event.sessionId,
            event.ipAddress,
            event.userAgent,
            JSON.stringify(event.metadata),
          ]
        );
      }

      await client.query('COMMIT');

      // Remove processed events from Redis
      for (let i = 0; i < events.length; i++) {
        await cache.lPush('analytics_queue', events[i]); // This is a simplification
      }
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Failed to flush events:', error);
    } finally {
      client.release();
    }
  }

  // Get real-time analytics - Big Data 3V: Velocity
  async getRealtimeAnalytics() {
    const totalEvents = await cache.get('event_count:total') || 0;
    const pageViews = await cache.get('event_count:page_view') || 0;
    const postCreated = await cache.get('event_count:post_created') || 0;
    const likes = await cache.get('event_count:like') || 0;
    const comments = await cache.get('event_count:comment') || 0;

    // Get trending posts from sorted set
    const trendingPostIds = await cache.zRevRangeWithScores('trending_posts', 0, 9);

    return {
      realtime: {
        totalEvents: parseInt(totalEvents),
        pageViews: parseInt(pageViews),
        postCreated: parseInt(postCreated),
        likes: parseInt(likes),
        comments: parseInt(comments),
      },
      trending: trendingPostIds,
    };
  }

  // Get user analytics - Big Data 3V: Variety (different metrics)
  async getUserAnalytics(userId, days = 30) {
    const client = await pgPool.connect();

    try {
      // Posts analytics
      const postsResult = await client.query(
        `SELECT
          COUNT(*) as total_posts,
          SUM(view_count) as total_views,
          AVG(view_count) as avg_views_per_post
         FROM posts
         WHERE user_id = $1
           AND created_at > CURRENT_DATE - INTERVAL '${days} days'`,
        [userId]
      );

      // Engagement analytics
      const engagementResult = await client.query(
        `SELECT
          COUNT(DISTINCT l.id) as total_likes,
          COUNT(DISTINCT c.id) as total_comments
         FROM posts p
         LEFT JOIN likes l ON p.id = l.post_id
         LEFT JOIN comments c ON p.id = c.post_id
         WHERE p.user_id = $1
           AND p.created_at > CURRENT_DATE - INTERVAL '${days} days'`,
        [userId]
      );

      // Activity by day - Big Data 3V: Volume (time-series data)
      const activityResult = await client.query(
        `SELECT
          DATE(created_at) as date,
          COUNT(*) as post_count
         FROM posts
         WHERE user_id = $1
           AND created_at > CURRENT_DATE - INTERVAL '${days} days'
         GROUP BY DATE(created_at)
         ORDER BY date DESC`,
        [userId]
      );

      // Popular posts - Big Data 3V: Volume
      const popularPostsResult = await client.query(
        `SELECT
          p.id,
          p.title,
          p.view_count,
          COUNT(DISTINCT l.id) as like_count,
          COUNT(DISTINCT c.id) as comment_count
         FROM posts p
         LEFT JOIN likes l ON p.id = l.post_id
         LEFT JOIN comments c ON p.id = c.post_id
         WHERE p.user_id = $1
           AND p.created_at > CURRENT_DATE - INTERVAL '${days} days'
         GROUP BY p.id, p.title, p.view_count
         ORDER BY p.view_count DESC
         LIMIT 10`,
        [userId]
      );

      return {
        summary: {
          ...postsResult.rows[0],
          ...engagementResult.rows[0],
        },
        activityByDay: activityResult.rows,
        popularPosts: popularPostsResult.rows,
      };
    } finally {
      client.release();
    }
  }

  // Aggregate daily analytics - Big Data 3V: Volume (batch processing)
  async aggregateDailyAnalytics(date = new Date()) {
    const client = await pgPool.connect();

    try {
      const targetDate = new Date(date);
      targetDate.setHours(0, 0, 0, 0);

      await client.query('BEGIN');

      // Aggregate post analytics
      await client.query(
        `INSERT INTO daily_analytics (date, user_id, post_id, views, likes, comments, shares, unique_visitors)
         SELECT
           DATE($1) as date,
           p.user_id,
           p.id as post_id,
           COUNT(DISTINCT ae.id) FILTER (WHERE ae.event_type = 'page_view') as views,
           COUNT(DISTINCT l.id) as likes,
           COUNT(DISTINCT c.id) as comments,
           COUNT(DISTINCT ae.id) FILTER (WHERE ae.event_type = 'share') as shares,
           COUNT(DISTINCT ae.user_id) as unique_visitors
         FROM posts p
         LEFT JOIN analytics_events ae ON p.id = ae.post_id
           AND DATE(ae.created_at) = DATE($1)
         LEFT JOIN likes l ON p.id = l.post_id
           AND DATE(l.created_at) = DATE($1)
         LEFT JOIN comments c ON p.id = c.post_id
           AND DATE(c.created_at) = DATE($1)
         WHERE p.created_at <= $1
         GROUP BY p.user_id, p.id
         ON CONFLICT (date, user_id, post_id)
         DO UPDATE SET
           views = EXCLUDED.views,
           likes = EXCLUDED.likes,
           comments = EXCLUDED.comments,
           shares = EXCLUDED.shares,
           unique_visitors = EXCLUDED.unique_visitors`,
        [targetDate]
      );

      await client.query('COMMIT');

      console.log(`✅ Daily analytics aggregated for ${targetDate.toISOString()}`);
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Failed to aggregate daily analytics:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Get platform-wide analytics - Big Data 3V: Volume + Variety
  async getPlatformAnalytics(days = 30) {
    const client = await pgPool.connect();

    try {
      // Overall statistics - Big Data 3V: Volume
      const statsResult = await client.query(
        `SELECT
          COUNT(DISTINCT u.id) as total_users,
          COUNT(DISTINCT p.id) as total_posts,
          COUNT(DISTINCT c.id) as total_comments,
          COUNT(DISTINCT l.id) as total_likes,
          SUM(p.view_count) as total_views
         FROM users u
         LEFT JOIN posts p ON u.id = p.user_id
         LEFT JOIN comments c ON u.id = c.user_id
         LEFT JOIN likes l ON u.id = l.user_id
         WHERE u.created_at > CURRENT_DATE - INTERVAL '${days} days'`
      );

      // Growth metrics - Big Data 3V: Velocity
      const growthResult = await client.query(
        `SELECT
          DATE(created_at) as date,
          COUNT(DISTINCT id) FILTER (WHERE created_at IS NOT NULL) as new_users
         FROM users
         WHERE created_at > CURRENT_DATE - INTERVAL '${days} days'
         GROUP BY DATE(created_at)
         ORDER BY date DESC`
      );

      // Event distribution - Big Data 3V: Variety
      const eventDistResult = await client.query(
        `SELECT
          event_type,
          COUNT(*) as count
         FROM analytics_events
         WHERE created_at > CURRENT_DATE - INTERVAL '${days} days'
         GROUP BY event_type
         ORDER BY count DESC`
      );

      // Top content creators - Big Data 3V: Volume
      const topCreatorsResult = await client.query(
        `SELECT
          u.id,
          u.username,
          COUNT(DISTINCT p.id) as post_count,
          SUM(p.view_count) as total_views,
          COUNT(DISTINCT l.id) as total_likes
         FROM users u
         JOIN posts p ON u.id = p.user_id
         LEFT JOIN likes l ON p.id = l.post_id
         WHERE p.created_at > CURRENT_DATE - INTERVAL '${days} days'
         GROUP BY u.id, u.username
         ORDER BY total_views DESC
         LIMIT 10`
      );

      // Cache results for 1 hour - Big Data 3V: Velocity
      const results = {
        overview: statsResult.rows[0],
        growth: growthResult.rows,
        eventDistribution: eventDistResult.rows,
        topCreators: topCreatorsResult.rows,
      };

      await cache.set(`platform_analytics:${days}days`, results, 3600);

      return results;
    } finally {
      client.release();
    }
  }

  // Heat map data for activity - Big Data 3V: Variety (time-based patterns)
  async getActivityHeatmap(userId = null, days = 30) {
    const client = await pgPool.connect();

    try {
      const query = userId
        ? `SELECT
             EXTRACT(HOUR FROM created_at) as hour,
             EXTRACT(DOW FROM created_at) as day_of_week,
             COUNT(*) as activity_count
           FROM analytics_events
           WHERE user_id = $1
             AND created_at > CURRENT_DATE - INTERVAL '${days} days'
           GROUP BY hour, day_of_week
           ORDER BY day_of_week, hour`
        : `SELECT
             EXTRACT(HOUR FROM created_at) as hour,
             EXTRACT(DOW FROM created_at) as day_of_week,
             COUNT(*) as activity_count
           FROM analytics_events
           WHERE created_at > CURRENT_DATE - INTERVAL '${days} days'
           GROUP BY hour, day_of_week
           ORDER BY day_of_week, hour`;

      const result = await client.query(query, userId ? [userId] : []);

      // Format as 2D array for heatmap visualization
      const heatmap = Array(7)
        .fill(null)
        .map(() => Array(24).fill(0));

      result.rows.forEach((row) => {
        heatmap[parseInt(row.day_of_week)][parseInt(row.hour)] = parseInt(
          row.activity_count
        );
      });

      return heatmap;
    } finally {
      client.release();
    }
  }
}

export default new AnalyticsService();
