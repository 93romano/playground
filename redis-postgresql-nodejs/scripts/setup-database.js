import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
});

const setupDatabase = async () => {
  const client = await pool.connect();

  try {
    console.log('🚀 Starting database setup...');

    // Enable extensions
    await client.query(`
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
      CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For text search optimization
    `);

    // Users table - CIA Triad: Confidentiality (password hashing, sensitive data)
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role VARCHAR(20) DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP,
        is_active BOOLEAN DEFAULT true,
        failed_login_attempts INT DEFAULT 0,
        account_locked_until TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
      CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
    `);

    // Posts table - Big Data 3V: Volume (large amount of posts)
    await client.query(`
      CREATE TABLE IF NOT EXISTS posts (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        title VARCHAR(255) NOT NULL,
        content TEXT NOT NULL,
        media_type VARCHAR(20) CHECK (media_type IN ('text', 'image', 'video', 'link')),
        media_url TEXT,
        status VARCHAR(20) DEFAULT 'published',
        view_count BIGINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        published_at TIMESTAMP,
        checksum VARCHAR(64) -- CIA Triad: Integrity (data integrity verification)
      );

      CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
      CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
      CREATE INDEX IF NOT EXISTS idx_posts_content_trgm ON posts USING gin(content gin_trgm_ops);
    `);

    // Comments table - Big Data 3V: Variety (different types of interactions)
    await client.query(`
      CREATE TABLE IF NOT EXISTS comments (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        content TEXT NOT NULL,
        parent_comment_id UUID REFERENCES comments(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_edited BOOLEAN DEFAULT false
      );

      CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
      CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);
      CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
    `);

    // Likes table - Big Data 3V: Volume + Velocity (high frequency operations)
    await client.query(`
      CREATE TABLE IF NOT EXISTS likes (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        post_id UUID REFERENCES posts(id) ON DELETE CASCADE,
        comment_id UUID REFERENCES comments(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT like_target_check CHECK (
          (post_id IS NOT NULL AND comment_id IS NULL) OR
          (post_id IS NULL AND comment_id IS NOT NULL)
        ),
        UNIQUE(user_id, post_id, comment_id)
      );

      CREATE INDEX IF NOT EXISTS idx_likes_user_id ON likes(user_id);
      CREATE INDEX IF NOT EXISTS idx_likes_post_id ON likes(post_id);
      CREATE INDEX IF NOT EXISTS idx_likes_comment_id ON likes(comment_id);
    `);

    // Analytics Events table - Big Data 3V: Velocity (real-time event streaming)
    await client.query(`
      CREATE TABLE IF NOT EXISTS analytics_events (
        id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(50) NOT NULL,
        user_id UUID REFERENCES users(id) ON DELETE SET NULL,
        post_id UUID REFERENCES posts(id) ON DELETE SET NULL,
        session_id VARCHAR(255),
        ip_address INET,
        user_agent TEXT,
        metadata JSONB, -- Big Data 3V: Variety (flexible schema for different event types)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics_events(event_type);
      CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_events(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_analytics_user_id ON analytics_events(user_id);
      CREATE INDEX IF NOT EXISTS idx_analytics_metadata ON analytics_events USING gin(metadata);
    `);

    // Audit log table - CIA Triad: Integrity & Accountability
    await client.query(`
      CREATE TABLE IF NOT EXISTS audit_logs (
        id BIGSERIAL PRIMARY KEY,
        table_name VARCHAR(50) NOT NULL,
        record_id UUID NOT NULL,
        operation VARCHAR(20) NOT NULL,
        user_id UUID REFERENCES users(id),
        old_data JSONB,
        new_data JSONB,
        ip_address INET,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_audit_table_record ON audit_logs(table_name, record_id);
      CREATE INDEX IF NOT EXISTS idx_audit_user_id ON audit_logs(user_id);
      CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_logs(created_at DESC);
    `);

    // Aggregated analytics table - Big Data 3V: Volume (pre-computed aggregations)
    await client.query(`
      CREATE TABLE IF NOT EXISTS daily_analytics (
        id BIGSERIAL PRIMARY KEY,
        date DATE NOT NULL,
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        post_id UUID REFERENCES posts(id) ON DELETE CASCADE,
        views BIGINT DEFAULT 0,
        likes BIGINT DEFAULT 0,
        comments BIGINT DEFAULT 0,
        shares BIGINT DEFAULT 0,
        unique_visitors BIGINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, user_id, post_id)
      );

      CREATE INDEX IF NOT EXISTS idx_daily_analytics_date ON daily_analytics(date DESC);
      CREATE INDEX IF NOT EXISTS idx_daily_analytics_user ON daily_analytics(user_id, date);
      CREATE INDEX IF NOT EXISTS idx_daily_analytics_post ON daily_analytics(post_id, date);
    `);

    // Create trigger for updated_at timestamp
    await client.query(`
      CREATE OR REPLACE FUNCTION update_updated_at_column()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
      END;
      $$ language 'plpgsql';
    `);

    // Apply triggers
    const tables = ['users', 'posts', 'comments'];
    for (const table of tables) {
      await client.query(`
        DROP TRIGGER IF EXISTS update_${table}_updated_at ON ${table};
        CREATE TRIGGER update_${table}_updated_at
          BEFORE UPDATE ON ${table}
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column();
      `);
    }

    console.log('✅ Database setup completed successfully!');
    console.log('📊 Tables created:');
    console.log('   - users (with security features)');
    console.log('   - posts (with integrity checksums)');
    console.log('   - comments (nested support)');
    console.log('   - likes (high-velocity operations)');
    console.log('   - analytics_events (real-time streaming)');
    console.log('   - audit_logs (integrity & accountability)');
    console.log('   - daily_analytics (pre-computed aggregations)');

  } catch (error) {
    console.error('❌ Database setup failed:', error);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
};

setupDatabase().catch(console.error);
