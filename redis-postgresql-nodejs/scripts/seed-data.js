import pg from 'pg';
import bcrypt from 'bcrypt';
import dotenv from 'dotenv';
import crypto from 'crypto';

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
});

const seedData = async () => {
  const client = await pool.connect();

  try {
    console.log('🌱 Starting data seeding...');

    await client.query('BEGIN');

    // Create users
    const users = [];
    const usernames = ['alice', 'bob', 'charlie', 'diana', 'evan'];

    console.log('Creating users...');
    for (const username of usernames) {
      const passwordHash = await bcrypt.hash('password123', 12);
      const result = await client.query(
        `INSERT INTO users (username, email, password_hash, role)
         VALUES ($1, $2, $3, $4)
         RETURNING id, username, email`,
        [username, `${username}@example.com`, passwordHash, 'user']
      );
      users.push(result.rows[0]);
    }

    // Create admin user
    const adminPasswordHash = await bcrypt.hash('admin123', 12);
    const adminResult = await client.query(
      `INSERT INTO users (username, email, password_hash, role)
       VALUES ($1, $2, $3, $4)
       RETURNING id, username, email`,
      ['admin', 'admin@example.com', adminPasswordHash, 'admin']
    );
    users.push(adminResult.rows[0]);

    console.log(`✅ Created ${users.length} users`);

    // Create posts - Big Data 3V: Volume
    const posts = [];
    const postTitles = [
      'Getting Started with Node.js',
      'Understanding Redis Caching',
      'PostgreSQL Performance Tips',
      'Building RESTful APIs',
      'Introduction to Big Data',
      'CIA Triad in Security',
      'Microservices Architecture',
      'Docker Best Practices',
      'Kubernetes for Beginners',
      'Cloud Computing Fundamentals',
    ];

    const postContents = [
      'Node.js is a powerful runtime for building scalable applications...',
      'Redis is an in-memory data structure store used as a database...',
      'PostgreSQL offers many features for optimizing query performance...',
      'REST APIs follow principles that make web services scalable...',
      'Big Data is characterized by Volume, Velocity, and Variety...',
      'The CIA Triad stands for Confidentiality, Integrity, and Availability...',
      'Microservices break down applications into smaller, independent services...',
      'Docker containers provide consistent environments across development...',
      'Kubernetes orchestrates containerized applications at scale...',
      'Cloud computing delivers computing services over the internet...',
    ];

    const mediaTypes = ['text', 'image', 'video', 'link'];

    console.log('Creating posts...');
    for (let i = 0; i < 50; i++) {
      const user = users[Math.floor(Math.random() * users.length)];
      const titleIndex = i % postTitles.length;
      const content = postContents[titleIndex] + ' ' + `Post number ${i + 1}. `.repeat(10);
      const mediaType = mediaTypes[Math.floor(Math.random() * mediaTypes.length)];

      // Generate checksum for integrity
      const checksum = crypto.createHash('sha256').update(content).digest('hex');

      const result = await client.query(
        `INSERT INTO posts (user_id, title, content, media_type, checksum, view_count, published_at)
         VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP - INTERVAL '${Math.floor(Math.random() * 30)} days')
         RETURNING id`,
        [
          user.id,
          postTitles[titleIndex] + ` #${i + 1}`,
          content,
          mediaType,
          checksum,
          Math.floor(Math.random() * 1000),
        ]
      );
      posts.push(result.rows[0].id);
    }

    console.log(`✅ Created ${posts.length} posts`);

    // Create comments - Big Data 3V: Variety
    console.log('Creating comments...');
    const commentTexts = [
      'Great post! Very informative.',
      'Thanks for sharing this.',
      'I learned a lot from this.',
      'Could you elaborate more on this topic?',
      'Interesting perspective!',
      'I have a different opinion on this...',
      'This is exactly what I was looking for.',
      'Well written article!',
    ];

    let commentCount = 0;
    for (const postId of posts) {
      const numComments = Math.floor(Math.random() * 5);
      for (let i = 0; i < numComments; i++) {
        const user = users[Math.floor(Math.random() * users.length)];
        const commentText = commentTexts[Math.floor(Math.random() * commentTexts.length)];

        await client.query(
          `INSERT INTO comments (post_id, user_id, content)
           VALUES ($1, $2, $3)`,
          [postId, user.id, commentText]
        );
        commentCount++;
      }
    }

    console.log(`✅ Created ${commentCount} comments`);

    // Create likes - Big Data 3V: Velocity
    console.log('Creating likes...');
    let likeCount = 0;
    for (const postId of posts) {
      const numLikes = Math.floor(Math.random() * users.length);
      const likedUsers = users.slice(0, numLikes);

      for (const user of likedUsers) {
        try {
          await client.query(
            `INSERT INTO likes (user_id, post_id)
             VALUES ($1, $2)`,
            [user.id, postId]
          );
          likeCount++;
        } catch (error) {
          // Skip duplicate likes
        }
      }
    }

    console.log(`✅ Created ${likeCount} likes`);

    // Create analytics events - Big Data 3V: Velocity
    console.log('Creating analytics events...');
    const eventTypes = ['page_view', 'post_created', 'like', 'comment', 'share'];

    for (let i = 0; i < 500; i++) {
      const eventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
      const user = users[Math.floor(Math.random() * users.length)];
      const post = posts[Math.floor(Math.random() * posts.length)];

      await client.query(
        `INSERT INTO analytics_events (event_type, user_id, post_id, session_id, ip_address, metadata, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP - INTERVAL '${Math.floor(Math.random() * 30)} days')`,
        [
          eventType,
          user.id,
          post,
          `session_${Math.floor(Math.random() * 100)}`,
          `192.168.1.${Math.floor(Math.random() * 255)}`,
          JSON.stringify({ source: 'web', device: 'desktop' }),
        ]
      );
    }

    console.log('✅ Created 500 analytics events');

    await client.query('COMMIT');

    console.log('');
    console.log('🎉 Database seeding completed successfully!');
    console.log('');
    console.log('📝 Test credentials:');
    console.log('   Email: alice@example.com | Password: password123');
    console.log('   Email: bob@example.com   | Password: password123');
    console.log('   Email: admin@example.com | Password: admin123 (Admin)');

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ Seeding failed:', error);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
};

seedData().catch(console.error);
