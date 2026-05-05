# Quick Start Guide

Get up and running in 5 minutes!

## Prerequisites Check

Make sure you have these installed:
```bash
node --version   # Should be v16+
psql --version   # PostgreSQL 12+
redis-cli --version  # Redis 6+
```

## Step-by-Step Setup

### 1. Install Dependencies
```bash
cd /Users/kimdonghyuk/Documents/workspace/db-study/redis-postgresql-nodejs
npm install
```

### 2. Start Required Services

**PostgreSQL** (if not running):
```bash
brew services start postgresql@15
```

**Redis** (if not running):
```bash
brew services start redis
```

### 3. Configure Environment
```bash
cp .env.example .env
```

Edit `.env` and update these values:
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=social_analytics

JWT_SECRET=your_super_secret_key_here_change_this
```

### 4. Create Database
```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE social_analytics;

# Exit
\q
```

### 5. Setup Database Schema
```bash
npm run setup:db
```

You should see:
```
✅ Database setup completed successfully!
📊 Tables created: users, posts, comments, likes, analytics_events, audit_logs, daily_analytics
```

### 6. Seed Sample Data (Optional)
```bash
npm run seed:data
```

This creates:
- 6 users (5 regular + 1 admin)
- 50 posts
- Comments and likes
- 500 analytics events

### 7. Start the Server
```bash
npm start
```

You should see:
```
✅ PostgreSQL connected
✅ Redis connected
🚀 Server running on port 3000
📊 Environment: development
🔒 Security: Helmet, CORS, Rate Limiting enabled
💾 Database: PostgreSQL + Redis
📈 Features: CRUD, CIA Triad, Big Data 3V
```

### 8. Test the API

**Check health**:
```bash
curl http://localhost:3000/health
```

**Register a user**:
```bash
curl -X POST http://localhost:3000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "password123"
  }'
```

**Login**:
```bash
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "password123"
  }'
```

Save the token from the response!

**Create a post** (replace YOUR_TOKEN):
```bash
curl -X POST http://localhost:3000/api/posts \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "title": "My First Post",
    "content": "This is my first post on the platform!",
    "mediaType": "text"
  }'
```

**Get posts**:
```bash
curl http://localhost:3000/api/posts
```

**Get trending posts**:
```bash
curl http://localhost:3000/api/posts/trending
```

## Test Credentials (After Seeding)

```
Regular Users:
- Email: alice@example.com | Password: password123
- Email: bob@example.com   | Password: password123

Admin:
- Email: admin@example.com | Password: admin123
```

## Explore the Features

### CRUD Operations
- ✅ Create posts, users, comments
- ✅ Read with pagination, search, filtering
- ✅ Update posts, passwords
- ✅ Delete posts, sessions

### CIA Triad Security
- ✅ **Confidentiality**: Hashed passwords, JWT auth
- ✅ **Integrity**: Checksums, audit logs, validation
- ✅ **Availability**: Caching, rate limiting, pooling

### Big Data 3V
- ✅ **Volume**: Pagination, indexes, aggregations
- ✅ **Velocity**: Real-time tracking, Redis caching
- ✅ **Variety**: Multiple data types, JSONB, search

## Monitor Redis Cache

Open a new terminal and watch Redis operations:
```bash
redis-cli MONITOR
```

Then make requests to see cache hits/misses in real-time!

## Check Database

```bash
psql -U postgres -d social_analytics

# See all tables
\dt

# Check users
SELECT username, email, role FROM users;

# Check posts
SELECT title, username FROM posts JOIN users ON posts.user_id = users.id LIMIT 5;

# Exit
\q
```

## Troubleshooting

### Port 3000 already in use
```bash
# Change PORT in .env
PORT=3001
```

### PostgreSQL connection failed
```bash
# Check if PostgreSQL is running
brew services list

# Start it
brew services start postgresql@15
```

### Redis connection failed
```bash
# Check if Redis is running
brew services list

# Start it
brew services start redis
```

### Database doesn't exist
```bash
psql -U postgres
CREATE DATABASE social_analytics;
\q
npm run setup:db
```

## Next Steps

1. Read [README.md](README.md) for full documentation
2. Read [ARCHITECTURE.md](ARCHITECTURE.md) for deep dive
3. Explore the API endpoints
4. Try the load testing
5. Modify and extend the project

## Development Mode

For auto-reload during development:
```bash
npm run dev
```

## Stop Services

```bash
# Stop the Node.js app: Ctrl+C

# Stop PostgreSQL
brew services stop postgresql@15

# Stop Redis
brew services stop redis
```

---

**You're all set! Happy coding!** 🚀
