# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Node.js social media analytics platform demonstrating Redis caching, PostgreSQL database, CRUD operations, CIA Triad security principles, and Big Data 3V characteristics. This is a learning/study project showcasing enterprise-level architecture patterns.

## Setup Commands

### Initial Setup
```bash
# Install dependencies
npm install

# Setup database schema (creates all tables, indexes, triggers)
npm run setup:db

# Seed sample data (optional - creates 6 users, 50 posts, comments, likes, analytics)
npm run seed:data
```

### Development
```bash
# Start server (production mode)
npm start

# Start with auto-reload (development)
npm run dev

# Load testing
npm run test:load
```

### Prerequisites
- PostgreSQL 12+ running on port 5432
- Redis 6+ running on port 6379
- Node.js 16+
- Environment variables configured in `.env` (copy from `.env.example`)

## Architecture

### Layered Architecture
```
HTTP Layer (Express) → Business Logic Layer (Services) → Data Access Layer (PostgreSQL + Redis)
```

**Key Principle**: Separation of concerns - routes handle HTTP, middleware processes requests, services contain business logic, config manages connections.

### Data Flow Pattern
1. Request → Middleware (auth/validation/rate limiting)
2. Route handler → Service layer
3. Service checks Redis cache first
4. On cache miss: Query PostgreSQL → Store in Redis → Return result
5. On cache hit: Return from Redis directly

### Caching Strategy
- **Cache-aside pattern**: Check cache first, query DB on miss, populate cache
- **TTL Strategy**:
  - Posts: 1 hour (3600s)
  - Sessions: 24 hours (86400s)
  - Analytics: 5 minutes (300s)
  - Trending: 5 minutes (300s)
- **Cache invalidation**: On UPDATE/DELETE, invalidate specific caches and related patterns
- **Cache keys pattern**: `post:{id}`, `user_posts:{userId}`, `session:{userId}`, `rate_limit:{identifier}`

### Database Design
- **Normalization**: 3NF with proper foreign key relationships
- **Tables**: users, posts, comments, likes, analytics_events, audit_logs, daily_analytics
- **Indexes**: B-tree on foreign keys, GIN for full-text search, composite indexes for common queries
- **Constraints**: Foreign keys, unique constraints, check constraints, NOT NULL where appropriate

## Core Concepts

### CIA Triad Implementation
- **Confidentiality**: Bcrypt password hashing (12 rounds), JWT authentication, session management in Redis, RBAC in [auth.middleware.js](src/middleware/auth.middleware.js)
- **Integrity**: SHA-256 content checksums in [post.service.js](src/services/post.service.js), audit logging, Joi validation in [validation.middleware.js](src/middleware/validation.middleware.js)
- **Availability**: Redis caching layer, PostgreSQL connection pooling (max 20), rate limiting (100 req/15min) in [rateLimit.middleware.js](src/middleware/rateLimit.middleware.js)

### Big Data 3V
- **Volume**: Pagination for large datasets, database indexes, pre-computed daily analytics
- **Velocity**: Redis for sub-millisecond reads, asynchronous event tracking, real-time counters
- **Variety**: Multiple content types (text/image/video/link), JSONB metadata, full-text search

## Important Files

### Service Layer (Business Logic)
- [src/services/auth.service.js](src/services/auth.service.js) - Authentication logic (password hashing, JWT, audit logs)
- [src/services/post.service.js](src/services/post.service.js) - CRUD operations with checksum verification and caching
- [src/services/analytics.service.js](src/services/analytics.service.js) - Real-time event tracking and aggregations

### Middleware (Request Processing)
- [src/middleware/auth.middleware.js](src/middleware/auth.middleware.js) - JWT verification, RBAC, ownership checks
- [src/middleware/validation.middleware.js](src/middleware/validation.middleware.js) - Joi schema validation
- [src/middleware/rateLimit.middleware.js](src/middleware/rateLimit.middleware.js) - Redis-based rate limiting (3 tiers: strict/normal/lenient)

### Configuration
- [src/config/database.js](src/config/database.js) - PostgreSQL connection pool
- [src/config/redis.js](src/config/redis.js) - Redis client with utility functions (cache, sorted sets, lists, hashes, rate limiting helpers)

### Scripts
- [scripts/setup-database.js](scripts/setup-database.js) - Creates complete database schema with indexes and triggers
- [scripts/seed-data.js](scripts/seed-data.js) - Populates test data

## Key Patterns and Conventions

### Authentication Flow
1. User registers → Password hashed with bcrypt (12 rounds) → Stored in PostgreSQL
2. User logs in → Password verified → JWT generated → Session stored in Redis (24h TTL)
3. Protected routes → JWT verified by [auth.middleware.js](src/middleware/auth.middleware.js) → User attached to `req.user`
4. Logout → Session removed from Redis

### Post CRUD with Security
1. **Create**: Checksum generated (SHA-256), stored in PostgreSQL, cached in Redis, audit logged
2. **Read**: Check Redis cache → On miss, query PostgreSQL, verify checksum, cache result
3. **Update**: Verify ownership, generate new checksum, update DB, invalidate cache, audit log
4. **Delete**: Verify ownership, delete from DB, invalidate all related caches, audit log

### Rate Limiting
Three tiers implemented in [rateLimit.middleware.js](src/middleware/rateLimit.middleware.js):
- **Strict**: 10 req/15min (auth endpoints like login)
- **Normal**: 100 req/15min (general API)
- **Lenient**: 1000 req/15min (read-heavy analytics)

### Error Handling
- All service methods wrapped in try-catch
- Errors logged with Winston to `logs/error.log` and `logs/combined.log`
- Graceful fallbacks (e.g., cache failures don't break requests)
- Production mode hides detailed error messages

## Testing

### Test Credentials (after running `npm run seed:data`)
- **Regular User**: alice@example.com / password123
- **Regular User**: bob@example.com / password123
- **Admin**: admin@example.com / admin123

### Manual Testing Flow
```bash
# 1. Register
curl -X POST http://localhost:3000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test","email":"test@test.com","password":"password123"}'

# 2. Login (save the token)
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@test.com","password":"password123"}'

# 3. Create post (use token from step 2)
curl -X POST http://localhost:3000/api/posts \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"title":"Test","content":"Test post","mediaType":"text"}'

# 4. Get posts
curl http://localhost:3000/api/posts
```

### Monitoring Cache Performance
```bash
# Terminal 1: Monitor Redis operations
redis-cli MONITOR

# Terminal 2: Make requests and observe cache hits/misses
curl http://localhost:3000/api/posts/SOME_ID
```

## API Endpoints

### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login, get JWT
- `POST /api/auth/logout` - Invalidate session
- `POST /api/auth/change-password` - Change password (requires auth)
- `GET /api/auth/me` - Get current user (requires auth)

### Posts (CRUD)
- `POST /api/posts` - Create (requires auth)
- `GET /api/posts` - List with pagination (?page=1&limit=10)
- `GET /api/posts/:id` - Get single post
- `GET /api/posts/trending` - Trending posts
- `GET /api/posts/search?q=query` - Full-text search
- `PUT /api/posts/:id` - Update (requires auth + ownership)
- `DELETE /api/posts/:id` - Delete (requires auth + ownership)

### Analytics
- `GET /api/analytics/realtime` - Real-time metrics
- `GET /api/analytics/user/:userId` - User analytics
- `GET /api/analytics/platform` - Platform-wide stats
- `GET /api/analytics/heatmap/:userId` - Activity heatmap
- `POST /api/analytics/track` - Track custom event

### Health
- `GET /health` - Service health check (PostgreSQL + Redis)

## Common Development Tasks

### Adding New Endpoints
1. Create route handler in `src/routes/*.routes.js`
2. Add business logic to corresponding service in `src/services/*.service.js`
3. Add validation schema if needed in middleware
4. Consider caching strategy (cache key, TTL, invalidation)
5. Add audit logging for sensitive operations

### Modifying Database Schema
1. Update schema in [scripts/setup-database.js](scripts/setup-database.js)
2. Consider migration strategy (this project doesn't use formal migrations)
3. Update indexes if query patterns change
4. Run `npm run setup:db` to recreate (drops existing tables!)

### Cache Management
Use the `cache` utilities from [redis.js](src/config/redis.js):
- `cache.set(key, value, ttl)` - Set with TTL
- `cache.get(key)` - Get value
- `cache.del(key)` - Delete single key
- `cache.delPattern(pattern)` - Delete by pattern (e.g., `posts:*`)
- `cache.incr(key)` - Increment counter (for analytics)

## Environment Variables

Required in `.env`:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=social_analytics
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

REDIS_HOST=localhost
REDIS_PORT=6379

PORT=3000
JWT_SECRET=your_super_secret_jwt_key
JWT_EXPIRES_IN=24h
```

## Troubleshooting

### PostgreSQL Connection Issues
- Ensure PostgreSQL is running: `brew services list`
- Check database exists: `psql -U postgres -l`
- Verify credentials in `.env` match your PostgreSQL setup

### Redis Connection Issues
- Ensure Redis is running: `brew services list`
- Test connection: `redis-cli ping` (should return PONG)

### Port Already in Use
- Change `PORT` in `.env` to a different port (e.g., 3001)

## Security Notes

- **Never commit `.env`** - contains secrets (already in `.gitignore`)
- **Failed login attempts**: Account locks after 5 failures for 15 minutes
- **Rate limiting**: Automatically enforced per IP/user
- **Password requirements**: Enforced by Joi validation
- **Audit logs**: All sensitive operations logged to `audit_logs` table
- **Checksum verification**: Content integrity verified on retrieval
