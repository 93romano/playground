# Social Media Analytics Platform

A comprehensive Node.js study project demonstrating **Redis**, **PostgreSQL**, and **Node.js** with proper architecture, CRUD operations, CIA Triad security principles, and Big Data 3V characteristics.

## Features

### CRUD Operations
- **Create**: Register users, create posts, add comments
- **Read**: Get posts with pagination, search, trending algorithms
- **Update**: Edit posts, change passwords
- **Delete**: Remove posts, logout sessions

### CIA Triad (Security Principles)

#### Confidentiality
- Password hashing with bcrypt (12 rounds)
- JWT-based authentication
- Secure session management
- Role-based access control (RBAC)

#### Integrity
- Content checksum verification (SHA-256)
- Audit logging for all critical operations
- Input validation with Joi
- Database constraints and foreign keys

#### Availability
- Redis caching for high performance
- Connection pooling (PostgreSQL)
- Rate limiting to prevent abuse
- Graceful error handling
- Health check endpoints

### Big Data 3V

#### Volume
- Handles large amounts of posts and analytics data
- Pagination for efficient data retrieval
- Pre-computed aggregations (daily analytics)
- Optimized database indexes

#### Velocity
- Real-time event tracking
- Redis for high-speed caching
- Asynchronous view counting
- Streaming analytics data
- Rate limiting counters

#### Variety
- Multiple data types (posts, comments, likes, analytics)
- Flexible JSONB metadata storage
- Different media types (text, image, video, link)
- Full-text search capabilities
- Time-series analytics data

## Architecture

```
├── src/
│   ├── config/
│   │   ├── database.js       # PostgreSQL connection pool
│   │   └── redis.js          # Redis client with utilities
│   ├── middleware/
│   │   ├── auth.middleware.js         # Authentication & Authorization
│   │   ├── rateLimit.middleware.js   # Rate limiting (CIA: Availability)
│   │   └── validation.middleware.js  # Input validation (CIA: Integrity)
│   ├── routes/
│   │   ├── auth.routes.js    # Authentication endpoints
│   │   ├── post.routes.js    # Post CRUD endpoints
│   │   └── analytics.routes.js # Analytics endpoints
│   ├── services/
│   │   ├── auth.service.js        # Authentication logic
│   │   ├── post.service.js        # Post CRUD operations
│   │   └── analytics.service.js   # Big Data 3V analytics
│   ├── utils/
│   │   └── logger.js         # Winston logger
│   └── server.js             # Express app setup
├── scripts/
│   ├── setup-database.js     # Database schema creation
│   └── seed-data.js          # Sample data seeding
└── package.json
```

## Database Schema

### Users Table
- Authentication and authorization
- Failed login tracking
- Account locking mechanism

### Posts Table
- Content with integrity checksums
- Multiple media types
- View counting

### Comments Table
- Nested comment support
- Edit tracking

### Likes Table
- High-velocity operations
- Unique constraints

### Analytics Events Table
- Real-time event streaming
- Flexible JSONB metadata
- IP and user agent tracking

### Audit Logs Table
- Complete audit trail
- Old/new data comparison
- User attribution

### Daily Analytics Table
- Pre-computed aggregations
- Performance optimization

## Setup Instructions

### Prerequisites
- Node.js (v16+)
- PostgreSQL (v12+)
- Redis (v6+)

### Installation

1. **Install dependencies**
```bash
npm install
```

2. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your database credentials
```

3. **Setup database**
```bash
npm run setup:db
```

4. **Seed sample data**
```bash
npm run seed:data
```

5. **Start the server**
```bash
npm start
# or for development with auto-reload
npm run dev
```

## API Endpoints

### Authentication (CIA: Confidentiality)
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login and get JWT token
- `POST /api/auth/logout` - Logout (invalidate session)
- `POST /api/auth/change-password` - Change password
- `GET /api/auth/me` - Get current user

### Posts (CRUD + Big Data 3V)
- `POST /api/posts` - Create post (C)
- `GET /api/posts` - Get posts with pagination (R)
- `GET /api/posts/:id` - Get single post (R)
- `GET /api/posts/trending` - Get trending posts (Big Data: Velocity)
- `GET /api/posts/search?q=query` - Search posts (Big Data: Variety)
- `PUT /api/posts/:id` - Update post (U)
- `DELETE /api/posts/:id` - Delete post (D)

### Analytics (Big Data 3V)
- `GET /api/analytics/realtime` - Real-time analytics (Velocity)
- `GET /api/analytics/user/:userId` - User analytics (Variety)
- `GET /api/analytics/platform` - Platform-wide analytics (Volume)
- `GET /api/analytics/heatmap/:userId` - Activity heatmap (Variety)
- `POST /api/analytics/track` - Track custom event (Velocity)

### Health Check (CIA: Availability)
- `GET /health` - Service health status

## Testing

### Manual Testing

1. **Register a user**
```bash
curl -X POST http://localhost:3000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","email":"test@example.com","password":"password123"}'
```

2. **Login**
```bash
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123"}'
```

3. **Create a post** (use token from login)
```bash
curl -X POST http://localhost:3000/api/posts \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"title":"My First Post","content":"This is my first post!","mediaType":"text"}'
```

4. **Get posts**
```bash
curl http://localhost:3000/api/posts?page=1&limit=10
```

### Load Testing

Check Redis cache performance:
```bash
# Monitor Redis
redis-cli monitor

# Make multiple requests to see caching in action
for i in {1..10}; do
  curl http://localhost:3000/api/posts/SOME_POST_ID
done
```

## Security Features (CIA Triad)

### Confidentiality
- ✅ Bcrypt password hashing (12 rounds)
- ✅ JWT authentication with expiration
- ✅ Session management in Redis
- ✅ Role-based access control
- ✅ Secure headers (Helmet.js)

### Integrity
- ✅ SHA-256 content checksums
- ✅ Audit logs for all operations
- ✅ Input validation (Joi)
- ✅ Database constraints
- ✅ Failed login tracking

### Availability
- ✅ Redis caching layer
- ✅ PostgreSQL connection pooling
- ✅ Rate limiting (100 req/15min)
- ✅ Graceful shutdown
- ✅ Health check endpoints
- ✅ Error recovery mechanisms

## Big Data 3V Implementation

### Volume
- Pagination for large datasets
- Database indexes for performance
- Pre-computed daily analytics
- Efficient aggregation queries

### Velocity
- Redis for real-time operations
- Asynchronous event tracking
- High-frequency view counting
- Streaming analytics pipeline
- Cache-first architecture

### Variety
- Multiple content types (text, image, video, link)
- Flexible JSONB metadata
- Full-text search
- Time-series analytics
- Activity heatmaps

## Monitoring

### Logs
Logs are stored in:
- `logs/error.log` - Error logs only
- `logs/combined.log` - All logs

### Redis Monitoring
```bash
redis-cli
> INFO stats
> MONITOR
```

### PostgreSQL Monitoring
```bash
psql -U postgres -d social_analytics
SELECT * FROM pg_stat_activity;
```

## Performance Optimization

1. **Caching Strategy**
   - Posts cached for 1 hour
   - User sessions cached for 24 hours
   - Analytics cached for 5 minutes

2. **Database Indexes**
   - B-tree indexes on foreign keys
   - GIN indexes for full-text search
   - Covering indexes for common queries

3. **Connection Pooling**
   - PostgreSQL: 20 max connections
   - Redis: Persistent connection

## Learning Objectives

This project demonstrates:
- ✅ **Good Architecture**: Separation of concerns, modular design
- ✅ **CRUD Operations**: Complete create, read, update, delete
- ✅ **CIA Triad**: Security best practices
- ✅ **Big Data 3V**: Volume, Velocity, Variety handling
- ✅ **Redis Caching**: Performance optimization
- ✅ **PostgreSQL**: Relational data modeling
- ✅ **Node.js**: Modern JavaScript backend

## License

MIT License - Free to use for learning purposes

## Test Credentials

After running seed script:
- **User**: alice@example.com | Password: password123
- **User**: bob@example.com | Password: password123
- **Admin**: admin@example.com | Password: admin123

---

**Happy Learning!** 🚀
