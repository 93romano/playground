# Project Structure

```
redis-postgresql-nodejs/
│
├── src/                          # Source code
│   ├── config/                   # Configuration files
│   │   ├── database.js          # PostgreSQL connection pool
│   │   └── redis.js             # Redis client + utilities
│   │
│   ├── middleware/               # Express middleware
│   │   ├── auth.middleware.js   # Authentication & Authorization (CIA: Confidentiality)
│   │   ├── rateLimit.middleware.js  # Rate limiting (CIA: Availability)
│   │   └── validation.middleware.js # Input validation (CIA: Integrity)
│   │
│   ├── routes/                   # API route handlers
│   │   ├── auth.routes.js       # /api/auth endpoints
│   │   ├── post.routes.js       # /api/posts endpoints (CRUD)
│   │   └── analytics.routes.js  # /api/analytics endpoints (Big Data 3V)
│   │
│   ├── services/                 # Business logic layer
│   │   ├── auth.service.js      # Authentication logic (CIA: Confidentiality)
│   │   ├── post.service.js      # Post CRUD operations
│   │   └── analytics.service.js # Big Data 3V analytics operations
│   │
│   ├── utils/                    # Utility functions
│   │   └── logger.js            # Winston logger (CIA: Integrity)
│   │
│   └── server.js                 # Express app entry point
│
├── scripts/                      # Setup and utility scripts
│   ├── setup-database.js        # Database schema creation
│   └── seed-data.js             # Sample data seeding
│
├── logs/                         # Application logs (gitignored)
│   ├── error.log                # Error logs
│   └── combined.log             # All logs
│
├── .env.example                  # Environment variables template
├── .gitignore                    # Git ignore rules
├── package.json                  # Node.js dependencies
├── README.md                     # Project documentation
├── QUICKSTART.md                 # Quick setup guide
├── ARCHITECTURE.md               # Architecture deep dive
└── PROJECT_STRUCTURE.md          # This file
```

## File Purposes

### Configuration Layer
| File | Purpose | Key Concepts |
|------|---------|--------------|
| `config/database.js` | PostgreSQL connection pooling | CIA: Availability (connection reuse) |
| `config/redis.js` | Redis client with utility functions | Big Data: Velocity (fast caching) |

### Middleware Layer
| File | Purpose | Key Concepts |
|------|---------|--------------|
| `middleware/auth.middleware.js` | JWT verification, RBAC | CIA: Confidentiality (access control) |
| `middleware/rateLimit.middleware.js` | Request rate limiting | CIA: Availability (prevent abuse) |
| `middleware/validation.middleware.js` | Input validation with Joi | CIA: Integrity (data validation) |

### Routes Layer
| File | Purpose | Key Concepts |
|------|---------|--------------|
| `routes/auth.routes.js` | Authentication endpoints | CRUD: Create user, login session |
| `routes/post.routes.js` | Post CRUD endpoints | Full CRUD operations |
| `routes/analytics.routes.js` | Analytics endpoints | Big Data: All 3V characteristics |

### Services Layer
| File | Purpose | Key Concepts |
|------|---------|--------------|
| `services/auth.service.js` | User auth business logic | Password hashing, JWT, audit logs |
| `services/post.service.js` | Post CRUD business logic | Checksum, caching, CRUD operations |
| `services/analytics.service.js` | Analytics processing | Real-time tracking, aggregations |

### Scripts
| File | Purpose | Key Concepts |
|------|---------|--------------|
| `scripts/setup-database.js` | Creates all database tables | Schema design, indexes, triggers |
| `scripts/seed-data.js` | Populates sample data | Testing data, relationships |

## Data Flow Diagrams

### Authentication Flow (CIA: Confidentiality)
```
Client Request
    ↓
POST /api/auth/login
    ↓
auth.routes.js → Validation Middleware
    ↓
auth.service.js
    ├─→ Check password (bcrypt)
    ├─→ Generate JWT
    ├─→ Store session in Redis
    └─→ Create audit log
    ↓
Return JWT token
```

### Post Creation Flow (CRUD + CIA)
```
Client Request (with JWT)
    ↓
POST /api/posts
    ↓
auth.middleware.js → Verify JWT
    ↓
validation.middleware.js → Validate input
    ↓
rateLimit.middleware.js → Check rate limit
    ↓
post.routes.js
    ↓
post.service.js
    ├─→ Generate checksum (CIA: Integrity)
    ├─→ Insert to PostgreSQL
    ├─→ Cache in Redis (Big Data: Velocity)
    ├─→ Create audit log (CIA: Integrity)
    └─→ Track analytics event (Big Data: Velocity)
    ↓
Return created post
```

### Post Retrieval Flow (Big Data: Velocity)
```
Client Request
    ↓
GET /api/posts/:id
    ↓
rateLimit.middleware.js
    ↓
post.routes.js
    ↓
post.service.js
    ├─→ Check Redis cache (Big Data: Velocity)
    │   ├─→ Cache HIT → Return from Redis
    │   └─→ Cache MISS ↓
    ├─→ Query PostgreSQL
    ├─→ Verify checksum (CIA: Integrity)
    ├─→ Store in cache
    └─→ Increment view count (async)
    ↓
Return post data
```

### Analytics Tracking Flow (Big Data: 3V)
```
User Action (view, like, comment)
    ↓
analytics.service.trackEvent()
    ├─→ Store in Redis queue (Big Data: Velocity)
    ├─→ Increment real-time counters (Big Data: Velocity)
    └─→ Async batch insert to PostgreSQL (Big Data: Volume)
    ↓
Background Worker
    └─→ Aggregate daily analytics (Big Data: Volume)
```

## Database Schema Overview

```
┌─────────────┐
│    users    │───────┐
└─────────────┘       │
      │               │
      │ (1:N)         │ (1:N)
      ↓               ↓
┌─────────────┐   ┌──────────────┐
│    posts    │←──│   comments   │
└─────────────┘   └──────────────┘
      │                  │
      │ (1:N)            │ (1:N)
      ↓                  ↓
┌─────────────┐   ┌──────────────┐
│    likes    │   │audit_logs    │
└─────────────┘   └──────────────┘

┌──────────────────┐
│analytics_events  │ (Time-series)
└──────────────────┘

┌──────────────────┐
│daily_analytics   │ (Aggregated)
└──────────────────┘
```

## Key Features by File

### CRUD Operations
- **Create**: `post.service.js:createPost()`, `auth.service.js:register()`
- **Read**: `post.service.js:getPostById()`, `post.service.js:getPosts()`
- **Update**: `post.service.js:updatePost()`, `auth.service.js:changePassword()`
- **Delete**: `post.service.js:deletePost()`, `auth.service.js:logout()`

### CIA Triad: Confidentiality
- `auth.service.js` - Password hashing, JWT
- `auth.middleware.js` - Token verification, RBAC
- `server.js` - Helmet security headers

### CIA Triad: Integrity
- `post.service.js` - Checksum verification
- `validation.middleware.js` - Input validation
- `auth.service.js` - Audit logging
- `setup-database.js` - Database constraints

### CIA Triad: Availability
- `redis.js` - Caching layer
- `database.js` - Connection pooling
- `rateLimit.middleware.js` - Rate limiting
- `server.js` - Graceful shutdown

### Big Data: Volume
- `post.service.js:getPosts()` - Pagination
- `setup-database.js` - Database indexes
- `analytics.service.js:aggregateDailyAnalytics()` - Pre-aggregation

### Big Data: Velocity
- `redis.js` - Fast caching utilities
- `analytics.service.js:trackEvent()` - Real-time tracking
- `post.service.js:incrementViewCount()` - High-frequency counters

### Big Data: Variety
- `analytics.service.js` - Multiple data types
- `setup-database.js` - JSONB metadata fields
- `post.service.js:searchPosts()` - Full-text search

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Runtime | Node.js | JavaScript backend |
| Framework | Express.js | Web server |
| Database | PostgreSQL | Persistent storage |
| Cache | Redis | In-memory caching |
| Auth | JWT + bcrypt | Authentication |
| Validation | Joi | Input validation |
| Logging | Winston | Application logging |
| Security | Helmet | Security headers |

## Dependencies Map

```
server.js
├── express (web framework)
├── helmet (security)
├── cors (cross-origin)
├── compression (performance)
├── dotenv (config)
│
├── Routes
│   ├── auth.routes.js
│   ├── post.routes.js
│   └── analytics.routes.js
│
├── Middleware
│   ├── auth.middleware.js
│   ├── rateLimit.middleware.js
│   └── validation.middleware.js
│
├── Services
│   ├── auth.service.js (bcrypt, jsonwebtoken)
│   ├── post.service.js (crypto)
│   └── analytics.service.js
│
├── Config
│   ├── database.js (pg)
│   └── redis.js (redis)
│
└── Utils
    └── logger.js (winston)
```

## API Endpoint Overview

```
/api
├── /auth
│   ├── POST /register        (Create user)
│   ├── POST /login          (Create session)
│   ├── POST /logout         (Delete session)
│   ├── POST /change-password (Update password)
│   └── GET  /me             (Read user)
│
├── /posts
│   ├── POST   /             (Create post)
│   ├── GET    /             (Read posts with pagination)
│   ├── GET    /:id          (Read single post)
│   ├── GET    /trending     (Read trending)
│   ├── GET    /search       (Search posts)
│   ├── PUT    /:id          (Update post)
│   └── DELETE /:id          (Delete post)
│
└── /analytics
    ├── GET  /realtime       (Real-time metrics)
    ├── GET  /user/:userId   (User analytics)
    ├── GET  /platform       (Platform analytics)
    ├── GET  /heatmap/:userId (Activity heatmap)
    └── POST /track          (Track custom event)
```

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
JWT_SECRET=your_secret_key
```

---

This structure demonstrates professional-grade Node.js architecture with clear separation of concerns, security best practices, and scalability considerations.
