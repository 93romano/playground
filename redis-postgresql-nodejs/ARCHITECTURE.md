# Architecture Documentation

## System Overview

The Social Media Analytics Platform is a full-stack Node.js application demonstrating enterprise-level architecture patterns with Redis caching, PostgreSQL database, and comprehensive security measures.

## Design Principles

### 1. Separation of Concerns
Each layer has a specific responsibility:
- **Routes**: Handle HTTP requests/responses
- **Middleware**: Process requests (auth, validation, rate limiting)
- **Services**: Business logic and data operations
- **Config**: Database and external service connections
- **Utils**: Shared utilities (logging, helpers)

### 2. Layered Architecture

```
┌─────────────────────────────────────┐
│         HTTP Layer (Express)        │
│  - Routes                           │
│  - Middleware                       │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│       Business Logic Layer          │
│  - Auth Service                     │
│  - Post Service                     │
│  - Analytics Service                │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│        Data Access Layer            │
│  - PostgreSQL (Persistent)          │
│  - Redis (Cache + Real-time)        │
└─────────────────────────────────────┘
```

## CIA Triad Implementation

### Confidentiality

**Goal**: Protect sensitive information from unauthorized access

**Implementation**:
1. **Password Security**
   - Bcrypt hashing with 12 rounds
   - No plain text storage
   - Secure password change flow

2. **Authentication**
   - JWT tokens with expiration
   - Token verification on protected routes
   - Session invalidation on logout

3. **Authorization**
   - Role-based access control (user/admin)
   - Resource ownership validation
   - Middleware-based permission checks

4. **Data Protection**
   - Helmet.js for security headers
   - CORS configuration
   - Sensitive data filtering in responses

**Code Reference**: [auth.service.js](src/services/auth.service.js), [auth.middleware.js](src/middleware/auth.middleware.js)

### Integrity

**Goal**: Ensure data accuracy and prevent unauthorized modifications

**Implementation**:
1. **Data Validation**
   - Joi schema validation
   - Input sanitization
   - Type checking

2. **Checksum Verification**
   - SHA-256 hashing for post content
   - Integrity checks on retrieval
   - Tamper detection

3. **Audit Logging**
   - Complete operation history
   - User attribution
   - Old/new data comparison
   - Timestamp tracking

4. **Database Constraints**
   - Foreign key relationships
   - Unique constraints
   - Check constraints
   - NOT NULL requirements

5. **Failed Login Tracking**
   - Attempt counting
   - Account locking after 5 failures
   - 15-minute lockout period

**Code Reference**: [post.service.js](src/services/post.service.js), [validation.middleware.js](src/middleware/validation.middleware.js)

### Availability

**Goal**: Ensure system remains accessible and responsive

**Implementation**:
1. **Caching Strategy**
   - Redis for hot data
   - TTL-based expiration
   - Cache invalidation patterns
   - Cache-aside pattern

2. **Connection Pooling**
   - PostgreSQL: 20 max connections
   - Connection reuse
   - Timeout handling

3. **Rate Limiting**
   - Per-user/IP limits
   - Different tiers (strict/normal/lenient)
   - Redis-based tracking
   - Graceful degradation

4. **Error Handling**
   - Try-catch blocks
   - Graceful failures
   - Fallback mechanisms
   - Detailed logging

5. **Health Monitoring**
   - Database connectivity checks
   - Redis ping tests
   - Service status endpoints

6. **Graceful Shutdown**
   - Connection cleanup
   - Request completion
   - Signal handling

**Code Reference**: [redis.js](src/config/redis.js), [rateLimit.middleware.js](src/middleware/rateLimit.middleware.js)

## Big Data 3V Implementation

### Volume

**Definition**: Handling large amounts of data efficiently

**Implementation**:
1. **Pagination**
   - Offset-based pagination
   - Configurable page sizes
   - Total count tracking

2. **Database Optimization**
   - B-tree indexes on foreign keys
   - GIN indexes for full-text search
   - Partial indexes where appropriate
   - Query optimization

3. **Aggregation**
   - Pre-computed daily analytics
   - Materialized views pattern
   - Batch processing

4. **Scalability Patterns**
   - Connection pooling
   - Query result limiting
   - Efficient JOIN operations

**Metrics**:
- Supports millions of posts
- Efficient querying with indexes
- O(log n) lookup times

**Code Reference**: [post.service.js](src/services/post.service.js:69-119)

### Velocity

**Definition**: Processing data at high speed in real-time

**Implementation**:
1. **Redis Caching**
   - Sub-millisecond reads
   - High-frequency writes
   - In-memory operations

2. **Asynchronous Processing**
   - Non-blocking I/O
   - Promise-based operations
   - Background tasks

3. **Real-time Counting**
   - View count increments
   - Like/comment tracking
   - Event streaming

4. **Event Tracking**
   - Queue-based ingestion
   - Batch inserts
   - Fire-and-forget pattern

5. **Cache Strategies**
   - Write-through caching
   - Cache invalidation
   - TTL management

**Metrics**:
- 1000+ requests/second capability
- <10ms cache response times
- Real-time analytics updates

**Code Reference**: [analytics.service.js](src/services/analytics.service.js), [redis.js](src/config/redis.js)

### Variety

**Definition**: Handling different types and structures of data

**Implementation**:
1. **Multiple Content Types**
   - Text posts
   - Image posts
   - Video posts
   - Link posts

2. **Flexible Schemas**
   - JSONB metadata columns
   - Dynamic event properties
   - Schema-less analytics

3. **Different Operations**
   - CRUD operations
   - Search (full-text)
   - Aggregations
   - Time-series analysis

4. **Data Formats**
   - Structured (PostgreSQL)
   - Semi-structured (JSONB)
   - Unstructured (text content)
   - Time-series (analytics)

5. **Search Capabilities**
   - Full-text search
   - Fuzzy matching (pg_trgm)
   - Relevance ranking

**Code Reference**: [analytics.service.js](src/services/analytics.service.js), Database schema

## Caching Strategy

### Cache Layers

```
Request → Cache Check → Cache Hit? → Return from Cache
                ↓ No
          Database Query → Store in Cache → Return Result
```

### Cache Keys Pattern
- Posts: `post:{postId}`
- User posts: `user_posts:{userId}`
- Sessions: `session:{userId}`
- Rate limits: `rate_limit:{identifier}`
- Trending: `trending_posts`

### TTL Strategy
- **Posts**: 1 hour (3600s)
- **Sessions**: 24 hours (86400s)
- **Analytics**: 5 minutes (300s)
- **Trending**: 5 minutes (300s)
- **Rate limits**: 15 minutes (900s)

### Cache Invalidation
- On UPDATE: Invalidate specific post cache
- On DELETE: Invalidate post + related caches
- Pattern deletion: `posts:*` for list invalidation

## Database Design

### Normalization
- 3NF (Third Normal Form)
- No redundant data
- Atomic values
- Proper relationships

### Indexes Strategy
```sql
-- Primary keys: Auto-indexed
-- Foreign keys: B-tree indexes
CREATE INDEX idx_posts_user_id ON posts(user_id);

-- Sorting/Filtering
CREATE INDEX idx_posts_created_at ON posts(created_at DESC);

-- Full-text search
CREATE INDEX idx_posts_content_trgm ON posts USING gin(content gin_trgm_ops);

-- Composite for common queries
CREATE INDEX idx_daily_analytics_user_date ON daily_analytics(user_id, date);
```

### Data Integrity
- Foreign key constraints
- Check constraints
- Unique constraints
- NOT NULL constraints
- Triggers for updated_at

## Security Layers

### 1. Network Layer
- CORS configuration
- Helmet.js headers
- Rate limiting

### 2. Application Layer
- Authentication middleware
- Authorization checks
- Input validation
- Output sanitization

### 3. Data Layer
- Password hashing
- Prepared statements (SQL injection prevention)
- Audit logging
- Checksum verification

### 4. Monitoring Layer
- Request logging
- Error logging
- Performance tracking
- Audit trail

## Performance Optimizations

### 1. Database Level
- Connection pooling
- Query optimization
- Proper indexing
- Batch operations

### 2. Application Level
- Redis caching
- Asynchronous operations
- Compression middleware
- Efficient algorithms

### 3. Network Level
- Gzip compression
- Response optimization
- Rate limiting

## Monitoring and Observability

### Logging Strategy
```
HTTP Request → Logger Middleware → Log entry
     ↓
Error occurs → Error Logger → Error log file
     ↓
Audit event → Audit Logger → Database
```

### Log Levels
- **ERROR**: System errors, failures
- **WARN**: Warnings, degraded performance
- **INFO**: General information, startup
- **HTTP**: HTTP request/response logs
- **DEBUG**: Detailed debug information

### Metrics to Monitor
- Request rate
- Response times
- Cache hit/miss ratio
- Database connection pool usage
- Error rates
- User activity

## Scalability Considerations

### Horizontal Scaling
- Stateless application design
- Session storage in Redis
- Database connection pooling
- Load balancer ready

### Vertical Scaling
- Optimized queries
- Efficient caching
- Connection reuse
- Memory management

### Future Enhancements
- Database replication (read replicas)
- Redis cluster
- Message queue (RabbitMQ/Kafka)
- Microservices architecture
- API Gateway
- CDN for static assets

## Error Handling Strategy

### Levels
1. **Application Errors**: Caught and logged
2. **Database Errors**: Transaction rollback
3. **Network Errors**: Retry mechanism
4. **Validation Errors**: User-friendly messages

### Error Response Format
```json
{
  "success": false,
  "error": "Error message",
  "details": [] // Optional validation details
}
```

## Testing Strategy

### Unit Tests (Recommended)
- Service layer functions
- Utility functions
- Validation schemas

### Integration Tests (Recommended)
- API endpoints
- Database operations
- Cache operations

### Load Tests
- High concurrency
- Cache performance
- Database stress

## Development Best Practices

1. **Code Organization**: Modular, single responsibility
2. **Error Handling**: Comprehensive try-catch blocks
3. **Logging**: Detailed, contextual information
4. **Security**: Defense in depth
5. **Performance**: Cache-first approach
6. **Maintainability**: Clear naming, comments
7. **Scalability**: Stateless design

## Deployment Considerations

### Environment Variables
- Database credentials
- API keys
- JWT secrets
- Feature flags

### Health Checks
- `/health` endpoint
- Database connectivity
- Redis connectivity
- Service status

### Graceful Shutdown
- SIGTERM handling
- Connection cleanup
- Request completion

---

This architecture provides a solid foundation for a production-ready application with security, performance, and scalability in mind.
