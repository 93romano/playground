import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import dotenv from 'dotenv';

import logger, { httpLogger } from './utils/logger.js';
import authRoutes from './routes/auth.routes.js';
import postRoutes from './routes/post.routes.js';
import analyticsRoutes from './routes/analytics.routes.js';
import pgPool from './config/database.js';
import redisClient from './config/redis.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// CIA Triad: Confidentiality - Security headers
app.use(helmet());

// CORS configuration
app.use(cors({
  origin: process.env.CORS_ORIGIN || '*',
  credentials: true,
}));

// Compression for better performance - Big Data 3V: Velocity
app.use(compression());

// Body parsing
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// HTTP request logging - CIA Triad: Integrity & Accountability
app.use(httpLogger);

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    // Check PostgreSQL connection
    await pgPool.query('SELECT 1');

    // Check Redis connection
    await redisClient.ping();

    res.json({
      success: true,
      message: 'Service is healthy',
      timestamp: new Date().toISOString(),
      services: {
        postgresql: 'connected',
        redis: 'connected',
      },
    });
  } catch (error) {
    logger.error('Health check failed', { error: error.message });
    res.status(503).json({
      success: false,
      message: 'Service unhealthy',
      error: error.message,
    });
  }
});

// API routes
app.use('/api/auth', authRoutes);
app.use('/api/posts', postRoutes);
app.use('/api/analytics', analyticsRoutes);

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'Social Media Analytics Platform API',
    version: '1.0.0',
    documentation: '/api/docs',
    features: {
      crud: 'Full CRUD operations on posts, comments, and users',
      ciaTriad: {
        confidentiality: 'Password hashing, JWT authentication, rate limiting',
        integrity: 'Checksum verification, audit logs, input validation',
        availability: 'Redis caching, connection pooling, rate limiting',
      },
      bigData3V: {
        volume: 'Handles large amounts of posts, analytics data with pagination',
        velocity: 'Real-time event tracking, Redis caching, streaming analytics',
        variety: 'Multiple data types (posts, comments, analytics), flexible schemas',
      },
    },
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
  });
});

// Error handler - CIA Triad: Integrity
app.use((err, req, res, next) => {
  logger.error('Unhandled error', {
    error: err.message,
    stack: err.stack,
    url: req.originalUrl,
    method: req.method,
    userId: req.user?.id,
  });

  res.status(err.status || 500).json({
    success: false,
    error: process.env.NODE_ENV === 'production'
      ? 'Internal server error'
      : err.message,
  });
});

// Graceful shutdown
const gracefulShutdown = async () => {
  logger.info('Received shutdown signal, closing connections...');

  try {
    await pgPool.end();
    await redisClient.quit();
    logger.info('All connections closed. Exiting...');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', { error: error.message });
    process.exit(1);
  }
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
app.listen(PORT, () => {
  logger.info(`🚀 Server running on port ${PORT}`);
  logger.info(`📊 Environment: ${process.env.NODE_ENV || 'development'}`);
  logger.info(`🔒 Security: Helmet, CORS, Rate Limiting enabled`);
  logger.info(`💾 Database: PostgreSQL + Redis`);
  logger.info(`📈 Features: CRUD, CIA Triad, Big Data 3V`);
});

export default app;
