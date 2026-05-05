import express from 'express';
import postService from '../services/post.service.js';
import analyticsService from '../services/analytics.service.js';
import { authenticate } from '../middleware/auth.middleware.js';
import { normalRateLimit, lenientRateLimit } from '../middleware/rateLimit.middleware.js';
import { validate, schemas } from '../middleware/validation.middleware.js';

const router = express.Router();

// CREATE - Create new post
router.post('/', authenticate, normalRateLimit, validate(schemas.createPost), async (req, res) => {
  try {
    const post = await postService.createPost(req.user.id, req.body);

    // Track analytics event - Big Data 3V: Velocity
    await analyticsService.trackEvent('post_created', {
      userId: req.user.id,
      postId: post.id,
      sessionId: req.headers['x-session-id'],
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'],
      metadata: { mediaType: post.media_type },
    });

    res.status(201).json({
      success: true,
      message: 'Post created successfully',
      data: post,
    });
  } catch (error) {
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// READ - Get all posts with pagination
router.get('/', lenientRateLimit, async (req, res) => {
  try {
    const { page, limit, sortBy } = req.query;

    const result = await postService.getPosts({
      page: parseInt(page) || 1,
      limit: parseInt(limit) || 20,
      sortBy: sortBy || 'created_at',
    });

    res.json({
      success: true,
      data: result,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// READ - Get trending posts
router.get('/trending', lenientRateLimit, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const posts = await postService.getTrendingPosts(limit);

    res.json({
      success: true,
      data: posts,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// READ - Search posts
router.get('/search', lenientRateLimit, async (req, res) => {
  try {
    const { q, page, limit } = req.query;

    if (!q) {
      return res.status(400).json({
        success: false,
        error: 'Search query is required',
      });
    }

    const posts = await postService.searchPosts(q, {
      page: parseInt(page) || 1,
      limit: parseInt(limit) || 20,
    });

    res.json({
      success: true,
      data: posts,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// READ - Get post by ID
router.get('/:id', lenientRateLimit, async (req, res) => {
  try {
    const post = await postService.getPostById(req.params.id, req.user?.id);

    // Track analytics event - Big Data 3V: Velocity
    await analyticsService.trackEvent('page_view', {
      userId: req.user?.id,
      postId: req.params.id,
      sessionId: req.headers['x-session-id'],
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'],
      metadata: {},
    });

    res.json({
      success: true,
      data: post,
    });
  } catch (error) {
    res.status(404).json({
      success: false,
      error: error.message,
    });
  }
});

// UPDATE - Update post
router.put('/:id', authenticate, normalRateLimit, validate(schemas.updatePost), async (req, res) => {
  try {
    const post = await postService.updatePost(req.params.id, req.user.id, req.body);

    // Track analytics event
    await analyticsService.trackEvent('post_updated', {
      userId: req.user.id,
      postId: req.params.id,
      sessionId: req.headers['x-session-id'],
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'],
      metadata: { fields: Object.keys(req.body) },
    });

    res.json({
      success: true,
      message: 'Post updated successfully',
      data: post,
    });
  } catch (error) {
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// DELETE - Delete post
router.delete('/:id', authenticate, normalRateLimit, async (req, res) => {
  try {
    const result = await postService.deletePost(req.params.id, req.user.id);

    // Track analytics event
    await analyticsService.trackEvent('post_deleted', {
      userId: req.user.id,
      postId: req.params.id,
      sessionId: req.headers['x-session-id'],
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'],
      metadata: {},
    });

    res.json({
      success: true,
      message: 'Post deleted successfully',
      data: result,
    });
  } catch (error) {
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

export default router;
