import express from 'express';
import analyticsService from '../services/analytics.service.js';
import { authenticate, authorize } from '../middleware/auth.middleware.js';
import { lenientRateLimit } from '../middleware/rateLimit.middleware.js';

const router = express.Router();

// Get real-time analytics - Big Data 3V: Velocity
router.get('/realtime', authenticate, lenientRateLimit, async (req, res) => {
  try {
    const analytics = await analyticsService.getRealtimeAnalytics();

    res.json({
      success: true,
      data: analytics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Get user analytics - Big Data 3V: Variety
router.get('/user/:userId?', authenticate, lenientRateLimit, async (req, res) => {
  try {
    // Users can only see their own analytics unless they're admin
    const userId = req.params.userId || req.user.id;

    if (userId !== req.user.id && req.user.role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Unauthorized access',
      });
    }

    const days = parseInt(req.query.days) || 30;
    const analytics = await analyticsService.getUserAnalytics(userId, days);

    res.json({
      success: true,
      data: analytics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Get platform-wide analytics - Big Data 3V: Volume
// Admin only
router.get('/platform', authenticate, authorize('admin'), lenientRateLimit, async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 30;
    const analytics = await analyticsService.getPlatformAnalytics(days);

    res.json({
      success: true,
      data: analytics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Get activity heatmap - Big Data 3V: Variety
router.get('/heatmap/:userId?', authenticate, lenientRateLimit, async (req, res) => {
  try {
    const userId = req.params.userId || req.user.id;

    if (userId !== req.user.id && req.user.role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Unauthorized access',
      });
    }

    const days = parseInt(req.query.days) || 30;
    const heatmap = await analyticsService.getActivityHeatmap(userId, days);

    res.json({
      success: true,
      data: heatmap,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Track custom event - Big Data 3V: Velocity
router.post('/track', lenientRateLimit, async (req, res) => {
  try {
    const { eventType, metadata } = req.body;

    const result = await analyticsService.trackEvent(eventType, {
      userId: req.user?.id,
      postId: req.body.postId,
      sessionId: req.headers['x-session-id'],
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'],
      metadata: metadata || {},
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

export default router;
