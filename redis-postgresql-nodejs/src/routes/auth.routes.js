import express from 'express';
import authService from '../services/auth.service.js';
import { authenticate } from '../middleware/auth.middleware.js';
import { strictRateLimit } from '../middleware/rateLimit.middleware.js';
import { validate, schemas } from '../middleware/validation.middleware.js';

const router = express.Router();

// Register - CREATE
router.post('/register', strictRateLimit, validate(schemas.register), async (req, res) => {
  try {
    const { username, email, password, role } = req.body;

    const user = await authService.register(username, email, password, role);

    res.status(201).json({
      success: true,
      message: 'User registered successfully',
      data: user,
    });
  } catch (error) {
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// Login - READ (authenticate and get token)
router.post('/login', strictRateLimit, validate(schemas.login), async (req, res) => {
  try {
    const { email, password } = req.body;
    const ipAddress = req.ip;

    const result = await authService.login(email, password, ipAddress);

    res.json({
      success: true,
      message: 'Login successful',
      data: result,
    });
  } catch (error) {
    res.status(401).json({
      success: false,
      error: error.message,
    });
  }
});

// Logout - DELETE session
router.post('/logout', authenticate, async (req, res) => {
  try {
    await authService.logout(req.user.id);

    res.json({
      success: true,
      message: 'Logout successful',
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Change password - UPDATE
router.post('/change-password', authenticate, strictRateLimit, validate(schemas.changePassword), async (req, res) => {
  try {
    const { oldPassword, newPassword } = req.body;

    await authService.changePassword(req.user.id, oldPassword, newPassword);

    res.json({
      success: true,
      message: 'Password changed successfully',
    });
  } catch (error) {
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// Get current user - READ
router.get('/me', authenticate, async (req, res) => {
  try {
    res.json({
      success: true,
      data: req.user,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

export default router;
