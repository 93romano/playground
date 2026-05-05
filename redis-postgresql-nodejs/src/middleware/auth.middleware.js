import authService from '../services/auth.service.js';

// CIA Triad: Confidentiality - Authentication middleware
export const authenticate = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        error: 'No token provided',
      });
    }

    const token = authHeader.substring(7);
    const user = await authService.verifyToken(token);

    req.user = user;
    next();
  } catch (error) {
    return res.status(401).json({
      success: false,
      error: 'Invalid or expired token',
    });
  }
};

// CIA Triad: Confidentiality - Authorization middleware (role-based access control)
export const authorize = (...roles) => {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required',
      });
    }

    if (!roles.includes(req.user.role)) {
      return res.status(403).json({
        success: false,
        error: 'Insufficient permissions',
      });
    }

    next();
  };
};

// Check if user owns the resource
export const checkOwnership = (resourceUserIdGetter) => {
  return async (req, res, next) => {
    try {
      const resourceUserId = await resourceUserIdGetter(req);

      if (req.user.id !== resourceUserId && req.user.role !== 'admin') {
        return res.status(403).json({
          success: false,
          error: 'Unauthorized access to resource',
        });
      }

      next();
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: 'Failed to verify ownership',
      });
    }
  };
};
