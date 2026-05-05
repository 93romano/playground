import Joi from 'joi';

// CIA Triad: Integrity - Input validation to maintain data integrity
export const validate = (schema) => {
  return (req, res, next) => {
    const { error, value } = schema.validate(req.body, {
      abortEarly: false,
      stripUnknown: true,
    });

    if (error) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        details: error.details.map((detail) => ({
          field: detail.path.join('.'),
          message: detail.message,
        })),
      });
    }

    req.body = value;
    next();
  };
};

// Validation schemas
export const schemas = {
  register: Joi.object({
    username: Joi.string().alphanum().min(3).max(50).required(),
    email: Joi.string().email().required(),
    password: Joi.string().min(8).max(128).required(),
    role: Joi.string().valid('user', 'admin').default('user'),
  }),

  login: Joi.object({
    email: Joi.string().email().required(),
    password: Joi.string().required(),
  }),

  changePassword: Joi.object({
    oldPassword: Joi.string().required(),
    newPassword: Joi.string().min(8).max(128).required(),
  }),

  createPost: Joi.object({
    title: Joi.string().min(1).max(255).required(),
    content: Joi.string().min(1).required(),
    mediaType: Joi.string().valid('text', 'image', 'video', 'link').default('text'),
    mediaUrl: Joi.string().uri().allow(null, ''),
  }),

  updatePost: Joi.object({
    title: Joi.string().min(1).max(255),
    content: Joi.string().min(1),
    mediaType: Joi.string().valid('text', 'image', 'video', 'link'),
    mediaUrl: Joi.string().uri().allow(null, ''),
    status: Joi.string().valid('draft', 'published', 'archived'),
  }).min(1),

  createComment: Joi.object({
    content: Joi.string().min(1).required(),
    parentCommentId: Joi.string().uuid().allow(null),
  }),

  pagination: Joi.object({
    page: Joi.number().integer().min(1).default(1),
    limit: Joi.number().integer().min(1).max(100).default(20),
    sortBy: Joi.string().valid('created_at', 'popular', 'views').default('created_at'),
  }),
};
