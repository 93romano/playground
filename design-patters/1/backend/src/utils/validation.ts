import Joi from 'joi';
import { LoginRequest, RegisterRequest, SendMessageRequest, CreateRoomRequest } from '@/types';

// 사용자 등록 검증
export const registerSchema = Joi.object<RegisterRequest>({
  username: Joi.string()
    .alphanum()
    .min(3)
    .max(30)
    .required()
    .messages({
      'string.alphanum': 'Username must contain only alphanumeric characters',
      'string.min': 'Username must be at least 3 characters long',
      'string.max': 'Username must not exceed 30 characters',
      'any.required': 'Username is required'
    }),
  
  email: Joi.string()
    .email()
    .required()
    .messages({
      'string.email': 'Please provide a valid email address',
      'any.required': 'Email is required'
    }),
  
  password: Joi.string()
    .min(6)
    .max(128)
    .pattern(new RegExp('^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#\$%\^&\*])'))
    .required()
    .messages({
      'string.min': 'Password must be at least 6 characters long',
      'string.max': 'Password must not exceed 128 characters',
      'string.pattern.base': 'Password must contain at least one lowercase letter, one uppercase letter, one number, and one special character',
      'any.required': 'Password is required'
    })
});

// 로그인 검증
export const loginSchema = Joi.object<LoginRequest>({
  email: Joi.string()
    .email()
    .required()
    .messages({
      'string.email': 'Please provide a valid email address',
      'any.required': 'Email is required'
    }),
  
  password: Joi.string()
    .required()
    .messages({
      'any.required': 'Password is required'
    })
});

// 메시지 전송 검증
export const sendMessageSchema = Joi.object<SendMessageRequest>({
  content: Joi.string()
    .min(1)
    .max(1000)
    .trim()
    .required()
    .messages({
      'string.min': 'Message cannot be empty',
      'string.max': 'Message must not exceed 1000 characters',
      'any.required': 'Message content is required'
    }),
  
  room_id: Joi.string()
    .uuid()
    .required()
    .messages({
      'string.uuid': 'Invalid room ID format',
      'any.required': 'Room ID is required'
    })
});

// 채팅방 생성 검증
export const createRoomSchema = Joi.object<CreateRoomRequest>({
  name: Joi.string()
    .min(1)
    .max(100)
    .trim()
    .required()
    .messages({
      'string.min': 'Room name cannot be empty',
      'string.max': 'Room name must not exceed 100 characters',
      'any.required': 'Room name is required'
    }),
  
  description: Joi.string()
    .max(500)
    .trim()
    .optional()
    .allow('')
    .messages({
      'string.max': 'Description must not exceed 500 characters'
    })
});

// UUID 검증
export const uuidSchema = Joi.string().uuid().required();

// 검증 헬퍼 함수
export const validate = <T>(schema: Joi.ObjectSchema<T>, data: unknown): T => {
  const { error, value } = schema.validate(data);
  
  if (error) {
    throw new Error(error.details[0]?.message || 'Validation error');
  }
  
  return value;
};

