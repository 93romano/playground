// 공통 타입 정의
export interface User {
  id: string;
  username: string;
  email: string;
  password?: string;
  created_at: Date;
  updated_at: Date;
}

export interface Message {
  id: string;
  content: string;
  user_id: string;
  room_id: string;
  created_at: Date;
  user?: User;
}

export interface Room {
  id: string;
  name: string;
  description?: string;
  created_by: string;
  created_at: Date;
  updated_at: Date;
}

export interface AuthTokens {
  accessToken: string;
  refreshToken: string;
}

export interface JWTPayload {
  userId: string;
  username: string;
  iat?: number;
  exp?: number;
}

// API 요청/응답 타입
export interface LoginRequest {
  email: string;
  password: string;
}

export interface RegisterRequest {
  username: string;
  email: string;
  password: string;
}

export interface SendMessageRequest {
  content: string;
  room_id: string;
}

export interface CreateRoomRequest {
  name: string;
  description?: string;
}

// Socket 이벤트 타입
export interface ServerToClientEvents {
  message: (message: Message) => void;
  userJoined: (user: User) => void;
  userLeft: (user: User) => void;
  onlineUsers: (users: User[]) => void;
  error: (error: string) => void;
}

export interface ClientToServerEvents {
  joinRoom: (roomId: string) => void;
  leaveRoom: (roomId: string) => void;
  sendMessage: (data: SendMessageRequest) => void;
}

export interface InterServerEvents {
  ping: () => void;
}

export interface SocketData {
  userId: string;
  username: string;
}

// 에러 타입
export interface ApiError {
  status: number;
  message: string;
  code?: string;
}

// 환경변수 타입
export interface Config {
  port: number;
  nodeEnv: string;
  corsOrigin: string;
  database: {
    host: string;
    port: number;
    name: string;
    user: string;
    password: string;
  };
  redis: {
    host: string;
    port: number;
    password?: string;
  };
  jwt: {
    secret: string;
    expiresIn: string;
    refreshSecret: string;
    refreshExpiresIn: string;
  };
  rateLimit: {
    windowMs: number;
    maxRequests: number;
  };
  logLevel: string;
}

