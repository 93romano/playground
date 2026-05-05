# Real-time Chat Application

TypeScript 기반 풀스택 실시간 채팅 애플리케이션

## 아키텍처

### Frontend (SPA)
- 순수 TypeScript + Webpack
- Socket.io 클라이언트
- MVC 패턴
- 클라이언트 사이드 라우팅

### Backend
- Node.js + TypeScript + Express
- Socket.io 서버
- JWT 인증
- PostgreSQL + Redis

### AWS 배포
- S3 (Frontend 정적 호스팅)
- EC2 (Backend + Database)
- CloudFront (CDN)

## 개발 환경 설정

```bash
# 루트에서 전체 설치
npm install

# 개발 서버 실행 (프론트엔드 + 백엔드 동시)
npm run dev

# 개별 실행
npm run dev:frontend
npm run dev:backend
```

## 통신 흐름

1. **사용자 인증**: Frontend → Backend API → PostgreSQL → JWT Token
2. **실시간 연결**: Frontend → Socket.io → Backend → Redis 세션
3. **메시지 전송**: Frontend → Socket.io → Backend → Redis (실시간) + PostgreSQL (영구저장)
4. **메시지 수신**: PostgreSQL → Backend → Socket.io → Frontend

## 디자인 패턴

- **Frontend**: MVC, Observer, Component, Router
- **Backend**: Repository, Service Layer, Factory, Middleware
- **Database**: Cache-Aside, Connection Pool
- **Real-time**: Pub/Sub, Room Pattern
