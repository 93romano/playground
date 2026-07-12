# 브라우저 & JS 런타임 내부 — 로우지식 정리

> **핵심 질문: "내가 쓴 JS 한 줄이 화면의 픽셀이 되기까지 무슨 일이 일어나는가?"**

프론트엔드 엔지니어의 코어 도메인을 **바닥까지** 파고드는 학습 모듈. `const x = a + b` 한 줄이 파싱되고, 바이트코드가 되고, JIT 최적화되고, 이벤트 루프에 실려, 레이아웃·페인트·합성을 거쳐 **픽셀**이 되기까지의 전 과정을 "왜 그렇게 설계됐는가" 중심으로 정리한다. 프레임워크가 아니라 그 **아래의 기계**를 다룬다.

이 모듈은 **파인만 학습법**을 따른다: 큰 개념을 더 못 쪼갤 때까지 **원자(atom)** 로 분해하고, 각 원자를 **10살에게 설명할 수 있으면** 안다고 인정한다.

---

## 로드맵에서의 위치 (4단계 중 4단계)

이 모듈은 아래로부터 쌓아 올린 지식의 **꼭대기**다. 픽셀까지의 여정은 사실 하드웨어→OS→네트워크→브라우저의 사슬 전체를 지난다.

```
1단계  컴퓨터 구조   캐시 · 파이프라인 · 분기예측 · 메모리 계층    → low-knowledge/claude
2단계  운영체제      프로세스/스레드 · epoll · 스케줄링 · 가상메모리 → low-knowledge/os      (동시 작성 중)
3단계  네트워크      DNS · TCP/TLS · HTTP/2·3 · RTT               → low-knowledge/network (동시 작성 중)
4단계  브라우저      V8 · GC · 이벤트루프 · 렌더링 · 로딩          → 여기 (low-knowledge/browser)
```

> 각 문서 끝의 **"이전 계층과의 연결"** 이 이 사슬을 명시적으로 잇는다. 예: 히든클래스 ↔ 1단계 캐시 지역성, 이벤트 루프 ↔ 2단계 epoll, 사이트 격리 ↔ 1단계 Spectre, preconnect ↔ 3단계 RTT.

### 선수 지식 (1단계, 강력 권장)

먼저 읽으면 이 모듈의 "이전 계층과의 연결"이 완전히 이해된다.

- [claude/topics/03-pipeline.md](../claude/topics/03-pipeline.md) — 파이프라인, 분기 예측 (→ JIT speculation, 인터프리터 dispatch)
- [claude/topics/04-microarchitecture.md](../claude/topics/04-microarchitecture.md) — 투기 실행 (→ TurboFan speculation / deopt)
- [claude/topics/06-cache.md](../claude/topics/06-cache.md) — 캐시·지역성 (→ inline cache, 히든클래스, GC compaction)
- [claude/topics/12-security-side-channel.md](../claude/topics/12-security-side-channel.md) — Spectre (→ 사이트 격리)

---

## 문서 목록

| # | 주제 | 핵심 질문 (한 줄) |
|---|------|------------------|
| [01](topics/01-v8-pipeline.md) | V8 파이프라인 | 같은 코드가 왜 두 번째부터 빨라지나? (파서→Ignition→TurboFan, 히든클래스, IC, deopt) |
| [02](topics/02-memory-and-gc.md) | 메모리 & GC | 내 객체는 언제 사라지나? 왜 안 사라져서 누수가 되나? (세대별 GC, WeakMap) |
| [03](topics/03-event-loop.md) | 이벤트 루프 | 싱글 스레드가 어떻게 동시에 일하나? `await` 다음 줄은 언제 도나? (매크로/마이크로 큐) |
| [04](topics/04-rendering-pipeline.md) | 렌더링 파이프라인 | JS 한 줄이 픽셀이 되는 경로는? 왜 `transform`은 부드럽나? (reflow/repaint/composite, 16.6ms) |
| [05](topics/05-browser-architecture.md) | 브라우저 아키텍처 | 탭 하나가 죽어도 왜 멀쩡하나? 사이트끼리 메모리를 어떻게 격리하나? (멀티프로세스, 사이트 격리) |
| [06](topics/06-loading-performance.md) | 로딩 & Core Web Vitals | 왜 어떤 사이트는 즉시 뜨나? script 태그 하나가 뭘 좌우하나? (파서 블로킹, LCP/INP/CLS, 하이드레이션) |

**추천 독서 순서**: 01 → 02 → 03 → 04 → 05 → 06 (엔진 → 메모리 → 시간축 → 픽셀 → 프로세스 구조 → 실무 종합). 급하면 03 → 04 → 06만 읽어도 실무의 8할.

---

## 각 문서의 구조 (파인만 구조)

모든 문서가 동일한 틀을 따른다. 원하는 깊이만 골라 읽어라.

1. **핵심 질문 1개** — 이 문서가 답하는 단 하나의 질문
2. **TL;DR** — 5줄 요약
3. **원자 분해** — 개념을 못 쪼갤 때까지 나눈 트리
4. **본문** — 소제목 하나 = 원자 하나, 도식(SVG/mermaid/ASCII) 포함
5. **프론트엔드에서 이렇게 만난다** — 실무 코드/함정
6. **이전 계층과의 연결** — 아래 계층(HW/OS/네트워크)과 잇기
7. **파인만 체크** — 10살에게 설명하는 3개의 과제
8. **DevTools로 직접 관찰** — Performance/Memory/Network 탭 등으로 눈으로 확인

---

## 파인만 학습 루틴

```
1. 핵심 질문을 소리 내어 읽는다 → 지금 내 답을 30초간 말해본다 (막히는 곳 = 구멍)
2. 원자 분해 트리를 보고, 각 원자를 "안다/모른다"로 표시한다
3. 모르는 원자만 본문을 읽는다
4. DevTools로 직접 재현한다 (숫자를 눈으로 봐야 진짜 안다)
5. 파인만 체크 3개를 10살 조카에게 설명하듯 말/글로 푼다
   → 전문 용어 없이 설명 못 하면 아직 모르는 것. 3번으로 돌아간다
6. "이전 계층과의 연결"로 아래 계층과 묶어, 고립된 지식이 아니라 사슬로 만든다
```

> 파인만의 핵심: **"쉽게 설명 못 하면 이해 못 한 것이다."** 체크 문항을 통과하지 못하면 진도를 나가지 말 것.

---

## 빠른 참조 (핵심 숫자)

프론트엔드 성능 논의에서 반복 등장하는 상수. 외워두면 감이 선다.

| 항목 | 값 | 출처 문서 |
|------|-----|-----------|
| 프레임 예산 (60Hz) | **16.6ms** (JS 실전 여유 ~10ms) | [04](topics/04-rendering-pipeline.md) |
| 프레임 예산 (120Hz) | **8.3ms** | [04](topics/04-rendering-pipeline.md) |
| Long Task 기준 | **50ms** 초과 | [03](topics/03-event-loop.md) |
| Minor GC (Scavenge) pause | 보통 **< 1ms** | [02](topics/02-memory-and-gc.md) |
| Major GC main-thread pause | 목표 **수 ms 이하** (incremental/concurrent) | [02](topics/02-memory-and-gc.md) |
| New space 크기 | 파티션당 **~1–16MB** | [02](topics/02-memory-and-gc.md) |
| Old space 승격 조건 | Scavenge **2회** 생존 | [02](topics/02-memory-and-gc.md) |
| LCP (good) | **≤ 2.5s** | [06](topics/06-loading-performance.md) |
| INP (good) | **≤ 200ms** | [06](topics/06-loading-performance.md) |
| CLS (good) | **≤ 0.1** | [06](topics/06-loading-performance.md) |
| IC 상태 | mono(1) → poly(2–4) → **mega(5+)** | [01](topics/01-v8-pipeline.md) |
| V8 실행 tier | Ignition → Sparkplug → Maglev → TurboFan | [01](topics/01-v8-pipeline.md) |

### 컴포지터 전용 속성 (레이아웃·페인트 건너뜀 → 60fps 애니메이션)

```
transform   opacity
```

그 외(`width`, `top`, `left`, `margin`, `box-shadow`...)는 Layout 또는 Paint를 다시 유발한다.

### 태스크 우선순위 (이벤트 루프)

```
동기 코드  >  마이크로태스크(Promise/await, 전부 소진)  >  매크로태스크(setTimeout, 1개)  >  렌더(rAF)
```

---

## 작성 원칙 (이 모듈의)

- 본문은 한국어, 핵심 용어는 영어 병기
- "왜 그렇게 설계되었는가"를 중심으로 서술
- 구체적 숫자(프레임 예산·GC pause·CWV 임계값)와 실행 가능한 코드 예시 포함
- 각 문서에 **DevTools 직접 관찰법 1개 이상**
- 도식은 SVG(모듈 핵심 4개: V8 파이프라인·GC·이벤트 루프·렌더링) + mermaid + ASCII
- 각 원자를 **10살에게 설명 가능**할 때까지 쪼갠다 (파인만)
