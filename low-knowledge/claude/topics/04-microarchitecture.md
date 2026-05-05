# 04. 마이크로아키텍쳐 (Modern Microarchitecture)

## TL;DR

현대 고성능 CPU는 (1) **Superscalar**: 한 cycle에 여러 명령 발사, (2) **Out-of-Order(OoO)**: 데이터 의존성만 지키면 순서 무시, (3) **Speculative Execution**: 분기 결과를 추측해 미리 실행, (4) **Register Renaming**: 가짜 의존성 제거 — 의 조합으로 IPC를 끌어올린다. 이 모든 것이 분기 예측과 캐시에 의존하기 때문에, 캐시 미스 한 번이 마이크로아키텍쳐의 모든 노력을 무위로 돌릴 수 있다.

---

## 기초 (면접/CS)

### Superscalar

한 cycle에 여러 명령을 fetch/decode/execute. 예: Skylake는 4-wide decode, 8 execution port.
→ 이론적 IPC > 1 가능. 잘 짠 SIMD 코드는 IPC 4~5 도달.

### Out-of-Order Execution (OoO)

```c
load r1, [a]     ; 캐시 미스 → 100 cycle 대기
add  r2, r3, r4  ; r1과 무관 → 기다리지 않고 먼저 실행!
mul  r5, r2, r6
```
순서를 바꿔도 결과가 같으면 먼저 실행. **메모리 latency를 다른 일로 가린다(memory level parallelism).**

### Speculative Execution

분기 예측 결과를 기반으로 **결과가 확정되기 전에 미리 실행**.
- 예측 맞음 → 결과 commit → 큰 이득
- 예측 틀림 → 모두 폐기(flush) → 손실

→ Spectre/Meltdown(12장)이 여기서 유래.

### Register Renaming

```asm
add r1, r2, r3    ; (1)
add r4, r1, r5    ; (2) — (1)과 RAW 의존
add r1, r6, r7    ; (3) — (1)과 WAW 가짜 의존
add r8, r1, r9    ; (4) — (3)과 RAW
```
(1)과 (3)은 같은 r1에 쓰지만 의미적으론 다른 변수. 물리 레지스터를 새로 할당해 분리:

```
r1_v1 ← r2 + r3   ; (1)
r4    ← r1_v1 + r5 ; (2)
r1_v2 ← r6 + r7   ; (3)  ← 가짜 의존 제거! (1)과 병렬 가능
r8    ← r1_v2 + r9 ; (4)
```

❓ **면접: "OoO는 왜 가능한가? 프로그램은 순차인데?"** 의존성만 지키면 순서를 바꿔도 결과 동일. 단 **외부에서 보이는 순서는 in-order로 commit** (Reorder Buffer가 보장).

---

## 심화 (마이크로아키텍쳐)

### 전체 OoO 파이프라인 그림

```
            Front-end (in-order)             Back-end (out-of-order)            Retire (in-order)
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────────┐ ┌──────────┐ ┌────────┐ ┌────────┐
│ Fetch  │→│ Decode │→│ Rename │→│Dispatch│→│Issue Queue │→│ Execute  │→│Writeback│→│ Retire │
│  (BPU) │ │  (μop) │ │  (RAT) │ │        │ │ (RS/Sched) │ │ (FU x N) │ │         │ │ (ROB)  │
└────────┘ └────────┘ └────────┘ └────────┘ └────────────┘ └──────────┘ └────────┘ └────────┘
                                                                                    ↓
                                                                          architecturally visible
```

### 핵심 구조물

#### Reorder Buffer (ROB)
inflight 중인 모든 μop을 프로그램 순서로 보관. retire는 ROB head에서만 일어남 → **외부에서 보이는 순서 = 프로그램 순서**, exception도 정확한 PC에서 발생.
- Skylake: 224 entry
- Apple M1 Firestorm: ~630 entry (압도적)
- Zen 4: ~320 entry

#### Reservation Station / Scheduler
피연산자가 준비된 μop을 골라 실행 유닛으로 보냄. CAM(Content-Addressable Memory)로 broadcast 결과를 매칭.

#### Physical Register File (PRF)
아키텍쳐 레지스터 16개의 "버전들"을 모두 보관. Skylake INT PRF는 180개.

#### Load/Store Queue
- **Store Buffer**: 아직 캐시에 commit 안 된 store
- **Load Queue**: inflight load의 주소 추적
- **Store-to-Load Forwarding**: 같은 주소로 가는 store→load는 캐시 거치지 않고 직접 전달

### 분기 예측 (심화)

#### 종류
- **Local predictor**: 각 분기 PC별 history (BHT)
- **Global predictor**: 최근 분기들의 패턴 (GHR)
- **Tournament**: 둘을 선택적으로 결합
- **TAGE**: 다양한 길이의 history 테이블, 현대 거의 모든 고성능 CPU의 기본

#### Branch Target Buffer (BTB)
간접 분기/함수 포인터의 목적지 예측. 가상 함수 호출이 비싼 이유 = BTB miss.

#### Return Address Stack (RAS)
함수 리턴 주소를 별도 스택에 저장. 깊이는 보통 16~32. 깊은 재귀는 RAS overflow → 예측 실패.

### Instruction-Level Parallelism (ILP)의 한계

ILP는 보통 워크로드에서 4~8 정도가 한계. 그 이상 끌어내려면 **Thread-Level Parallelism (TLP)** 필요 = SMT/Hyperthreading.

#### SMT (Hyperthreading)
물리 코어 1개 × 논리 코어 2개. 한 스레드 stall 시 다른 스레드 실행 → 자원 활용률 ↑.
- 메모리 대역폭/캐시는 공유 → CPU-bound 잘 짠 코드에서는 오히려 손해
- Apple Silicon은 SMT 없음 (P-core/E-core 비대칭)

### 실제 칩 비교

| 칩 | Decode | Issue | ROB | INT ALU | FP/SIMD | LSU |
|----|--------|-------|-----|---------|---------|-----|
| Intel Skylake | 4+1 | 8 | 224 | 4 | 2x256bit | 2 ld + 1 st |
| AMD Zen 4 | 4 | 6 | 320 | 4 | 2x256bit | 3 ld + 2 st |
| Apple M1 Firestorm | 8 | ~13 | 630 | 6 | 4x128bit | 3 ld + 2 st |

> Apple Silicon이 빠른 이유의 본질: **front-end가 광폭(8-wide)** + **거대한 ROB** + **막대한 PRF**. ARM 고정 길이 명령이 가능하게 함.

---

## 실무 성능 관점

### 1. 진짜 적은 의존성을 만들자

```c
// dependency chain → IPC 1
for (i = 0; i < n; i++) acc = (acc * 31) + s[i];

// 누적기 분리 → IPC 4
uint32_t a0=0,a1=0,a2=0,a3=0;
for (i = 0; i < n; i+=4) {
    a0 = a0*31 + s[i];
    a1 = a1*31 + s[i+1];
    a2 = a2*31 + s[i+2];
    a3 = a3*31 + s[i+3];
}
```

### 2. 가상 함수 호출 비용

```cpp
class Animal { virtual void speak() = 0; };
for (auto* a : animals) a->speak();   // 매번 BTB 예측 필요
```
- 같은 타입만 들어있으면 BTB 적중 100% → 거의 공짜
- 무작위 타입 섞이면 BTB miss → 함수 호출당 ~15 cycle 추가
- 해결: 타입별로 분리 정렬 후 호출, 또는 `std::variant` + visit 패턴

### 3. 함수 포인터/`switch`/`if-else`

작은 `switch`(8개 이하)는 컴파일러가 jump table → BTB. 의외로 if-else 체인이 더 빠를 때도 있음 (분기 예측이 잘 되면).

### 4. ILP 측정 (toplev / VTune)

```bash
toplev.py --level 2 ./prog
```
→ Front-end Bound / Bad Speculation / Back-end Bound (Memory/Core) 4분류로 병목 진단.

| 카테고리 | 의미 | 처방 |
|---------|------|------|
| Front-end Bound | I-cache miss, 디코딩 한계 | 코드 줄이기, hot path 분리 |
| Bad Speculation | 분기 mispredict | 분기 패턴 개선 |
| Memory Bound | LLC/DRAM 대기 | 캐시 친화적 데이터 구조 |
| Core Bound | 실행 유닛 포화 | SIMD, 알고리즘 변경 |

### 5. L1 I-cache miss는 보이지 않는 적

큰 함수, 깊은 콜 스택, hot/cold 코드 섞여있으면 I-cache 압박 → front-end stall.
PGO (Profile Guided Optimization) + LTO + `__attribute__((cold))`로 cold 경로 분리.

---

## 추가 학습 키워드

- Tomasulo's algorithm, Robert Tomasulo (1967)
- TAGE branch predictor, Perceptron predictor
- Memory disambiguation, memory dependence prediction
- Zero-cycle move (Apple Silicon 특기)
- Loop buffer / micro-op queue
- Macro-op fusion: cmp+jcc → 하나의 μop
- WAR / WAW / RAW 의존성
- Itanium / EPIC — VLIW의 실패
