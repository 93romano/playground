# 03. 파이프라인 (Pipeline)

## TL;DR

명령어 처리를 **여러 단계로 쪼개 동시에 진행**해 throughput을 끌어올리는 기법. 이상적으로 N단계 파이프라인은 throughput을 N배로 만들지만, 실제로는 **해저드(hazard)** 때문에 stall이 발생한다. 데이터 해저드는 **forwarding**으로, 제어 해저드는 **분기 예측**으로, 구조 해저드는 자원 복제로 해결한다.

---

## 기초 (면접/CS)

### 5단계 파이프라인 (MIPS 교과서 모델)

```
사이클:    1     2     3     4     5     6     7     8
명령 1:   IF -> ID -> EX -> MEM -> WB
명령 2:         IF -> ID -> EX -> MEM -> WB
명령 3:               IF -> ID -> EX -> MEM -> WB
명령 4:                     IF -> ID -> EX -> MEM -> WB
```

- **장점**: 각 단계가 1 cycle씩 → throughput **명령어/cycle ≈ 1**
- 실제 실행 시간: latency는 5 cycle 그대로지만, throughput이 5배

### 해저드(Hazard) 종류

#### 1. Data Hazard
```asm
add r1, r2, r3   ; r1 = r2 + r3
sub r4, r1, r5   ; r1을 바로 사용 → 아직 WB 안 됨!
```
**해결: forwarding (bypassing)** — EX 단계 출력을 다음 명령의 EX 단계 입력으로 직접 연결.

#### 2. Load-use Hazard
```asm
lw  r1, 0(r2)    ; 메모리에서 로드
add r3, r1, r4   ; load 결과 즉시 사용 → 1 cycle stall 불가피
```
forwarding으로도 못 막음 → 컴파일러가 **명령 재배열**로 stall 채움.

#### 3. Control Hazard
```asm
beq r1, r2, label   ; 분기 → 다음 PC를 모름
???                 ; IF 단계가 뭘 fetch?
```
**해결: 분기 예측(branch prediction)** + 잘못 예측 시 flush.

#### 4. Structural Hazard
같은 자원을 두 단계가 동시에 쓰려는 충돌. 예: 단일 포트 메모리에서 IF와 MEM 동시 접근 → I/D cache 분리로 해결.

### 분기 예측 (기초)

- **정적 예측**: "분기는 항상 not taken", 또는 "역방향 분기는 taken"(루프 가정)
- **동적 예측**: 과거 결과를 기반으로 예측 (1-bit, 2-bit saturating counter)
- 적중률 ≈ 95%+이면 합격, 90% 이하면 성능 큰 손실

❓ **면접: "파이프라인 단계가 많으면 무조건 좋은가?"** No. 단계가 많을수록 (1) 분기 mispredict 페널티 ↑, (2) 단계 간 latch 비용 ↑, (3) 클럭 ↑하지만 IPC 떨어짐. Pentium 4는 31단계까지 갔다가 실패 → Core 아키텍쳐로 회귀.

---

## 심화 (마이크로아키텍쳐)

### 현대 CPU의 파이프라인은 훨씬 깊다

- Intel Skylake: ~14~19 단계
- Apple M1: ~13~15 단계
- Pentium 4 NetBurst: 20~31 단계 (실패 사례)

각 단계 더 잘게 쪼갠 이유: 클럭을 올리기 위해 = 1 cycle 안에 처리할 일을 줄여야 해서.

### Front-end 파이프라인 (대략)

```
[Fetch] -> [Pre-decode] -> [Decode] -> [μop cache] -> [Rename] -> [Dispatch]
```

- **Fetch**: 16~32 byte씩 fetch. 분기 예측이 여기서 작동.
- **Pre-decode**: x86은 명령 경계를 찾아야 함 (가변 길이)
- **Decode**: 명령을 μop으로 분해. complex decoder 1개 + simple decoder 3개 식.
- **μop cache (DSB)**: 한 번 디코딩한 μop을 캐싱 → 재실행 시 디코딩 우회
- **Rename**: 아키텍쳐 레지스터를 물리 레지스터로 매핑(4장에서 상세)
- **Dispatch**: 실행 유닛으로 분배

### Back-end 파이프라인 (Out-of-Order)

```
[Issue Queue] -> [Execute (다수 유닛)] -> [Writeback] -> [Retire/Commit]
                                                        ↑
                                                    프로그램 순서로
```

- 실행은 순서 무시(OoO)지만 **retire는 in-order** → 외부에서는 순서대로 보임 (precise exception 보장)

### Branch Misprediction Penalty

`misprediction penalty ≈ 파이프라인 깊이 × cycle`

- 분기 예측 실패 → **wrong path에서 진행한 모든 μop을 flush** → fetch부터 다시
- Skylake에서 ~15~20 cycle 손실, AMD Zen ~18 cycle, Apple M ~13 cycle
- 99% 적중률이라도 분기 많은 코드에서는 큰 손실. **분기 없는(branchless) 코드**가 핫 루프에서 종종 더 빠른 이유.

### Loop Unrolling과 파이프라인

```c
for (i = 0; i < n; i++) sum += a[i];
```
→ 매 iter에 분기 + 종료 조건 체크. Unroll하면 분기 비율 ↓, ILP(명령어 수준 병렬성) ↑.
컴파일러가 자동으로 함 (`-O2` 이상). `#pragma unroll`도 가능.

---

## 실무 성능 관점

### 1. 분기 예측 친화적으로 코딩

**❌ 분기 패턴이 무작위:**
```c
for (int i = 0; i < n; i++) {
    if (data[i] > 128) sum += data[i];   // 정렬되지 않은 입력 → 50% 적중률
}
```

**✅ 정렬해서 예측 가능하게:**
```c
sort(data, data + n);                    // 한 번 정렬하면 분기 예측 ~100%
for (int i = 0; i < n; i++) {
    if (data[i] > 128) sum += data[i];
}
```
StackOverflow의 유명한 예제 — **정렬만 했는데 6배 빨라진다.**

### 2. Branchless 트릭

```c
// 분기 있음
if (x < 0) y = -x; else y = x;

// branchless (CMOV/conditional select 활용)
y = x < 0 ? -x : x;
y = (x ^ (x >> 31)) - (x >> 31);   // bit trick
```
컴파일러가 `cmov`로 변환할 수 있도록 단순한 패턴 권장. 단, **분기 예측이 잘 되는 분기는 branchless보다 빠름** (false branch 실행 안 함).

### 3. likely/unlikely 힌트

```c
if (__builtin_expect(error_flag, 0)) {  // 거의 안 일어남
    handle_error();
}
```
컴파일러가 cold 경로를 함수 끝으로 옮겨 hot path의 I-cache 효율을 높임. C++20 `[[likely]]`, `[[unlikely]]`.

### 4. 데이터 의존 체인 끊기

```c
// 의존성 긴 체인 → 1 cycle/iter
sum = 0;
for (i = 0; i < n; i++) sum += a[i];

// 4개 누적 변수로 분할 → ILP 4배
s0=s1=s2=s3 = 0;
for (i = 0; i < n; i+=4) {
    s0 += a[i]; s1 += a[i+1]; s2 += a[i+2]; s3 += a[i+3];
}
sum = s0+s1+s2+s3;
```

### 5. 측정

```bash
perf stat -e cycles,instructions,branches,branch-misses ./prog
```
→ `branch-miss-rate`가 5% 넘으면 분기 패턴 점검.

---

## 추가 학습 키워드

- Tomasulo's algorithm
- Scoreboarding (CDC 6600)
- Branch Target Buffer (BTB), Return Address Stack (RAS)
- Hazard detection unit, hazard table
- Static vs dynamic scheduling
- VLIW (Very Long Instruction Word) — Itanium 실패 사례
