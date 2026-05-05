# 06. 캐시 (Cache)

## TL;DR

캐시는 자주 쓰는 메모리 데이터를 CPU 가까이에 복사해 두는 작은 SRAM 저장소다. **캐시 라인(64B)** 단위로 동작하고, 어느 라인을 어디 둘지는 **매핑 방식**(Direct/Set-associative/Fully)으로, 어느 라인을 쫓아낼지는 **교체 정책**(LRU 근사)으로, 쓰기 처리는 **write-back/write-through**로 결정한다. 캐시 미스의 90%는 (1) 라인 크기를 무시한 데이터 레이아웃, (2) 큰 working set, (3) 잘못된 액세스 패턴으로 발생.

---

## 기초 (면접/CS)

### 매핑 방식

64KB 캐시, 64B 라인, 따라서 1024 라인이라고 가정. 주소 X가 어느 라인에 들어가는가?

#### Direct-mapped
주소 X → 라인 (X / 64) mod 1024. 단 한 곳.
- 장점: 단순, 빠른 lookup
- 단점: 같은 슬롯을 두 핫 데이터가 다투면 thrashing

#### Fully associative
어디든 들어갈 수 있음. CAM으로 모든 슬롯 비교.
- 장점: conflict miss 없음
- 단점: 비교기 1024개 필요 → 비싸고 전력↑ → 작은 캐시(TLB 등)에만 사용

#### N-way Set associative (실전 표준)
캐시를 set 단위로 묶고, 한 set 안에서는 fully associative.
- 8-way: 1024 / 8 = 128 set. 각 주소는 정해진 1개 set의 8칸 중 어디든.
- 현대 L1: 8-way, L2: 8~16-way, L3: 16-way 정도

```
주소 비트:
[ TAG | INDEX | OFFSET ]
       (set 결정) (라인 내 위치)
```

### 교체 정책

set 안에서 어느 라인을 쫓아낼까:
- **LRU (Least Recently Used)**: 이상적이지만 8-way 이상은 비용 큼
- **Pseudo-LRU**: tree-based 근사, 실전 표준
- **RRIP (Re-Reference Interval Prediction)**: 최근 Intel/AMD에서 사용, scan 패턴에 강함

### Write 정책

#### Write-through vs Write-back
- **Write-through**: 캐시와 다음 계층 모두에 즉시 쓰기. 단순, but 메모리 트래픽 큼.
- **Write-back**: 캐시에만 쓰고 dirty 표시 → 쫓겨날 때만 메모리 쓰기. 현대 표준.

#### Write-allocate vs No-write-allocate
write miss 시:
- **Write-allocate**: 라인을 캐시에 가져온 후 쓰기 (다음 read에 유리)
- **No-write-allocate**: 캐시 거치지 않고 바로 메모리에 쓰기 (write-through와 자주 짝)

현대 CPU는 보통 **Write-back + Write-allocate**.

### 3C 캐시 미스 (Mark Hill)

| 종류 | 원인 | 해결 |
|------|------|------|
| **Compulsory (cold)** | 처음 접근 → 무조건 미스 | prefetch |
| **Capacity** | working set > 캐시 | blocking, 데이터 줄이기 |
| **Conflict** | 같은 set 충돌 (set assoc 한계) | padding, associativity 증가 |

(4번째 **Coherence miss**: 멀티코어에서 다른 코어가 invalidate해 발생 — 8장)

❓ **면접: "캐시 라인이 64B인 이유?"** 공간 지역성 활용 + DRAM burst 길이 + 메타데이터(tag) 오버헤드 균형. 너무 작으면 tag 비율 ↑, 너무 크면 전송 낭비.

---

## 심화 (마이크로아키텍쳐)

### Inclusive vs Exclusive vs NINE

- **Inclusive**: 상위 캐시(L2)는 하위(L1)의 모든 라인을 포함. 코히런스 단순.
- **Exclusive**: L1과 L2가 라인을 공유하지 않음. 효율적 용량.
- **NINE (Non-Inclusive Non-Exclusive)**: 둘 다 아님. 현대 Intel은 mostly NINE.

### Virtually Indexed, Physically Tagged (VIPT)

L1은 보통 **VIPT**: index는 가상 주소로 (TLB와 병렬), tag는 물리 주소로 (alias 방지).
→ 캐시 크기에 제약: `set 수 × line size ≤ page size` (4KB) 안 그러면 alias 가능.
→ Apple은 16KB 페이지로 이 제약을 풀어 더 큰 L1 가능.

### Cache Coherence (간단)

여러 코어가 같은 라인을 캐싱하면 동기화 필요. **MESI** 프로토콜:
- **Modified**: 나만 가지고 있고 dirty
- **Exclusive**: 나만 가지고 있고 clean
- **Shared**: 여러 코어가 clean 카피
- **Invalid**: 무효

(상세는 8장)

### Cache Hit/Miss Latency

| 캐시 | Hit latency | Miss penalty (다음 계층 가야 함) |
|------|-------------|----------------------------|
| L1 | 4~5 cycle | +10 cycle (L2까지) |
| L2 | 12~14 cycle | +30 cycle (L3까지) |
| L3 | 40 cycle | +200 cycle (DRAM) |

### MSHR (Miss Status Handling Register)

outstanding cache miss를 추적하는 슬롯. 일반적으로 코어당 ~10. **미스가 많아도 MSHR이 차면 stall**. → MLP의 상한.

---

## 실무 성능 관점

### 1. False Sharing — 가장 잘 빠지는 함정

```c
struct {
    int counter_a;   // 코어 A가 자주 update
    int counter_b;   // 코어 B가 자주 update
} shared;
```
둘이 같은 캐시 라인(64B) 안 → 매 update마다 코어끼리 ping-pong → **수십 배 느려짐**.

```c
struct alignas(64) Counter { int value; char pad[60]; };
Counter a, b;   // 라인 분리
```
또는 `alignas(std::hardware_destructive_interference_size)`.

### 2. Cache Blocking (Tiling)

```c
// 매트릭스 곱 — naive: B 매트릭스가 캐시 안 맞음
for (i=0; i<N; i++)
    for (j=0; j<N; j++)
        for (k=0; k<N; k++)
            C[i][j] += A[i][k] * B[k][j];

// blocked — block_size를 L1/L2에 맞춤
for (ii=0; ii<N; ii+=B)
  for (jj=0; jj<N; jj+=B)
    for (kk=0; kk<N; kk+=B)
      for (i=ii; i<ii+B; i++)
        for (j=jj; j<jj+B; j++)
          for (k=kk; k<kk+B; k++)
            C[i][j] += A[i][k] * B[k][j];
```
→ block size = 64~128 정도면 BLAS급 속도. 실전에서는 BLAS(MKL, OpenBLAS, Apple Accelerate)를 쓸 것.

### 3. Loop Interchange

```c
// row-major C에서 column-first 접근 → 64B 라인 중 8 byte만 쓰고 버림
for (j=0; j<N; j++)
    for (i=0; i<N; i++)
        sum += a[i][j];

// row-first로 바꾸면 캐시 라인 fully 활용 → 8배 빠름
for (i=0; i<N; i++)
    for (j=0; j<N; j++)
        sum += a[i][j];
```

### 4. Conflict Miss 회피 (padding)

```c
double a[1024][1024];   // row size = 8KB = power of 2 → 같은 set 매핑 충돌 빈발
double a[1024][1025];   // row size를 살짝 깨뜨림 → conflict 분산
```
2의 거듭제곱 stride는 set associative cache에서 thrashing을 유발. 이래서 **stride가 page size의 배수일 때 매우 느려질 수 있음**.

### 5. Streaming 데이터: Non-temporal store

큰 배열을 한 번 쓰고 다시 안 읽을 때 (예: memset, 비디오 인코딩 출력):
- `_mm_stream_ps` 등 **non-temporal store** → 캐시 우회, write-combining buffer로 직접 메모리에
- 캐시 오염 방지 → 다른 핫 데이터 보호

### 6. 측정

```bash
perf stat -e L1-dcache-load-misses,LLC-load-misses,LLC-loads ./prog
# LLC-load-miss / LLC-loads 비율로 DRAM 의존도 추정

perf c2c record ./prog       # cache-to-cache 트래픽 분석 → false sharing 잡기
perf c2c report
```

### 7. 라인 크기를 코드에서 가정

```cpp
constexpr size_t CACHE_LINE = 64;
struct alignas(CACHE_LINE) PaddedCounter { ... };
```
C++17 `std::hardware_destructive_interference_size` / `_constructive_`도 활용 가능.

---

## 추가 학습 키워드

- Way prediction, partitioned cache
- Cache coloring (OS 측 페이지 할당으로 conflict 분산)
- Skewed associative cache
- Replacement policies: LRU, FIFO, Random, RRIP, SHIP, DRRIP
- Cache compression (CompressionRatio 기반 capacity 확장)
- Cache partitioning (Intel CAT — class of service)
- Streaming buffer, victim cache
