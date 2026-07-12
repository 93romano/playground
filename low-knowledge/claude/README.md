# 컴퓨터 아키텍쳐 로우지식 정리

CPU와 메모리가 실제로 어떻게 동작하는지를 이해하기 위한 학습 자료. 면접 단골 주제부터 마이크로아키텍쳐 심화, 그리고 실무에서 성능을 짜낼 때 알아야 할 로우레벨 지식을 한국어로 정리한다.

---

## 사용법

각 주제별 파일은 동일한 5단 구조로 되어 있어 원하는 깊이만 골라 읽을 수 있다.

1. **TL;DR** — 한 문단 요약
2. **기초 (면접/CS)** — 정의, 동작 원리, 자주 나오는 질문
3. **심화 (마이크로아키텍쳐)** — 내부 동작과 트레이드오프, 실제 칩 사례
4. **실무 성능 관점** — 측정 도구, 코드 패턴, 흔한 함정
5. **추가 학습 키워드** — 더 깊이 파고들 검색어

---

## 학습 로드맵

```
[기초 다지기]
01 수 표현 → 02 ISA & CPU 기본 → 03 파이프라인

[메모리 시스템]
05 메모리 계층 → 06 캐시 → 07 가상 메모리

[병렬성과 현대 CPU]
04 마이크로아키텍쳐 → 08 멀티코어 & 일관성 → 10 SIMD/벡터

[시스템 & 실전]
09 I/O & 인터럽트 → 11 성능 튜닝 → 12 보안/사이드채널
```

---

## 주제별 인덱스

| # | 주제 | 한 줄 요약 |
|---|------|-----------|
| [01](topics/01-number-representation.md) | 수 표현 | 2의 보수, IEEE 754 부동소수점, 엔디안 |
| [02](topics/02-isa-and-cpu-basics.md) | ISA & CPU 기본 | RISC vs CISC, 폰 노이만, 명령어 사이클, 레지스터 |
| [03](topics/03-pipeline.md) | 파이프라인 | 5단계 파이프라인, 해저드, 포워딩, 분기 예측 기초 |
| [04](topics/04-microarchitecture.md) | 마이크로아키텍쳐 | Superscalar, OoO 실행, 분기 예측, 투기 실행, ROB |
| [05](topics/05-memory-hierarchy.md) | 메모리 계층 | 레지스터→L1/L2/L3→DRAM→Disk, 지연/대역폭 |
| [06](topics/06-cache.md) | 캐시 | 매핑 방식, write 정책, 3C 미스, 캐시 라인 |
| [07](topics/07-virtual-memory.md) | 가상 메모리 | 페이지 테이블, MMU, TLB, 페이지 폴트, Huge Page |
| [08](topics/08-multicore-coherence.md) | 멀티코어 & 일관성 | SMP/NUMA, MESI, 메모리 모델, 배리어 |
| [09](topics/09-io-and-interrupts.md) | I/O & 인터럽트 | 인터럽트, DMA, MMIO, 컨텍스트 스위치 |
| [10](topics/10-simd-and-vector.md) | SIMD/벡터 | SSE/AVX/NEON, 자동 벡터화 |
| [11](topics/11-performance-tuning.md) | 성능 튜닝 | perf, CPI/IPC, false sharing, 데이터 지향 설계 |
| [12](topics/12-security-side-channel.md) | 보안/사이드채널 | Spectre, Meltdown, 캐시 사이드채널 |

---

## 파인만 학습 루틴 (권장)

각 문서는 TL;DR → 기초(면접/CS) → 심화(마이크로아키텍쳐) → 실무 성능 관점 → 추가 키워드의 5단이다. 아래 루틴으로 한 바퀴 돌면 그 원자를 "설명할 수 있는" 수준에 도달한다.

1. **TL;DR만 읽고 나머지를 덮은 채 스스로 설명해보기.** 말문이 막히는 지점이 곧 당신의 지식 구멍이다.
2. **도식을 백지에 다시 그리기.** 파이프라인 5단계, 캐시의 `TAG | INDEX | OFFSET`, 4단계 페이지 워크를 손으로. ASCII·mermaid의 화살표 하나하나가 "왜" 그 방향인지 소리 내어 설명한다.
3. **숫자를 손으로 굴려보기.** 2의 보수로 음수를 직접 만들고, 캐시 라인(64B)을 세고, 지연시간 사다리(L1 ~1ns · DRAM ~100ns · SSD ~100µs)로 "이 루프가 몇 ns 걸릴지"를 back-of-envelope으로 추정한다.
4. **❓ 면접 박스와 실무 코드를 자기 손으로 재현.** `perf stat`을 실제로 돌려 IPC·캐시 미스를 보거나, 예제를 컴파일해 `objdump`로 어셈블리를 확인한다. "왜 정렬만 했는데 6배 빨라지지?"를 직접 재현해본다.
5. **파인만 체크 3개를 10살에게 설명하듯 답하기.** 비유로도, 비유 없이 원리로도 막힘없이 되면 다음 문서로 넘어간다.

> 여기가 바닥이다. 위층(OS·네트워크·브라우저)이 막히면 대개 여기로 내려와 답을 찾지만, 여기서 막히면 더 내려갈 계층이 없다. 그러니 비유로 얼버무리며 우회하지 말고, 그 원자(비트·사이클·트랜지스터)가 더는 쪼개지지 않을 때까지 끝까지 쪼개라 — 마법 상자를 여기서만큼은 하나도 남기지 마라.

---

## 빠른 참조

### 메모리 계층별 지연시간 (현대 데스크탑/서버 기준)

| 계층 | 지연 | 대역폭 | 용량 |
|------|------|--------|------|
| 레지스터 | 0 cycle | — | 수십 개 |
| L1 캐시 | ~4 cycle (~1ns) | ~1 TB/s | 32~64 KB |
| L2 캐시 | ~12 cycle (~3ns) | ~500 GB/s | 256 KB~2 MB |
| L3 캐시 | ~40 cycle (~10ns) | ~250 GB/s | 수~수십 MB |
| DRAM | ~200 cycle (~80~100ns) | ~50 GB/s (DDR5) | GB ~ TB |
| NVMe SSD | ~10~100 µs | ~7 GB/s | TB |
| HDD | ~5~10 ms | ~200 MB/s | TB |
| 네트워크 (LAN) | ~0.5 ms | 1~100 Gb/s | — |
| 네트워크 (대륙간) | ~150 ms | — | — |

> 핵심 직관: **DRAM 한 번 = L1 50번 = ALU 연산 200번**. 캐시 미스가 왜 그렇게 비싼지의 출처.

### 자주 쓰는 상수

| 항목 | 값 |
|------|-----|
| 캐시 라인 크기 | 64 B (Intel/AMD/ARM 대부분) |
| 일반 페이지 크기 | 4 KB |
| Huge Page | 2 MB / 1 GB |
| x86-64 가상 주소 폭 | 48 bit (5단계 페이징 시 57 bit) |
| 일반적인 IPC (스칼라 워크로드) | 0.5 ~ 2 |
| 일반적인 IPC (잘 짠 SIMD/벡터) | 3 ~ 6 |

### 자주 보는 perf 카운터

```
cycles                  실제 사이클 수
instructions            실행한 명령어 수 (IPC = instructions/cycles)
cache-references        L1 외부로 나간 메모리 접근
cache-misses            LLC 미스 → DRAM 접근
branch-instructions     분기 명령
branch-misses           분기 예측 실패
dTLB-load-misses        TLB 미스 (페이지 워크 발생)
L1-dcache-load-misses   L1 데이터 캐시 미스
```

### 사용 예
```bash
perf stat -e cycles,instructions,cache-misses,branch-misses ./my_program
perf record -g ./my_program && perf report
```

---

## 작성 원칙 (이 문서의)

- 본문은 한국어, 핵심 용어는 영어 병기
- "왜 그렇게 설계되었는가"를 중심으로 서술
- 가능한 한 코드/숫자/벤치마크 예시 포함
- 다이어그램은 ASCII 아트로 표현
- ❓ 박스는 면접 단골 질문
