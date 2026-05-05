# Topic 1: PostgreSQL Architecture (PostgreSQL 아키텍처)

## 1. Process Architecture (프로세스 아키텍처)

PostgreSQL uses a **client/server, multi-process** model (not multi-threaded).
PostgreSQL은 **클라이언트/서버, 멀티 프로세스** 모델을 사용합니다 (멀티 스레드가 아님).

```
Client (psql, app, etc.)
    |
    v
+---------------------------------------------+
|         Postmaster (main process)            |
|       (listens for new connections)          |
+----------------------+-----------------------+
                       | fork()
                       v
+-----------------------------+
|     Backend Process         |  <-- one per client connection
|   (handles SQL queries)     |      (클라이언트 접속마다 1개 생성)
+-----------------------------+
```

### Background Processes (백그라운드 프로세스)

| Process                      | Role (역할)                                                                 |
|------------------------------|-----------------------------------------------------------------------------|
| **Postmaster**               | Main process. Listens for connections and forks backend processes. / 메인 프로세스. 연결을 수신하고 백엔드 프로세스를 fork. |
| **Background Writer**        | Flushes dirty pages from shared buffers to disk. / shared buffer의 dirty page를 디스크에 기록. |
| **WAL Writer**               | Flushes WAL buffers to disk. / WAL 버퍼를 디스크에 기록. |
| **Checkpointer**             | Periodically syncs all data to disk. / 주기적으로 모든 데이터를 디스크에 동기화. |
| **Autovacuum Launcher**      | Cleans up dead tuples (related to MVCC). / 죽은 튜플 정리 (MVCC와 관련). |
| **Logical Replication Launcher** | Manages logical replication. / 논리적 복제 관리. |

### Key Point (핵심)

- Each client connection gets its own backend process via `fork()`.
- 클라이언트가 접속할 때마다 Postmaster가 `fork()`으로 새로운 백엔드 프로세스를 생성합니다.
- The architecture (Postmaster + Background Workers) is **one per cluster**. Only backend processes multiply.
- 아키텍처(Postmaster + Background Workers)는 **클러스터당 하나**. 백엔드 프로세스만 늘어납니다.

### Verify with command (확인 명령어)

```bash
ps aux | grep postgres
```

---

## 2. Memory Architecture (메모리 아키텍처)

```
+-------------- Shared Memory ----------------+
|                                              |
|  +------------------+  +------------------+  |
|  |  Shared Buffers  |  |   WAL Buffers    |  |
|  |  (data cache)    |  |   (log buffer)   |  |
|  +------------------+  +------------------+  |
|  +------------------+                        |
|  |  CLOG Buffers    |                        |
|  | (txn status)     |                        |
|  +------------------+                        |
+----------------------------------------------+

+------------- Per-Backend Memory -------------+
|  work_mem             -- sorting, hash ops    |
|  maintenance_work_mem -- VACUUM, CREATE INDEX |
|  temp_buffers         -- temporary tables     |
+----------------------------------------------+
```

| Memory Area             | Scope (범위)      | Purpose (용도)                                  |
|-------------------------|-------------------|-------------------------------------------------|
| **Shared Buffers**      | All processes (공유) | Caches data pages read from disk. / 디스크에서 읽은 데이터 페이지 캐시. |
| **WAL Buffers**         | All processes (공유) | Temporary storage for transaction logs before writing to disk. / 트랜잭션 로그를 디스크에 쓰기 전 임시 저장. |
| **work_mem**            | Per backend (개별)  | Used for sort and hash operations per query. / 쿼리당 정렬 및 해시 작업에 사용. |
| **maintenance_work_mem**| Per backend (개별)  | Used for VACUUM, CREATE INDEX. / VACUUM, CREATE INDEX에 사용. |

### Verify with command (확인 명령어)

```sql
SHOW shared_buffers;
SHOW work_mem;
SHOW wal_buffers;
```

---

## 3. Storage Architecture (스토리지 아키텍처)

```
$PGDATA (data directory)
+-- base/            <-- per-database directories (tables, indexes)
+-- global/          <-- cluster-wide shared tables (pg_database, etc.)
+-- pg_wal/          <-- WAL files (transaction logs)
+-- pg_xact/         <-- transaction commit status (CLOG)
+-- postgresql.conf  <-- configuration file
+-- pg_hba.conf      <-- authentication settings
+-- pg_ident.conf    <-- user mapping
```

### Verify data directory (데이터 디렉토리 확인)

```sql
SHOW data_directory;
```

---

## 4. Write Path (데이터 쓰기 흐름)

```
Client: INSERT INTO ...
    |
    v
Backend Process
    |
    +-->  WAL Buffer --> WAL Writer --> pg_wal/ (disk)
    |
    +-->  Shared Buffers (dirty page)
              |
              +--> Background Writer / Checkpointer --> base/ (disk)
```

### WAL (Write-Ahead Logging)

1. Changes are **written to WAL first** (sequential write, fast).
   변경 사항이 **먼저 WAL에 기록** (순차 쓰기, 빠름).
2. Actual data files are written **later** by Background Writer / Checkpointer.
   실제 데이터 파일은 Background Writer / Checkpointer가 **나중에** 기록.
3. On crash, WAL is replayed to recover data -> **durability guaranteed**.
   장애 발생 시 WAL을 재생(replay)해서 데이터 복구 -> **내구성 보장**.
