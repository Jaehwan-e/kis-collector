# scripts

수집 서버 운영 보조 스크립트 모음.

## pull_backup.py

수집 서버(클라우드)에서 로컬 DB로 데이터를 당겨오는 Pull 방식 백업 스크립트.
포트포워딩이나 `pg_hba.conf` 외부 IP 허용 없이, SSH 터널만으로 동작.

수집 서버의 텔레그램 봇 push 백업과 `.backup_state.json` 파일을 SCP로 동기화하여
이미 push 백업된 날짜는 건너뛰고 밀린 날짜만 선별적으로 당겨온다.

### 사용법

```bash
# 자동 모드 (밀린 날짜 전부 pull)
# 원격 .backup_state.json을 참조하여 last_success_date 이후 ~ 오늘까지 자동 처리
python scripts/pull_backup.py

# 강제 특정 날짜 (상태 파일 무시 / 변경 없음)
python scripts/pull_backup.py --date 2026-04-10

# 강제 범위 (상태 파일 무시 / 변경 없음)
python scripts/pull_backup.py --from 2026-04-07 --to 2026-04-10

# 다른 SSH 호스트 사용
python scripts/pull_backup.py --ssh ubuntu@1.2.3.4

# 원격 프로젝트 경로 변경
python scripts/pull_backup.py --remote-path /opt/kis-collector
```

### 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--ssh` | `Oracle_cloud` | SSH 호스트 (`~/.ssh/config` 별칭 사용 권장) |
| `--remote-path` | `/home/ubuntu/kis-collector` | 원격 프로젝트 경로 |
| `--date` | — | 특정 날짜 강제 pull (YYYY-MM-DD) |
| `--from` / `--to` | — | 날짜 범위 강제 pull |

### 동작 원리

```
[로컬 맥북]                                    [수집 서버]
  │                                               │
  │  scp ubuntu@host:.backup_state.json ./        │
  ├──────────────────────────────────────────────►│ 1) 상태 파일 동기화
  │                                               │
  │  ssh -L 15432:127.0.0.1:5432 ubuntu@host      │
  ├──────────────────────────────────────────────►│ 2) SSH 터널
  │                                               │
  │  asyncpg → 127.0.0.1:15432                    │
  │  서버 사이드 커서 5000건씩 스트리밍            │
  │◄─────────────────────────────────────────────│ 3) 데이터 pull
  │                                               │
  │  로컬 PG에 배치 INSERT                         │
  │                                               │
  │  scp .backup_state.json ubuntu@host:          │
  ├──────────────────────────────────────────────►│ 4) 상태 파일 업로드
  │                                               │
```

### 백업 대상 테이블

| 테이블 | 기준 컬럼 | 백업 범위 |
|--------|----------|----------|
| `ws_trade` | `ts` (epoch ms) | target_date 0시~24시 KST |
| `ws_orderbook` | `ts` | 동일 |
| `rest_member` | `ts` | 동일 |
| `rest_daily_base` | `trade_date` | target_date (당일) |
| `rest_daily_investor` | `trade_date` | target_date - 7일 ~ target_date<br>(전일 확정 데이터 특성 + 주말/연휴 보완) |

### 사전 설정

**1. `~/.ssh/config` 호스트 별칭**

```
Host Oracle_cloud
  HostName 158.179.168.237
  IdentityFile ~/.ssh/oc_ssh.key
  Port 22
  User ubuntu
```

**2. 수집 서버 PostgreSQL**

- `listen_addresses = 'localhost'` (외부 노출 불필요)
- `pg_hba.conf`: `host all all 127.0.0.1/32 md5`
- 방화벽: 5432 외부 차단해도 됨, **22(SSH)만 열려있으면 됨**

**3. 로컬 `config.json`**

`db_dsn` (로컬 DB 접속) + 원격 DB 인증정보가 동일해야 함.
스크립트가 `db_dsn`의 user/password/database를 그대로 쓰고 host/port만 SSH 터널로 교체.

### 로그

콘솔과 `logs/pull_backup.log` 양쪽에 동시 출력.

```
2026-04-11 10:00:00 INFO ============================================================
2026-04-11 10:00:00 INFO Pull 백업 시작 (host=Oracle_cloud, mode=auto)
2026-04-11 10:00:01 INFO 원격 상태 파일 동기화 완료
2026-04-11 10:00:01 INFO 원격 마지막 성공 날짜: 2026-04-09
2026-04-11 10:00:01 INFO 밀린 날짜 1일: 2026-04-10 ~ 2026-04-10
2026-04-11 10:00:01 INFO SSH 터널 생성: Oracle_cloud → localhost:15432
2026-04-11 10:00:02 INFO SSH 터널 준비 완료
2026-04-11 10:00:02 INFO 원격 DB 연결: 127.0.0.1:15432/stock_data
2026-04-11 10:00:02 INFO 로컬 DB 연결 완료
2026-04-11 10:00:02 INFO === 2026-04-10 당겨오는 중 ===
2026-04-11 10:00:45 INFO   ws_trade: 1234567건
2026-04-11 10:01:30 INFO   ws_orderbook: 3456789건
2026-04-11 10:01:31 INFO   rest_member: 56300건
2026-04-11 10:01:31 INFO   rest_daily_base: 80건
2026-04-11 10:01:32 INFO   rest_daily_investor: 540건 (2026-04-03~2026-04-10)
2026-04-11 10:01:32 INFO === 2026-04-10 완료: 4747276건 | 90초 ===
2026-04-11 10:01:32 INFO DB 연결 종료
2026-04-11 10:01:33 INFO SSH 터널 종료
2026-04-11 10:01:33 INFO 원격 상태 파일 업로드 완료
2026-04-11 10:01:33 INFO ============================================================
2026-04-11 10:01:33 INFO Pull 백업 종료: 1일 성공 (2026-04-10 ~ 2026-04-10) | 총 4747276건 | 93초
2026-04-11 10:01:33 INFO ============================================================
```
