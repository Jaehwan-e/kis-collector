# 주식 실시간 데이터 수집 시스템

한국투자증권 Open API 기반 국내 주식 틱 데이터 수집기.
PostgreSQL + TimescaleDB에 저장.

## 수집 데이터 명세

### 1. ws_trade — 실시간 체결가 (WebSocket H0STCNT0)

틱 단위, 초당 수십 건. TimescaleDB 하이퍼테이블 (1일 청크).

| 컬럼 | 타입 | API 필드 | 설명 |
|------|------|----------|------|
| ts | BIGINT | (수신시각) | epoch ms |
| symbol | VARCHAR | mksc_shrn_iscd [0] | 종목코드 |
| cntg_hour | VARCHAR | stck_cntg_hour [1] | 체결시간 HHMMSS |
| price | INT | stck_prpr [2] | 체결가 |
| volume | INT | cntg_vol [12] | 체결수량 |
| trade_dir | VARCHAR | ccld_dvsn [21] | 체결구분 (1:매수, 5:매도) |
| ask1 | INT | askp1 [10] | 매도호가1 |
| bid1 | INT | bidp1 [11] | 매수호가1 |
| ask1_qty | INT | askp_rsqn1 [36] | 매도호가잔량1 |
| bid1_qty | INT | bidp_rsqn1 [37] | 매수호가잔량1 |
| mkop_code | VARCHAR | new_mkop_cls_code [34] | 장운영구분 (21:장중, 31:장마감동시호가 등) |
| hour_cls | VARCHAR | hour_cls_code [43] | 시간구분 (0:장중, A:장후, B:장전, C:VI) |
| vi_std_price | INT | vi_stnd_prc [45] | 정적VI 발동기준가 |

- 파이프 구분자 포맷: `encrypt|H0STCNT0|count|field0^field1^...^field45`
- 전체 46개 필드 중 13개 저장

### 2. ws_orderbook — 실시간 호가 (WebSocket H0STASP0)

틱 단위, 호가 변동 시마다. TimescaleDB 하이퍼테이블.

| 컬럼 | 타입 | API 필드 | 설명 |
|------|------|----------|------|
| ts | BIGINT | (수신시각) | epoch ms |
| symbol | VARCHAR | mksc_shrn_iscd [0] | 종목코드 |
| bsop_hour | VARCHAR | bsop_hour [1] | 영업시간 HHMMSS |
| hour_cls | VARCHAR | hour_cls_code [2] | 시간구분 |
| mkop_code | VARCHAR | new_mkop_cls_code [60] | 장운영구분 |
| ask_prices | INT[10] | askp1~askp10 [3~12] | 매도호가 1~10단계 |
| bid_prices | INT[10] | bidp1~bidp10 [13~22] | 매수호가 1~10단계 |
| ask_qtys | INT[10] | askp_rsqn1~10 [23~32] | 매도잔량 1~10단계 |
| bid_qtys | INT[10] | bidp_rsqn1~10 [33~42] | 매수잔량 1~10단계 |

- 파이프 구분자 포맷: `encrypt|H0STASP0|count|field0^field1^...^field61`
- 전체 62개 필드 중 44개 저장 (10단계 호가/잔량을 배열로 압축)
- 주의: mkop_code가 체결(H0STCNT0)과 다른 코드 체계 (체결: "20"=장중, 호가: "0"=장중)

### 3. rest_member — 회원사별 매매동향 (REST FHKST01010600)

30초 주기 폴링, 장중(09:00~15:30). TimescaleDB 하이퍼테이블.

| 컬럼 | 타입 | API 필드 | 설명 |
|------|------|----------|------|
| ts | BIGINT | (폴링시각) | epoch ms |
| symbol | VARCHAR | (요청 파라미터) | 종목코드 |
| sell_qtys | INT[5] | total_seln_qty1~5 | 매도 상위 1~5위 수량 |
| buy_qtys | INT[5] | total_shnu_qty1~5 | 매수 상위 1~5위 수량 |
| glob_sell_qty | INT | glob_total_seln_qty | 외국계 총 매도수량 |
| glob_buy_qty | INT | glob_total_shnu_qty | 외국계 총 매수수량 |
| glob_net_qty | INT | glob_ntby_qty | 외국계 순매수수량 |

- API endpoint: `/uapi/domestic-stock/v1/quotations/inquire-member`
- `output`은 단일 객체를 담은 리스트 (output[0]에서 추출)
- 회원사 번호/이름은 저장하지 않음 (딥러닝 피처로 수량만 사용)

### 4. rest_daily_base — 일별 기준시세 (REST FHKST01010100)

장 시작 전 1회. 일반 테이블.

| 컬럼 | 타입 | API 필드 | 설명 |
|------|------|----------|------|
| trade_date | DATE | (당일) | 거래일 |
| symbol | VARCHAR | (요청 파라미터) | 종목코드 |
| base_price | INT | stck_sdpr | 기준가 (전일종가) |
| upper_limit | INT | stck_mxpr | 상한가 |
| lower_limit | INT | stck_llam | 하한가 |
| tick_unit | INT | aspr_unit | 호가단위 |
| listed_shares | BIGINT | lstn_stcn | 상장주수 |
| status_code | VARCHAR | iscd_stat_cls_code | 종목상태 |

- API endpoint: `/uapi/domestic-stock/v1/quotations/inquire-price`
- `output`은 단일 dict
- ON CONFLICT DO NOTHING (중복 방지)

### 5. rest_daily_investor — 일별 투자자별 매매동향 (REST FHKST01010900)

장 마감 후 1회. 일반 테이블.

| 컬럼 | 타입 | API 필드 | 설명 |
|------|------|----------|------|
| trade_date | DATE | stck_bsop_date | 영업일자 |
| symbol | VARCHAR | (요청 파라미터) | 종목코드 |
| prsn_net_qty | INT | prsn_ntby_qty | 개인 순매수수량 |
| frgn_net_qty | INT | frgn_ntby_qty | 외국인 순매수수량 |
| orgn_net_qty | INT | orgn_ntby_qty | 기관계 순매수수량 |

- API endpoint: `/uapi/domestic-stock/v1/quotations/inquire-investor`
- `output`은 일별 리스트 (최근 30일)
- 당일 데이터는 빈 문자열 → 값이 있는 첫 항목(전일 확정 데이터) 사용
- ON CONFLICT DO NOTHING

### 6. 휴장일 조회 (REST CTCA0903R)

매일 08:00 토큰 발급 후 1회 호출. DB 저장 없음.

- API endpoint: `/uapi/domestic-stock/v1/quotations/chk-holiday`
- `output[0].opnd_yn == "Y"` → 개장일
- 1일 1회 호출 권장 (KIS 원장서비스 연동)

## 수집 스케줄

```
08:00  토큰 발급 → 휴장일 조회 → daily_base 수집
08:30  WebSocket 연결 (체결+호가) + REST 회원사 30초 폴링
15:30  WebSocket 종료 + 회원사 폴링 종료
15:35  daily_investor 수집
       → 다음 영업일 08:00까지 대기
```

## 수집 대상 종목 (20종목)

코스피 시가총액 상위, 주당 10만원 미만 기준.
.env의 SYMBOLS에서 관리.

| 코드 | 종목 |
|------|------|
| 034020 | 두산에너빌리티 |
| 055550 | 신한지주 |
| 006800 | 미래에셋증권 |
| 015760 | 한국전력 |
| 316140 | 우리금융지주 |
| 010140 | 삼성중공업 |
| 035720 | 카카오 |
| 011200 | HMM |
| 024110 | 기업은행 |
| 017670 | SK텔레콤 |
| 030200 | KT |
| 003550 | LG |
| 047050 | 포스코인터내셔널 |
| 323410 | 카카오뱅크 |
| 005940 | NH투자증권 |
| 003490 | 대한항공 |
| 016360 | 삼성증권 |
| 000100 | 유한양행 |
| 326030 | SK바이오팜 |
| 007660 | 이수페타시스 |

## 실행 방법

### 가상환경 활성화

```bash
source .venv/bin/activate
```

### 디버깅 (포그라운드, DEBUG 로그)

```bash
LOG_LEVEL=DEBUG python run.py
```

### 실제 운영 (포그라운드)

```bash
python run.py
```

### 백그라운드 운영 (로그 파일 저장)

```bash
nohup python run.py > logs/realtime.log 2>&1 &

# 로그 실시간 확인
tail -f logs/realtime.log

# 프로세스 확인
ps aux | grep "python run.py"

# 종료 (SIGTERM → graceful shutdown)
kill $(pgrep -f "python run.py")
```

### 테스트

```bash
python -m tests.test all          # 전체
python -m tests.test parser       # 파서 (오프라인)
python -m tests.test db           # DB 연결 + INSERT
python -m tests.test auth         # 토큰 발급 + 캐싱
python -m tests.test holiday      # 휴장일 조회
python -m tests.test daily_base   # 일별 시세 조회
python -m tests.test ws           # WebSocket 10초 (장중에만 유효)
```

### DB 확인

```bash
psql stock_data -c "SELECT COUNT(*) FROM ws_trade;"
psql stock_data -c "SELECT COUNT(*) FROM ws_orderbook;"
psql stock_data -c "SELECT COUNT(*) FROM rest_member;"
psql stock_data -c "SELECT COUNT(*) FROM rest_daily_base;"
psql stock_data -c "SELECT COUNT(*) FROM rest_daily_investor;"
```

### PostgreSQL 서비스 관리

```bash
brew services start postgresql@17
brew services stop postgresql@17
brew services restart postgresql@17
```

## 기술 스택

- Python 3.9 + asyncio
- PostgreSQL 17 + TimescaleDB
- asyncpg (DB), websockets (WS), aiohttp (REST)
- KIS Open API (한국투자증권)
