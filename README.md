# 주식 실시간 데이터 수집 시스템

한국투자증권 Open API 기반 국내 주식 틱 데이터 수집기.
PostgreSQL + TimescaleDB에 저장.

## 프로젝트 목적

틱 단위 주식 데이터를 수집하여 딥러닝 기반 단타 매매 모델을 학습시키기 위한 원시 데이터 파이프라인.

### 모델 목표

단타 매매를 위한 초단기 가격 변동 예측 모델.

- **입력**: 틱 단위 체결/호가/수급 시퀀스
- **출력 헤드 3개 × 3호라이즌 = 9개**:
  - max_up(h): 향후 h초간 최대 상승률 (매수 기회)
  - max_down(h): 향후 h초간 최대 하락률 (리스크)
  - tradable_volume(h): 향후 h초간 체결 가능 수량 (실행 가능성)
- **분위 회귀**: 각 출력에 대해 50%(기대값), 90%, 99%(최악 시나리오) 분위를 동시 예측
- **매매 판단**: 50% 분위의 max_up이 충분히 크고, 99% 분위의 max_down이 허용 범위 내이며, tradable_volume이 주문 수량 이상일 때 진입
- **타겟 변환**: y_transformed = sign(y) × log(1 + |y| × k), k=100~500 (0 근처 극단 편향 완화)

### 라벨링 전략 (사후 계산)

원시 데이터만 빠짐없이 저장하면, 라벨은 사후에 윈도우 연산으로 생성 가능.

- **기준가**: 시점 t의 mid price = (매수1호가 + 매도1호가) / 2 → ws_trade.ask1, bid1에서 계산
- **max_up(h)**: t+5초 ~ t+(5+h)초 구간 내 체결가 최고값의 기준가 대비 상승률
- **max_down(h)**: 동일 구간 내 체결가 최저값의 기준가 대비 하락률
- **tradable_volume(h)**: 동일 구간 내 총 체결 수량
- **호라이즌 h**: 10초, 30초, 60초 (3구간 × 3라벨 = 9개 타겟)
- t+5초 오프셋: 모델 추론 + 주문 전송 지연(2~5초) 감안

### 저장 필드 선정 근거

- **저장하는 것**: 원시 데이터 (체결가, 체결량, 체결방향, 10단계 호가/잔량, 외국계 수급, 기준가/상하한가)
- **저장하지 않는 것**: 원시 데이터에서 사후 계산 가능한 파생값 (전일대비, 가중평균, 시고저, 누적거래량, 체결강도, 매수비율, 총잔량, 거래량회전율 등)
- **회원사 데이터**: 개별 회원사 번호/이름/외국계 여부는 저장하지 않음. 상위 5개 매매 규모와 외국계 합산 수급만 저장 (모델 피처로는 규모와 방향이 중요)
- **투자자 데이터**: 당일 장중에는 빈 값 → 장 마감 후 전일 확정 데이터를 일별 보조 피처로 저장

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
| hour_cls | VARCHAR | hour_cls_code [2] | 시간구분 (0:장중, A:장후예상, B:장전예상, C:VI, D:시간외단일가) |
| ask_prices | INT[10] | askp1~askp10 [3~12] | 매도호가 1~10단계 |
| bid_prices | INT[10] | bidp1~bidp10 [13~22] | 매수호가 1~10단계 |
| ask_qtys | INT[10] | askp_rsqn1~10 [23~32] | 매도잔량 1~10단계 |
| bid_qtys | INT[10] | bidp_rsqn1~10 [33~42] | 매수잔량 1~10단계 |

- 파이프 구분자 포맷: `encrypt|H0STASP0|count|field0^field1^...^field58` (공식 59개 필드, 실제 62개 수신 — 59~61은 미문서화)
- 공식 59개 필드 중 44개 저장 (10단계 호가/잔량을 배열로 압축)
- 호가에는 장운영구분코드(NEW_MKOP_CLS_CODE)가 없음 — hour_cls로 장 구간 구분

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
mkdir -p logs && nohup python run.py > logs/realtime.log 2>&1 &

# 로그 실시간 확인
tail -f logs/realtime.log

# 프로세스 확인
ps aux | grep "python run.py" | grep -v grep

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

## 멀티 계정 설정

여러 KIS 계정으로 종목을 나눠 수집할 수 있다.
`.env`에 `ACCOUNTS` 환경변수를 JSON 배열로 설정하면 멀티 모드로 동작.
설정하지 않으면 기존 `APP_KEY`/`APP_SECRET`/`SYMBOLS`로 싱글 모드 동작.

```env
# 싱글 모드 (기존 방식)
APP_KEY=your_key
APP_SECRET=your_secret
SYMBOLS=005930,000660,...

# 멀티 모드 (ACCOUNTS 설정 시 위 3개는 무시됨)
ACCOUNTS=[{"name":"main","app_key":"KEY1","app_secret":"SEC1","symbols":"005930,000660,..."},{"name":"sub","app_key":"KEY2","app_secret":"SEC2","symbols":"035720,011200,..."}]
```

- 계정당 WS 1연결, 최대 40종목 구독 가능
- DB, 텔레그램, 백업은 모든 계정이 공유
- 계정별 토큰은 `.tokens_{name}.json`으로 개별 관리

## 기술 스택

- Python 3.9 + asyncio
- PostgreSQL 17 + TimescaleDB
- asyncpg (DB), websockets (WS), aiohttp (REST)
- KIS Open API (한국투자증권)
