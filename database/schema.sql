-- ============================================================
-- 주식 데이터 수집용 데이터베이스 스키마
-- 한국투자증권 Open API 기준 / PostgreSQL + TimescaleDB
-- ============================================================

CREATE DATABASE stock_data;
\c stock_data

CREATE EXTENSION IF NOT EXISTS timescaledb;


-- 1. 틱 단위: 웹소켓 실시간체결가 (H0STCNT0)
CREATE TABLE ws_trade (
    ts              BIGINT        NOT NULL,   -- 수신 시각 (epoch ms)
    symbol          VARCHAR(10)   NOT NULL,   -- 종목코드 (MKSC_SHRN_ISCD)
    cntg_hour       VARCHAR(10)   NOT NULL,   -- 체결 시간 (STCK_CNTG_HOUR, HHMMSS)
    price           INT           NOT NULL,   -- 체결가 (STCK_PRPR)
    volume          INT           NOT NULL,   -- 체결 수량 (CNTG_VOL)
    trade_dir       VARCHAR(5)    NOT NULL,   -- 체결구분 (CCLD_DVSN, 1:매수 5:매도)
    ask1            INT           NOT NULL,   -- 매도호가1 (ASKP1)
    bid1            INT           NOT NULL,   -- 매수호가1 (BIDP1)
    ask1_qty        INT           NOT NULL,   -- 매도호가 잔량1 (ASKP_RSQN1)
    bid1_qty        INT           NOT NULL,   -- 매수호가 잔량1 (BIDP_RSQN1)
    mkop_code       VARCHAR(10)   NOT NULL,   -- 장운영 구분 코드 (NEW_MKOP_CLS_CODE)
    hour_cls        VARCHAR(5)    NOT NULL,   -- 시간 구분 코드 (HOUR_CLS_CODE, 0:장중/A:장후/B:장전/C:VI)
    vi_std_price    INT           NOT NULL    -- 정적VI 발동기준가 (VI_STND_PRC)
);

-- TimescaleDB 하이퍼테이블 변환 (ts 기준 자동 파티셔닝, 1일 단위 청크)
SELECT create_hypertable('ws_trade', 'ts',
    chunk_time_interval => 86400000  -- 1일 = 86400000ms
);

CREATE INDEX idx_ws_trade_symbol_ts ON ws_trade (symbol, ts);


-- 2. 틱 단위: 웹소켓 실시간호가 (H0STASP0)
CREATE TABLE ws_orderbook (
    ts              BIGINT        NOT NULL,   -- 수신 시각 (epoch ms)
    symbol          VARCHAR(10)   NOT NULL,   -- 종목코드
    bsop_hour       VARCHAR(10)   NOT NULL,   -- 영업 시간 (BSOP_HOUR)
    hour_cls        VARCHAR(5)    NOT NULL,   -- 시간 구분 코드
    mkop_code       VARCHAR(10)   NOT NULL,   -- 장운영 구분 코드 (NEW_MKOP_CLS_CODE)
    -- 매도호가 가격/잔량 10단계 (배열: [1호가, 2호가, ..., 10호가])
    ask_prices      INT[10]       NOT NULL,   -- 매도호가 1~10 (ASKP1~ASKP10)
    bid_prices      INT[10]       NOT NULL,   -- 매수호가 1~10 (BIDP1~BIDP10)
    ask_qtys        INT[10]       NOT NULL,   -- 매도호가 잔량 1~10 (ASKP_RSQN1~ASKP_RSQN10)
    bid_qtys        INT[10]       NOT NULL    -- 매수호가 잔량 1~10 (BIDP_RSQN1~BIDP_RSQN10)
);

SELECT create_hypertable('ws_orderbook', 'ts',
    chunk_time_interval => 86400000
);

CREATE INDEX idx_ws_orderbook_symbol_ts ON ws_orderbook (symbol, ts);


-- 3. 30초~1분 폴링: REST 주식현재가 회원사 (FHKST01010600)
CREATE TABLE rest_member (
    ts              BIGINT        NOT NULL,   -- 폴링 시각 (epoch ms)
    symbol          VARCHAR(10)   NOT NULL,   -- 종목코드
    sell_qtys       INT[5],                   -- 매도 상위 1~5위 수량 (total_seln_qty1~5)
    buy_qtys        INT[5],                   -- 매수 상위 1~5위 수량 (total_shnu_qty1~5)
    glob_sell_qty   INT,                      -- 외국계 총 매도 수량 (glob_total_seln_qty)
    glob_buy_qty    INT,                      -- 외국계 총 매수 수량 (glob_total_shnu_qty)
    glob_net_qty    INT                       -- 외국계 순매수 수량 (glob_ntby_qty)
);

SELECT create_hypertable('rest_member', 'ts',
    chunk_time_interval => 86400000
);

CREATE INDEX idx_rest_member_symbol_ts ON rest_member (symbol, ts);


-- 4. 장 시작 전 1회: REST 주식현재가 시세 (FHKST01010100)
--    일별 데이터 — hypertable 불필요
CREATE TABLE rest_daily_base (
    trade_date      DATE          NOT NULL,   -- 거래일
    symbol          VARCHAR(10)   NOT NULL,   -- 종목코드
    base_price      INT           NOT NULL,   -- 기준가 (stck_sdpr)
    upper_limit     INT           NOT NULL,   -- 상한가 (stck_mxpr)
    lower_limit     INT           NOT NULL,   -- 하한가 (stck_llam)
    tick_unit       INT           NOT NULL,   -- 호가단위 (aspr_unit)
    listed_shares   BIGINT        NOT NULL,   -- 상장 주수 (lstn_stcn)
    status_code     VARCHAR(10)   NOT NULL,   -- 종목 상태 구분 코드 (iscd_stat_cls_code)
    PRIMARY KEY (trade_date, symbol)
);


-- 5. 장 마감 후 1회: REST 주식현재가 투자자 (FHKST01010900)
--    일별 데이터 — hypertable 불필요
CREATE TABLE rest_daily_investor (
    trade_date      DATE          NOT NULL,   -- 영업 일자 (stck_bsop_date)
    symbol          VARCHAR(10)   NOT NULL,   -- 종목코드
    prsn_net_qty    INT           NOT NULL,   -- 개인 순매수 수량 (prsn_ntby_qty)
    frgn_net_qty    INT           NOT NULL,   -- 외국인 순매수 수량 (frgn_ntby_qty)
    orgn_net_qty    INT           NOT NULL,   -- 기관계 순매수 수량 (orgn_ntby_qty)
    PRIMARY KEY (trade_date, symbol)
);
