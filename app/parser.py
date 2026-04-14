from __future__ import annotations

"""KIS WebSocket 메시지 파서

KIS는 두 가지 포맷으로 메시지를 전송:
  1. JSON: 구독 확인, 에러 등 제어 메시지
  2. 파이프 구분자: 실시간 데이터 (encrypt|tr_id|count|data)
     data 내 필드는 ^ 로 구분
"""

import logging

logger = logging.getLogger(__name__)

# H0STCNT0 실시간 체결가 필드 순서 (KIS API 문서 기준)
TRADE_FIELDS = [
    "mksc_shrn_iscd",   # 0  유가증권 단축 종목코드
    "stck_cntg_hour",   # 1  주식 체결 시간 (HHMMSS)
    "stck_prpr",        # 2  주식 현재가
    "prdy_vrss_sign",   # 3  전일 대비 부호
    "prdy_vrss",        # 4  전일 대비
    "prdy_ctrt",        # 5  전일 대비율
    "wghn_avrg_stck_prc",  # 6  가중 평균 주식 가격
    "stck_oprc",        # 7  주식 시가
    "stck_hgpr",        # 8  주식 최고가
    "stck_lwpr",        # 9  주식 최저가
    "askp1",            # 10 매도호가1
    "bidp1",            # 11 매수호가1
    "cntg_vol",         # 12 체결 거래량
    "acml_vol",         # 13 누적 거래량
    "acml_tr_pbmn",     # 14 누적 거래 대금
    "seln_cntg_csnu",   # 15 매도 체결 건수
    "shnu_cntg_csnu",   # 16 매수 체결 건수
    "ntby_cntg_csnu",   # 17 순매수 체결 건수
    "cttr",             # 18 체결강도
    "seln_cntg_smtn",   # 19 총 매도 수량
    "shnu_cntg_smtn",   # 20 총 매수 수량
    "ccld_dvsn",        # 21 체결구분 (1:매수 5:매도)
    "shnu_rate",        # 22 매수비율
    "prdy_vol_vrss_acml_vol_rate",  # 23 전일 거래량 대비 등락율
    "oprc_hour",        # 24 시가 시간
    "oprc_vrss_prpr_sign",  # 25 시가대비 부호
    "oprc_vrss_prpr",   # 26 시가대비
    "hgpr_hour",        # 27 최고가 시간
    "hgpr_vrss_prpr_sign",  # 28 고가대비 부호
    "hgpr_vrss_prpr",   # 29 고가대비
    "lwpr_hour",        # 30 최저가 시간
    "lwpr_vrss_prpr_sign",  # 31 저가대비 부호
    "lwpr_vrss_prpr",   # 32 저가대비
    "bsop_date",        # 33 영업일자
    "new_mkop_cls_code",  # 34 장운영 구분 코드
    "trht_yn",          # 35 거래정지 여부
    "askp_rsqn1",       # 36 매도호가 잔량1
    "bidp_rsqn1",       # 37 매수호가 잔량1
    "total_askp_rsqn",  # 38 총 매도호가 잔량
    "total_bidp_rsqn",  # 39 총 매수호가 잔량
    "vol_tnrt",         # 40 거래량 회전율
    "prdy_smns_hour_acml_vol",  # 41 전일 동시간 누적 거래량
    "prdy_smns_hour_acml_vol_rate",  # 42 전일 동시간 누적 거래량 비율
    "hour_cls_code",    # 43 시간 구분 코드 (0:장중/A:장후/B:장전/C:VI)
    "mrkt_trtm_cls_code",  # 44 임의종료구분코드
    "vi_stnd_prc",      # 45 정적VI 발동기준가
]

# H0STASP0 실시간 호가 필드 순서 (KIS API 문서 기준)
ORDERBOOK_FIELDS = [
    "mksc_shrn_iscd",   # 0  유가증권 단축 종목코드
    "bsop_hour",        # 1  영업 시간
    "hour_cls_code",    # 2  시간 구분 코드
    "askp1",            # 3
    "askp2",            # 4
    "askp3",            # 5
    "askp4",            # 6
    "askp5",            # 7
    "askp6",            # 8
    "askp7",            # 9
    "askp8",            # 10
    "askp9",            # 11
    "askp10",           # 12
    "bidp1",            # 13
    "bidp2",            # 14
    "bidp3",            # 15
    "bidp4",            # 16
    "bidp5",            # 17
    "bidp6",            # 18
    "bidp7",            # 19
    "bidp8",            # 20
    "bidp9",            # 21
    "bidp10",           # 22
    "askp_rsqn1",       # 23
    "askp_rsqn2",       # 24
    "askp_rsqn3",       # 25
    "askp_rsqn4",       # 26
    "askp_rsqn5",       # 27
    "askp_rsqn6",       # 28
    "askp_rsqn7",       # 29
    "askp_rsqn8",       # 30
    "askp_rsqn9",       # 31
    "askp_rsqn10",      # 32
    "bidp_rsqn1",       # 33
    "bidp_rsqn2",       # 34
    "bidp_rsqn3",       # 35
    "bidp_rsqn4",       # 36
    "bidp_rsqn5",       # 37
    "bidp_rsqn6",       # 38
    "bidp_rsqn7",       # 39
    "bidp_rsqn8",       # 40
    "bidp_rsqn9",       # 41
    "bidp_rsqn10",      # 42
    "total_askp_rsqn",  # 43
    "total_bidp_rsqn",  # 44
    "ovtm_total_askp_rsqn",  # 45
    "ovtm_total_bidp_rsqn",  # 46
    "antc_cnpr",        # 47 예상 체결가 (0 = 장중)
    "antc_cnqn",        # 48 예상 체결량
    "antc_vol",         # 49 예상 거래량
    "antc_cntg_vrss",   # 50 예상 체결 대비
    "antc_cntg_vrss_sign",  # 51 예상 체결 대비 부호
    "antc_cntg_prdy_ctrt",  # 52 예상 체결 전일대비율
    "acml_vol",         # 53 누적 거래량
    "total_askp_rsqn_icdc",  # 54 총 매도호가 잔량 증감
    "total_bidp_rsqn_icdc",  # 55 총 매수호가 잔량 증감
    "ovtm_total_askp_icdc",  # 56 시간외 총 매도호가 잔량 증감
    "ovtm_total_bidp_icdc",  # 57 시간외 총 매수호가 잔량 증감
    "stck_deal_cls_code",    # 58 주식 매매 구분 코드 (사용X, 삭제된 값)
]


def parse_message(raw: str) -> dict | None:
    """KIS WebSocket 메시지를 파싱하여 dict 반환

    Returns:
        dict with '_type' key ('trade'|'orderbook'|'control') or None on error
    """
    # JSON 제어 메시지
    if raw.startswith("{"):
        import json
        try:
            data = json.loads(raw)
            tr_id = data.get("header", {}).get("tr_id", "")
            msg = data.get("body", {}).get("msg1", "")
            logger.debug("제어 메시지: tr_id=%s msg=%s", tr_id, msg)
            return {"_type": "control", "tr_id": tr_id, "data": data}
        except json.JSONDecodeError:
            logger.warning("JSON 파싱 실패: %s", raw[:100])
            return None

    # 파이프 구분자 실시간 데이터: encrypt|tr_id|count|payload
    parts = raw.split("|", 3)
    if len(parts) < 4:
        logger.warning("알 수 없는 메시지 포맷: %s", raw[:100])
        return None

    _encrypt, tr_id, _count, payload = parts
    fields = payload.split("^")

    if tr_id == "H0STCNT0":
        return _parse_trade(fields)
    elif tr_id == "H0STASP0":
        return _parse_orderbook(fields)
    else:
        logger.debug("미처리 tr_id: %s", tr_id)
        return None


def _safe_int(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _parse_trade(fields: list[str]) -> dict | None:
    if len(fields) < len(TRADE_FIELDS):
        logger.warning("체결 필드 수 부족: %d < %d", len(fields), len(TRADE_FIELDS))
        return None

    raw = {name: fields[i] for i, name in enumerate(TRADE_FIELDS)}

    # 진단용: 마감 단일가(15:20~15:30) raw 필드 기록
    #   trade_dir='' 케이스 원인 파악 — KIS가 보내는 실제 값 확인
    cntg_hour = raw.get("stck_cntg_hour", "")
    if "152000" <= cntg_hour <= "153059":
        logger.info(
            "CLOSE_AUCTION raw symbol=%s time=%s ccld_dvsn='%s' mkop='%s' hour_cls='%s' price=%s vol=%s ask1=%s bid1=%s",
            raw.get("mksc_shrn_iscd"), cntg_hour, raw.get("ccld_dvsn"),
            raw.get("new_mkop_cls_code"), raw.get("hour_cls_code"),
            raw.get("stck_prpr"), raw.get("cntg_vol"),
            raw.get("askp1"), raw.get("bidp1"),
        )

    # 진단용: 장전(08:30~09:00) trade_dir='3' 원인
    if raw.get("ccld_dvsn") not in ("1", "5"):
        logger.info(
            "NONSTD_DIR raw symbol=%s time=%s ccld_dvsn='%s' mkop='%s' hour_cls='%s'",
            raw.get("mksc_shrn_iscd"), cntg_hour, raw.get("ccld_dvsn"),
            raw.get("new_mkop_cls_code"), raw.get("hour_cls_code"),
        )

    try:
        return {
            "_type": "trade",
            "symbol": raw["mksc_shrn_iscd"],
            "cntg_hour": raw["stck_cntg_hour"],
            "price": _safe_int(raw["stck_prpr"]),
            "volume": _safe_int(raw["cntg_vol"]),
            "trade_dir": raw["ccld_dvsn"],
            "ask1": _safe_int(raw["askp1"]),
            "bid1": _safe_int(raw["bidp1"]),
            "ask1_qty": _safe_int(raw["askp_rsqn1"]),
            "bid1_qty": _safe_int(raw["bidp_rsqn1"]),
            "mkop_code": raw["new_mkop_cls_code"],
            "hour_cls": raw["hour_cls_code"],
            "vi_std_price": _safe_int(raw["vi_stnd_prc"]),
        }
    except Exception:
        logger.warning("체결 파싱 실패: %s", raw.get("mksc_shrn_iscd", "?"))
        return None


def _parse_orderbook(fields: list[str]) -> dict | None:
    if len(fields) < len(ORDERBOOK_FIELDS):
        logger.warning("호가 필드 수 부족: %d < %d", len(fields), len(ORDERBOOK_FIELDS))
        return None

    raw = {name: fields[i] for i, name in enumerate(ORDERBOOK_FIELDS)}

    try:
        return {
            "_type": "orderbook",
            "symbol": raw["mksc_shrn_iscd"],
            "bsop_hour": raw["bsop_hour"],
            "hour_cls": raw["hour_cls_code"],
            "ask_prices": [_safe_int(raw[f"askp{i}"]) for i in range(1, 11)],
            "bid_prices": [_safe_int(raw[f"bidp{i}"]) for i in range(1, 11)],
            "ask_qtys": [_safe_int(raw[f"askp_rsqn{i}"]) for i in range(1, 11)],
            "bid_qtys": [_safe_int(raw[f"bidp_rsqn{i}"]) for i in range(1, 11)],
        }
    except Exception:
        logger.warning("호가 파싱 실패: %s", raw.get("mksc_shrn_iscd", "?"))
        return None
