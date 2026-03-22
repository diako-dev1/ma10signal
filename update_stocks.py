"""
MA10 Signal — 주가 데이터 수집 및 업데이트 스크립트
매일 6회 GitHub Actions에서 자동 실행됩니다.

필요 패키지: yfinance, pandas, requests, supabase-py
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from supabase import create_client

# ── 환경변수에서 Supabase 접속 정보 읽기 ──────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service_role 키 사용

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── 미국 시장 Top 30 종목 ─────────────────────────────────
US_STOCKS = [
    ("AAPL",  "Apple Inc.",         "기술"),
    ("MSFT",  "Microsoft Corp.",    "기술"),
    ("NVDA",  "NVIDIA Corp.",       "반도체"),
    ("GOOGL", "Alphabet Inc.",      "기술"),
    ("AMZN",  "Amazon.com",         "이커머스"),
    ("META",  "Meta Platforms",     "소셜미디어"),
    ("TSM",   "TSMC",               "반도체"),
    ("AVGO",  "Broadcom Inc.",      "반도체"),
    ("LLY",   "Eli Lilly",          "제약"),
    ("JPM",   "JPMorgan Chase",     "금융"),
    ("V",     "Visa Inc.",          "금융"),
    ("TSLA",  "Tesla Inc.",         "전기차"),
    ("MA",    "Mastercard Inc.",    "금융"),
    ("UNH",   "UnitedHealth",       "헬스케어"),
    ("XOM",   "ExxonMobil",         "에너지"),
    ("ORCL",  "Oracle Corp.",       "기술"),
    ("COST",  "Costco Wholesale",   "소비재"),
    ("ABBV",  "AbbVie Inc.",        "헬스케어"),
    ("BAC",   "Bank of America",    "금융"),
    ("MRK",   "Merck & Co.",        "헬스케어"),
    ("CRM",   "Salesforce",         "기술"),
    ("AMD",   "Adv. Micro Dev.",    "반도체"),
    ("CVX",   "Chevron Corp.",      "에너지"),
    ("ADBE",  "Adobe Inc.",         "기술"),
    ("WMT",   "Walmart Inc.",       "소비재"),
    ("HD",    "Home Depot",         "소비재"),
    ("QCOM",  "Qualcomm",           "반도체"),
    ("NOW",   "ServiceNow",         "기술"),
    ("GS",    "Goldman Sachs",      "금융"),
    ("TMO",   "Thermo Fisher",      "헬스케어"),
]

# ── 한국 시장 Top 30 종목 ─────────────────────────────────
KR_STOCKS = [
    ("005930.KS", "005930", "삼성전자",         "반도체"),
    ("000660.KS", "000660", "SK하이닉스",       "반도체"),
    ("005380.KS", "005380", "현대차",           "자동차"),
    ("373220.KS", "373220", "LG에너지솔루션",   "2차전지"),
    ("000270.KS", "000270", "기아",             "자동차"),
    ("207940.KS", "207940", "삼성바이오로직스", "바이오"),
    ("105560.KS", "105560", "KB금융",           "금융"),
    ("005490.KS", "005490", "POSCO홀딩스",      "철강"),
    ("035420.KS", "035420", "NAVER",            "IT플랫폼"),
    ("055550.KS", "055550", "신한지주",         "금융"),
    ("003670.KS", "003670", "포스코퓨처엠",     "2차전지"),
    ("068270.KS", "068270", "셀트리온",         "바이오"),
    ("086790.KS", "086790", "하나금융지주",     "금융"),
    ("051910.KS", "051910", "LG화학",           "화학"),
    ("006400.KS", "006400", "삼성SDI",          "2차전지"),
    ("035720.KS", "035720", "카카오",           "IT플랫폼"),
    ("128940.KS", "128940", "한미약품",         "바이오"),
    ("012330.KS", "012330", "현대모비스",       "자동차"),
    ("316140.KS", "316140", "우리금융지주",     "금융"),
    ("247540.KS", "247540", "에코프로비엠",     "2차전지"),
    ("086520.KS", "086520", "에코프로",         "2차전지"),
    ("058470.KS", "058470", "리노공업",         "반도체"),
    ("042700.KS", "042700", "한미반도체",       "반도체"),
    ("185750.KS", "185750", "종근당",           "바이오"),
    ("000100.KS", "000100", "유한양행",         "바이오"),
    ("096770.KS", "096770", "SK이노베이션",     "화학"),
    ("138930.KS", "138930", "BNK금융지주",      "금융"),
    ("011170.KS", "011170", "롯데케미칼",       "화학"),
    ("000720.KS", "000720", "현대건설",         "건설"),
    ("139480.KS", "139480", "이마트",           "유통"),
]

def format_market_cap(cap_usd: float, market: str) -> str:
    """시가총액을 표시용 문자열로 변환"""
    if market == "kr":
        krw = cap_usd * 1350  # 환율 근사값 (실서비스에서는 실시간 환율 사용 권장)
        jo = krw / 1e12
        if jo >= 1:
            return f"{jo:.0f}조"
        eok = krw / 1e8
        return f"{eok:.0f}억"
    else:
        if cap_usd >= 1e12:
            return f"${cap_usd/1e12:.1f}조"
        return f"${cap_usd/1e9:.0f}B"

def calc_ma10(ticker_yf: str) -> dict | None:
    """
    Yahoo Finance에서 월봉 데이터를 가져와 MA10 및 이격률 계산
    반환값: { price, ma10, gap_pct, market_cap_usd } 또는 None
    """
    try:
        stock = yf.Ticker(ticker_yf)

        # 월봉 최근 12개월치 (MA10 계산에는 최소 10개월 필요)
        hist = stock.history(period="2y", interval="1mo")
        if len(hist) < 10:
            print(f"  ⚠ {ticker_yf}: 데이터 부족 ({len(hist)}개월)")
            return None

        # MA10 계산 (종가 기준 10개월 이동평균)
        close_prices = hist["Close"].dropna()
        ma10 = float(close_prices.iloc[-10:].mean())
        price = float(close_prices.iloc[-1])
        gap_pct = round((price - ma10) / ma10 * 100, 2)

        # 시가총액
        info = stock.info
        market_cap = info.get("marketCap", 0) or 0

        return {
            "price": round(price, 2),
            "ma10": round(ma10, 2),
            "gap_pct": gap_pct,
            "market_cap_usd": market_cap,
        }
    except Exception as e:
        print(f"  ✗ {ticker_yf} 오류: {e}")
        return None

def upsert_stock(row: dict):
    """Supabase에 종목 데이터 upsert (있으면 업데이트, 없으면 삽입)"""
    supabase.table("stocks").upsert(
        row,
        on_conflict="ticker"
    ).execute()

def run_us():
    """미국 주식 업데이트"""
    print("\n🇺🇸 미국 주식 업데이트 시작")
    success = 0
    for ticker, name, sector in US_STOCKS:
        print(f"  처리 중: {ticker}")
        data = calc_ma10(ticker)
        if data is None:
            continue

        mcs = format_market_cap(data["market_cap_usd"], "us")
        row = {
            "ticker": ticker,
            "name": name,
            "market": "us",
            "sector": sector,
            "price": data["price"],
            "ma10": data["ma10"],
            "gap_pct": data["gap_pct"],
            "market_cap_usd": data["market_cap_usd"],
            "market_cap_label": mcs,
        }
        upsert_stock(row)
        success += 1
        time.sleep(0.5)  # API 호출 간격

    print(f"  ✅ 미국: {success}/{len(US_STOCKS)} 완료")

def run_kr():
    """한국 주식 업데이트"""
    print("\n🇰🇷 한국 주식 업데이트 시작")
    success = 0
    for yf_ticker, ticker, name, sector in KR_STOCKS:
        print(f"  처리 중: {ticker} ({name})")
        data = calc_ma10(yf_ticker)
        if data is None:
            continue

        # 원화로 변환 (Yahoo Finance는 KRW 종목을 원화로 반환)
        mcs = format_market_cap(data["market_cap_usd"], "kr")
        row = {
            "ticker": ticker,
            "name": name,
            "market": "kr",
            "sector": sector,
            "price": data["price"],
            "ma10": data["ma10"],
            "gap_pct": data["gap_pct"],
            "market_cap_usd": data["market_cap_usd"],
            "market_cap_label": mcs,
        }
        upsert_stock(row)
        success += 1
        time.sleep(0.5)

    print(f"  ✅ 한국: {success}/{len(KR_STOCKS)} 완료")

if __name__ == "__main__":
    print("=" * 50)
    print("MA10 Signal 주가 업데이트 시작")
    print("=" * 50)
    run_us()
    run_kr()
    print("\n🎉 전체 업데이트 완료")
