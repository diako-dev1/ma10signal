"""
MA10 Signal — 주가 데이터 수집 스크립트 (미국 S&P500 + 한국 시총 500)
GitHub Actions에서 하루 6회 자동 실행
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from supabase import create_client
from datetime import datetime

# ── Supabase 연결 ──────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── 섹터 한국어 매핑 (미국) ────────────────────────────────
SECTOR_MAP_EN_TO_KO = {
    "Information Technology": "기술",
    "Technology": "기술",
    "Semiconductors": "반도체",
    "Semiconductor": "반도체",
    "Financials": "금융",
    "Financial Services": "금융",
    "Health Care": "헬스케어",
    "Healthcare": "헬스케어",
    "Consumer Discretionary": "소비재",
    "Consumer Staples": "소비재",
    "Energy": "에너지",
    "Industrials": "산업재",
    "Materials": "소재",
    "Real Estate": "리츠",
    "Utilities": "유틸리티",
    "Communication Services": "통신/미디어",
    "Biotechnology": "바이오",
    "Pharmaceuticals": "제약",
    "Automobiles": "자동차",
    "Electric Vehicles": "전기차",
    "E-Commerce": "이커머스",
    "Software": "소프트웨어",
    "Hardware": "하드웨어",
    "Banks": "은행",
    "Insurance": "보험",
}

def ko_sector(en: str) -> str:
    if not en:
        return "기타"
    for key, val in SECTOR_MAP_EN_TO_KO.items():
        if key.lower() in en.lower():
            return val
    return en

def format_market_cap(cap_usd: float, market: str) -> str:
    if not cap_usd:
        return "-"
    if market == "kr":
        krw = cap_usd * 1350
        jo = krw / 1e12
        return f"{jo:.0f}조" if jo >= 1 else f"{krw/1e8:.0f}억"
    if cap_usd >= 1e12:
        return f"${cap_usd/1e12:.1f}조"
    return f"${cap_usd/1e9:.0f}B"

def upsert_stock(row: dict):
    row["updated_at"] = datetime.utcnow().isoformat()
    supabase.table("stocks").upsert(row, on_conflict="ticker").execute()

# ══════════════════════════════════════════════════════════
# 미국 S&P 500 — Wikipedia에서 종목 리스트 자동 수집
# ══════════════════════════════════════════════════════════
def get_sp500_tickers() -> list[dict]:
    """Wikipedia S&P 500 테이블에서 종목 리스트 수집"""
    print("  📋 S&P 500 종목 리스트 수집 중...")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    result = []
    for _, row in df.iterrows():
        result.append({
            "ticker": str(row["Symbol"]).replace(".", "-"),  # BRK.B → BRK-B
            "name": str(row["Security"]),
            "sector": ko_sector(str(row.get("GICS Sector", ""))),
        })
    print(f"  ✅ {len(result)}개 종목 수집 완료")
    return result

def run_us():
    print("\n🇺🇸 미국 S&P 500 업데이트 시작")
    tickers_info = get_sp500_tickers()

    # Yahoo Finance 배치 다운로드 (한번에 여러 종목)
    tickers_list = [t["ticker"] for t in tickers_info]
    ticker_to_info = {t["ticker"]: t for t in tickers_info}

    print(f"  📥 Yahoo Finance에서 {len(tickers_list)}개 종목 월봉 다운로드 중...")

    # 25개씩 묶어서 배치 처리 (API 부하 분산)
    batch_size = 25
    success = 0
    failed = []

    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i:i+batch_size]
        batch_str = " ".join(batch)
        print(f"  배치 처리: {i+1}~{min(i+batch_size, len(tickers_list))} / {len(tickers_list)}")

        try:
            # 2년치 월봉 한번에 다운로드
            hist = yf.download(
                batch_str,
                period="2y",
                interval="1mo",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            for ticker in batch:
                try:
                    # 단일 종목인 경우 컬럼 구조가 다름
                    if len(batch) == 1:
                        close = hist["Close"].dropna()
                    else:
                        close = hist[ticker]["Close"].dropna()

                    if len(close) < 10:
                        failed.append(ticker)
                        continue

                    ma10 = float(close.iloc[-10:].mean())
                    price = float(close.iloc[-1])
                    gap_pct = round((price - ma10) / ma10 * 100, 2)

                    # 시가총액은 개별 호출 (배치에서 안 가져옴)
                    info = yf.Ticker(ticker).fast_info
                    market_cap = getattr(info, 'market_cap', 0) or 0

                    t_info = ticker_to_info[ticker]
                    row = {
                        "ticker": ticker,
                        "name": t_info["name"],
                        "market": "us",
                        "sector": t_info["sector"],
                        "price": round(price, 2),
                        "ma10": round(ma10, 2),
                        "gap_pct": gap_pct,
                        "market_cap_usd": market_cap,
                        "market_cap_label": format_market_cap(market_cap, "us"),
                    }
                    upsert_stock(row)
                    success += 1

                except Exception as e:
                    failed.append(ticker)

        except Exception as e:
            print(f"  ⚠ 배치 오류: {e}")
            failed.extend(batch)

        time.sleep(1)  # 배치 간 딜레이

    print(f"  ✅ 미국: {success}/{len(tickers_list)} 완료")
    if failed:
        print(f"  ⚠ 실패: {len(failed)}개 → {failed[:10]}")

# ══════════════════════════════════════════════════════════
# 한국 시총 상위 500 — KRX 공식 API
# ══════════════════════════════════════════════════════════

# KRX 섹터 → 한국어 매핑
KRX_SECTOR_MAP = {
    "전기전자": "반도체/전자",
    "화학": "화학",
    "운수장비": "자동차",
    "금융업": "금융",
    "의약품": "바이오",
    "서비스업": "서비스",
    "철강금속": "철강",
    "음식료품": "식품",
    "통신업": "통신",
    "건설업": "건설",
    "유통업": "유통",
    "기계": "기계",
    "섬유의복": "섬유",
    "종이목재": "소재",
    "비금속광물": "소재",
    "전기가스업": "에너지",
    "운수창고": "물류",
    "의료정밀": "헬스케어",
    "보험": "금융",
    "증권": "금융",
}

def get_krx_top500() -> list[dict]:
    """KRX API에서 KOSPI + KOSDAQ 시총 상위 500종목 수집"""
    print("  📋 KRX 종목 리스트 수집 중...")

    def fetch_market(market_id: str, market_name: str) -> pd.DataFrame:
        url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "mktId": market_id,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
        }
        headers = {
            "Referer": "https://data.krx.co.kr/",
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        resp = requests.post(url, data=payload, headers=headers, timeout=30)
        data = resp.json()
        df = pd.DataFrame(data["OutBlock_1"])
        df["market_name"] = market_name
        return df

    try:
        kospi = fetch_market("STK", "KOSPI")
        kosdaq = fetch_market("KSQ", "KOSDAQ")
        combined = pd.concat([kospi, kosdaq], ignore_index=True)

        # 시총 기준 정렬 (MKTCAP 컬럼)
        combined["MKTCAP"] = pd.to_numeric(combined["MKTCAP"], errors="coerce").fillna(0)
        combined = combined.sort_values("MKTCAP", ascending=False).head(500)

        result = []
        for _, row in combined.iterrows():
            ticker = str(row["ISU_SRT_CD"]).zfill(6)
            name = str(row["ISU_ABBRV"])
            sector_raw = str(row.get("IDX_IND_NM", "기타"))
            sector = KRX_SECTOR_MAP.get(sector_raw, sector_raw)
            market_cap = float(row["MKTCAP"]) * 1e6  # 백만원 → 원
            market_cap_usd = market_cap / 1350  # 원 → USD

            result.append({
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "market_cap_usd": market_cap_usd,
                "market_cap_label": format_market_cap(market_cap_usd, "kr"),
            })

        print(f"  ✅ {len(result)}개 종목 수집 완료")
        return result

    except Exception as e:
        print(f"  ⚠ KRX API 오류: {e}")
        print("  → 하드코딩 백업 리스트 사용")
        return get_kr_fallback()

def get_kr_fallback() -> list[dict]:
    """KRX API 실패 시 백업 리스트 (상위 50개)"""
    return [
        {"ticker":"005930","name":"삼성전자","sector":"반도체/전자","market_cap_usd":280e9,"market_cap_label":"378조"},
        {"ticker":"000660","name":"SK하이닉스","sector":"반도체/전자","market_cap_usd":110e9,"market_cap_label":"149조"},
        {"ticker":"005380","name":"현대차","sector":"자동차","market_cap_usd":33e9,"market_cap_label":"45조"},
        {"ticker":"373220","name":"LG에너지솔루션","sector":"2차전지","market_cap_usd":66e9,"market_cap_label":"90조"},
        {"ticker":"000270","name":"기아","sector":"자동차","market_cap_usd":29e9,"market_cap_label":"40조"},
        {"ticker":"207940","name":"삼성바이오로직스","sector":"바이오","market_cap_usd":46e9,"market_cap_label":"63조"},
        {"ticker":"105560","name":"KB금융","sector":"금융","market_cap_usd":28e9,"market_cap_label":"38조"},
        {"ticker":"005490","name":"POSCO홀딩스","sector":"철강","market_cap_usd":23e9,"market_cap_label":"32조"},
        {"ticker":"035420","name":"NAVER","sector":"IT플랫폼","market_cap_usd":22e9,"market_cap_label":"30조"},
        {"ticker":"055550","name":"신한지주","sector":"금융","market_cap_usd":20e9,"market_cap_label":"28조"},
    ]

def run_kr():
    print("\n🇰🇷 한국 시총 상위 500 업데이트 시작")
    stocks = get_krx_top500()
    success = 0
    failed = []

    # 20개씩 배치 처리
    batch_size = 20
    tickers_yf = [s["ticker"] + ".KS" for s in stocks]
    ticker_map = {s["ticker"] + ".KS": s for s in stocks}

    for i in range(0, len(tickers_yf), batch_size):
        batch = tickers_yf[i:i+batch_size]
        batch_str = " ".join(batch)
        print(f"  배치 처리: {i+1}~{min(i+batch_size, len(tickers_yf))} / {len(tickers_yf)}")

        try:
            hist = yf.download(
                batch_str,
                period="2y",
                interval="1mo",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            for yf_ticker in batch:
                try:
                    if len(batch) == 1:
                        close = hist["Close"].dropna()
                    else:
                        close = hist[yf_ticker]["Close"].dropna()

                    if len(close) < 10:
                        failed.append(yf_ticker)
                        continue

                    ma10 = float(close.iloc[-10:].mean())
                    price = float(close.iloc[-1])
                    gap_pct = round((price - ma10) / ma10 * 100, 2)

                    s_info = ticker_map[yf_ticker]
                    pure_ticker = yf_ticker.replace(".KS", "")

                    row = {
                        "ticker": pure_ticker,
                        "name": s_info["name"],
                        "market": "kr",
                        "sector": s_info["sector"],
                        "price": round(price, 0),
                        "ma10": round(ma10, 0),
                        "gap_pct": gap_pct,
                        "market_cap_usd": s_info["market_cap_usd"],
                        "market_cap_label": s_info["market_cap_label"],
                    }
                    upsert_stock(row)
                    success += 1

                except Exception as e:
                    failed.append(yf_ticker)

        except Exception as e:
            print(f"  ⚠ 배치 오류: {e}")
            failed.extend(batch)

        time.sleep(1)

    print(f"  ✅ 한국: {success}/{len(stocks)} 완료")
    if failed:
        print(f"  ⚠ 실패: {len(failed)}개")

# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 55)
    print("MA10 Signal 주가 업데이트 시작")
    print(f"실행 시각: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)
    run_us()
    run_kr()
    print("\n🎉 전체 업데이트 완료")
