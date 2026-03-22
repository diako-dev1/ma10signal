"""
MA10 Signal — 주가 데이터 수집 스크립트
미국 S&P 500 + 한국 시총 상위 500
GitHub Actions에서 하루 3회 자동 실행 (KST 04:00, 10:00, 16:00)
"""

import os, time, requests
import pandas as pd
import yfinance as yf
from supabase import create_client
from datetime import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── 섹터 한국어 매핑 ───────────────────────────────────────
SECTOR_MAP = {
    "Information Technology":"기술","Technology":"기술",
    "Semiconductors":"반도체","Semiconductor":"반도체",
    "Financials":"금융","Financial Services":"금융",
    "Health Care":"헬스케어","Healthcare":"헬스케어",
    "Consumer Discretionary":"소비재","Consumer Staples":"소비재",
    "Energy":"에너지","Industrials":"산업재","Materials":"소재",
    "Real Estate":"리츠","Utilities":"유틸리티",
    "Communication Services":"통신/미디어","Biotechnology":"바이오",
    "Pharmaceuticals":"제약","Automobiles":"자동차",
    "Software":"소프트웨어","Banks":"은행","Insurance":"보험",
}
def ko_sector(en):
    if not en: return "기타"
    for k,v in SECTOR_MAP.items():
        if k.lower() in en.lower(): return v
    return en

def fmt_cap(usd, market):
    if not usd: return "-"
    if market == "kr":
        krw = usd * 1350
        jo = krw / 1e12
        return f"{jo:.0f}조" if jo >= 1 else f"{krw/1e8:.0f}억"
    if usd >= 1e12: return f"${usd/1e12:.1f}조"
    return f"${usd/1e9:.0f}B"

def upsert(row):
    row["updated_at"] = datetime.utcnow().isoformat()
    supabase.table("stocks").upsert(row, on_conflict="ticker").execute()

# ── S&P 500 종목 리스트 (내장) ────────────────────────────
SP500 = [
    ("MMM","3M Co.","산업재"),("AOS","A.O. Smith","산업재"),("ABT","Abbott Labs","헬스케어"),
    ("ABBV","AbbVie","헬스케어"),("ACN","Accenture","기술"),("ADBE","Adobe","기술"),
    ("AMD","Adv. Micro Dev.","반도체"),("AES","AES Corp","유틸리티"),("AFL","Aflac","보험"),
    ("A","Agilent Tech","헬스케어"),("APD","Air Products","소재"),("AKAM","Akamai","기술"),
    ("ALK","Alaska Air","산업재"),("ALB","Albemarle","소재"),("ARE","Alexandria RE","리츠"),
    ("ALGN","Align Technology","헬스케어"),("ALLE","Allegion","산업재"),("LNT","Alliant Energy","유틸리티"),
    ("ALL","Allstate","보험"),("GOOGL","Alphabet A","기술"),("GOOG","Alphabet C","기술"),
    ("MO","Altria","소비재"),("AMZN","Amazon","이커머스"),("AMCR","Amcor","소재"),
    ("AEE","Ameren","유틸리티"),("AAL","American Airlines","산업재"),("AEP","American Electric","유틸리티"),
    ("AXP","American Express","금융"),("AIG","American Intl","보험"),("AMT","American Tower","리츠"),
    ("AWK","American Water","유틸리티"),("AMP","Ameriprise","금융"),("AME","AMETEK","산업재"),
    ("AMGN","Amgen","바이오"),("APH","Amphenol","기술"),("ADI","Analog Devices","반도체"),
    ("ANSS","ANSYS","기술"),("AON","Aon","보험"),("APA","APA Corp","에너지"),
    ("AAPL","Apple","기술"),("AMAT","Applied Materials","반도체"),("APTV","Aptiv","자동차"),
    ("ACGL","Arch Capital","보험"),("ADM","Archer Daniels","소비재"),("ANET","Arista Networks","기술"),
    ("AJG","Arthur Gallagher","보험"),("AIZ","Assurant","보험"),("T","AT&T","통신/미디어"),
    ("ATO","Atmos Energy","유틸리티"),("ADSK","Autodesk","기술"),("ADP","Auto Data Proc","기술"),
    ("AZO","AutoZone","소비재"),("AVB","AvalonBay","리츠"),("AVY","Avery Dennison","소재"),
    ("AXON","Axon Enterprise","산업재"),("BKR","Baker Hughes","에너지"),("BALL","Ball Corp","소재"),
    ("BAC","Bank of America","금융"),("BK","BNY Mellon","금융"),("BBWI","Bath & Body","소비재"),
    ("BAX","Baxter Intl","헬스케어"),("BDX","Becton Dickinson","헬스케어"),("BRK-B","Berkshire B","금융"),
    ("BBY","Best Buy","소비재"),("TECH","Bio-Techne","헬스케어"),("BIIB","Biogen","바이오"),
    ("BLK","BlackRock","금융"),("BX","Blackstone","금융"),("BA","Boeing","산업재"),
    ("BCH","Brookfield Asset","금융"),("BKNG","Booking Holdings","소비재"),("BWA","BorgWarner","자동차"),
    ("BSX","Boston Scientific","헬스케어"),("BMY","Bristol Myers","제약"),("AVGO","Broadcom","반도체"),
    ("BR","Broadridge","기술"),("BRO","Brown & Brown","보험"),("BF-B","Brown-Forman","소비재"),
    ("BLDR","Builders FirstSource","산업재"),("BG","Bunge Global","소비재"),("CDNS","Cadence Design","기술"),
    ("CZR","Caesars Entertainment","소비재"),("CPT","Camden Property","리츠"),("CPB","Campbell Soup","소비재"),
    ("COF","Capital One","금융"),("CAH","Cardinal Health","헬스케어"),("KMX","CarMax","소비재"),
    ("CCL","Carnival","소비재"),("CARR","Carrier Global","산업재"),("CAT","Caterpillar","산업재"),
    ("CBOE","Cboe Global","금융"),("CBRE","CBRE Group","리츠"),("CDW","CDW Corp","기술"),
    ("CE","Celanese","소재"),("COR","Cencora","헬스케어"),("CNC","Centene","헬스케어"),
    ("CNP","CenterPoint","유틸리티"),("CF","CF Industries","소재"),("CRL","Charles River","헬스케어"),
    ("SCHW","Charles Schwab","금융"),("CHTR","Charter Comm","통신/미디어"),("CVX","Chevron","에너지"),
    ("CMG","Chipotle","소비재"),("CB","Chubb","보험"),("CHD","Church & Dwight","소비재"),
    ("CI","Cigna","헬스케어"),("CINF","Cincinnati Fin","보험"),("CTAS","Cintas","산업재"),
    ("CSCO","Cisco","기술"),("C","Citigroup","금융"),("CFG","Citizens Financial","금융"),
    ("CLX","Clorox","소비재"),("CME","CME Group","금융"),("CMS","CMS Energy","유틸리티"),
    ("KO","Coca-Cola","소비재"),("CTSH","Cognizant","기술"),("CL","Colgate-Palmolive","소비재"),
    ("CMCSA","Comcast","통신/미디어"),("CAG","Conagra","소비재"),("COP","ConocoPhillips","에너지"),
    ("ED","Consolidated Edison","유틸리티"),("STZ","Constellation Brands","소비재"),("CEG","Constellation Energy","유틸리티"),
    ("COO","Cooper Companies","헬스케어"),("CPRT","Copart","산업재"),("GLW","Corning","기술"),
    ("CPAY","Corpay","금융"),("CTVA","Corteva","소재"),("CSGP","CoStar","리츠"),
    ("COST","Costco","소비재"),("CTRA","Coterra Energy","에너지"),("CRWD","CrowdStrike","기술"),
    ("CCI","Crown Castle","리츠"),("CSX","CSX","산업재"),("CMI","Cummins","산업재"),
    ("CVS","CVS Health","헬스케어"),("DHR","Danaher","헬스케어"),("DRI","Darden Restaurants","소비재"),
    ("DVA","DaVita","헬스케어"),("DAY","Dayforce","기술"),("DECK","Deckers Outdoor","소비재"),
    ("DE","Deere & Co","산업재"),("DAL","Delta Air","산업재"),("DVN","Devon Energy","에너지"),
    ("DXCM","DexCom","헬스케어"),("FANG","Diamondback","에너지"),("DLR","Digital Realty","리츠"),
    ("DFS","Discover Financial","금융"),("DG","Dollar General","소비재"),("DLTR","Dollar Tree","소비재"),
    ("D","Dominion Energy","유틸리티"),("DPZ","Domino's Pizza","소비재"),("DOV","Dover","산업재"),
    ("DOW","Dow","소재"),("DHI","D.R. Horton","소비재"),("DTE","DTE Energy","유틸리티"),
    ("DUK","Duke Energy","유틸리티"),("DD","DuPont","소재"),("EMN","Eastman Chemical","소재"),
    ("ETN","Eaton","산업재"),("EBAY","eBay","이커머스"),("ECL","Ecolab","소재"),
    ("EIX","Edison Intl","유틸리티"),("EW","Edwards Lifesciences","헬스케어"),("EA","Electronic Arts","기술"),
    ("ELV","Elevance Health","헬스케어"),("EMR","Emerson Electric","산업재"),("ENPH","Enphase Energy","에너지"),
    ("ETR","Entergy","유틸리티"),("EOG","EOG Resources","에너지"),("EPAM","EPAM Systems","기술"),
    ("EQT","EQT","에너지"),("EFX","Equifax","기술"),("EQIX","Equinix","리츠"),
    ("EQR","Equity Residential","리츠"),("ESS","Essex Property","리츠"),("EL","Estee Lauder","소비재"),
    ("EG","Everest Group","보험"),("EVRG","Evergy","유틸리티"),("ES","Eversource","유틸리티"),
    ("EXC","Exelon","유틸리티"),("EXPE","Expedia","소비재"),("EXPD","Expeditors","산업재"),
    ("EXR","Extended Stay","리츠"),("XOM","ExxonMobil","에너지"),("FFIV","F5","기술"),
    ("FDS","FactSet","금융"),("FICO","FICO","기술"),("FAST","Fastenal","산업재"),
    ("FRT","Federal Realty","리츠"),("FDX","FedEx","산업재"),("FIS","Fidelity Natl Info","기술"),
    ("FITB","Fifth Third","금융"),("FSLR","First Solar","에너지"),("FE","FirstEnergy","유틸리티"),
    ("FI","Fiserv","기술"),("F","Ford Motor","자동차"),("FTNT","Fortinet","기술"),
    ("FTV","Fortive","산업재"),("FOXA","Fox A","통신/미디어"),("FOX","Fox B","통신/미디어"),
    ("BEN","Franklin Resources","금융"),("FCX","Freeport-McMoRan","소재"),("GRMN","Garmin","기술"),
    ("IT","Gartner","기술"),("GE","GE Aerospace","산업재"),("GEHC","GE HealthCare","헬스케어"),
    ("GEV","GE Vernova","유틸리티"),("GEN","Gen Digital","기술"),("GNRC","Generac","산업재"),
    ("GD","General Dynamics","산업재"),("GIS","General Mills","소비재"),("GM","General Motors","자동차"),
    ("GPC","Genuine Parts","소비재"),("GILD","Gilead Sciences","바이오"),("GS","Goldman Sachs","금융"),
    ("HAL","Halliburton","에너지"),("HIG","Hartford Financial","보험"),("HAS","Hasbro","소비재"),
    ("HCA","HCA Healthcare","헬스케어"),("DOC","Healthpeak","리츠"),("HSIC","Henry Schein","헬스케어"),
    ("HSY","Hershey","소비재"),("HES","Hess","에너지"),("HPE","Hewlett Packard Ent","기술"),
    ("HLT","Hilton Worldwide","소비재"),("HOLX","Hologic","헬스케어"),("HD","Home Depot","소비재"),
    ("HON","Honeywell","산업재"),("HRL","Hormel Foods","소비재"),("HST","Host Hotels","리츠"),
    ("HWM","Howmet Aerospace","산업재"),("HPQ","HP Inc","기술"),("HUBB","Hubbell","산업재"),
    ("HUM","Humana","헬스케어"),("HBAN","Huntington Bancshares","금융"),("HII","Huntington Ingalls","산업재"),
    ("IBM","IBM","기술"),("IEX","IDEX","산업재"),("IDXX","IDEXX Labs","헬스케어"),
    ("ITW","Illinois Tool Works","산업재"),("INCY","Incyte","바이오"),("IR","Ingersoll Rand","산업재"),
    ("PODD","Insulet","헬스케어"),("INTC","Intel","반도체"),("ICE","Intercontinental Exchange","금융"),
    ("IFF","Intl Flavors","소재"),("IP","International Paper","소재"),("IPG","Interpublic","통신/미디어"),
    ("INTU","Intuit","기술"),("ISRG","Intuitive Surgical","헬스케어"),("IVZ","Invesco","금융"),
    ("INVH","Invitation Homes","리츠"),("IQV","IQVIA Holdings","헬스케어"),("IRM","Iron Mountain","리츠"),
    ("JBHT","J.B. Hunt Transport","산업재"),("JBL","Jabil","기술"),("JKHY","Jack Henry","기술"),
    ("J","Jacobs Solutions","산업재"),("JNJ","Johnson & Johnson","헬스케어"),("JCI","Johnson Controls","산업재"),
    ("JPM","JPMorgan Chase","금융"),("JNPR","Juniper Networks","기술"),("K","Kellanova","소비재"),
    ("KVUE","Kenvue","소비재"),("KIM","Kimco Realty","리츠"),("KMI","Kinder Morgan","에너지"),
    ("KKR","KKR & Co","금융"),("KLAC","KLA","반도체"),("KHC","Kraft Heinz","소비재"),
    ("KR","Kroger","소비재"),("LHX","L3Harris","산업재"),("LH","LabCorp","헬스케어"),
    ("LRCX","Lam Research","반도체"),("LW","Lamb Weston","소비재"),("LVS","Las Vegas Sands","소비재"),
    ("LDOS","Leidos Holdings","산업재"),("LEN","Lennar","소비재"),("LLY","Eli Lilly","제약"),
    ("LIN","Linde","소재"),("LYV","Live Nation","소비재"),("LKQ","LKQ","소비재"),
    ("LMT","Lockheed Martin","산업재"),("L","Loews","보험"),("LOW","Lowe's","소비재"),
    ("LULU","Lululemon","소비재"),("LYB","LyondellBasell","소재"),("MTB","M&T Bank","금융"),
    ("MRO","Marathon Oil","에너지"),("MPC","Marathon Petroleum","에너지"),("MKTX","MarketAxess","금융"),
    ("MAR","Marriott Intl","소비재"),("MMC","Marsh McLennan","보험"),("MLM","Martin Marietta","소재"),
    ("MAS","Masco","산업재"),("MA","Mastercard","금융"),("MTCH","Match Group","통신/미디어"),
    ("MKC","McCormick","소비재"),("MCD","McDonald's","소비재"),("MCK","McKesson","헬스케어"),
    ("MDT","Medtronic","헬스케어"),("MRK","Merck","제약"),("META","Meta Platforms","통신/미디어"),
    ("MET","MetLife","보험"),("MTD","Mettler-Toledo","헬스케어"),("MGM","MGM Resorts","소비재"),
    ("MCHP","Microchip Tech","반도체"),("MU","Micron Tech","반도체"),("MSFT","Microsoft","기술"),
    ("MAA","Mid-America Apt","리츠"),("MRNA","Moderna","바이오"),("MOH","Molina Healthcare","헬스케어"),
    ("TAP","Molson Coors","소비재"),("MDLZ","Mondelez","소비재"),("MPWR","Monolithic Power","반도체"),
    ("MNST","Monster Beverage","소비재"),("MCO","Moody's","금융"),("MS","Morgan Stanley","금융"),
    ("MOS","Mosaic","소재"),("MSI","Motorola Solutions","기술"),("MSCI","MSCI Inc","금융"),
    ("NDAQ","Nasdaq","금융"),("NTAP","NetApp","기술"),("NFLX","Netflix","통신/미디어"),
    ("NEM","Newmont","소재"),("NWSA","News Corp A","통신/미디어"),("NWS","News Corp B","통신/미디어"),
    ("NEE","NextEra Energy","유틸리티"),("NKE","Nike","소비재"),("NI","NiSource","유틸리티"),
    ("NSCSC","Norfolk Southern","산업재"),("NSC","Norfolk Southern","산업재"),("NTRS","Northern Trust","금융"),
    ("NOC","Northrop Grumman","산업재"),("NCLH","Norwegian Cruise","소비재"),("NRG","NRG Energy","유틸리티"),
    ("NUE","Nucor","철강"),("NVDA","NVIDIA","반도체"),("NVR","NVR Inc","소비재"),
    ("NXPI","NXP Semiconductors","반도체"),("ORLY","O'Reilly Auto","소비재"),("OXY","Occidental","에너지"),
    ("ODFL","Old Dominion","산업재"),("OMC","Omnicom","통신/미디어"),("ON","ON Semiconductor","반도체"),
    ("OKE","ONEOK","에너지"),("ORCL","Oracle","기술"),("OTIS","Otis Worldwide","산업재"),
    ("PCAR","PACCAR","산업재"),("PKG","Packaging Corp","소재"),("PANW","Palo Alto Networks","기술"),
    ("PARA","Paramount Global","통신/미디어"),("PH","Parker-Hannifin","산업재"),("PAYX","Paychex","기술"),
    ("PAYC","Paycom","기술"),("PYPL","PayPal","금융"),("PNR","Pentair","산업재"),
    ("PEP","PepsiCo","소비재"),("PFE","Pfizer","제약"),("PCG","PG&E","유틸리티"),
    ("PM","Philip Morris","소비재"),("PSX","Phillips 66","에너지"),("PNW","Pinnacle West","유틸리티"),
    ("PNC","PNC Financial","금융"),("POOL","Pool Corp","산업재"),("PPG","PPG Industries","소재"),
    ("PPL","PPL Corp","유틸리티"),("PFG","Principal Financial","금융"),("PG","Procter & Gamble","소비재"),
    ("PGR","Progressive","보험"),("PLD","Prologis","리츠"),("PRU","Prudential","보험"),
    ("PEG","PSEG","유틸리티"),("PTC","PTC Inc","기술"),("PSA","Public Storage","리츠"),
    ("PHM","PulteGroup","소비재"),("QRVO","Qorvo","반도체"),("QCOM","Qualcomm","반도체"),
    ("PWR","Quanta Services","산업재"),("DGX","Quest Diagnostics","헬스케어"),("RL","Ralph Lauren","소비재"),
    ("RJF","Raymond James","금융"),("RTX","RTX Corp","산업재"),("O","Realty Income","리츠"),
    ("REG","Regency Centers","리츠"),("REGN","Regeneron","바이오"),("RF","Regions Financial","금융"),
    ("RSG","Republic Services","산업재"),("RMD","ResMed","헬스케어"),("RVTY","Revvity","헬스케어"),
    ("ROK","Rockwell Automation","산업재"),("ROL","Rollins","산업재"),("ROP","Roper Technologies","산업재"),
    ("ROST","Ross Stores","소비재"),("RCL","Royal Caribbean","소비재"),("SPGI","S&P Global","금융"),
    ("CRM","Salesforce","기술"),("SBAC","SBA Comm","리츠"),("SLB","SLB","에너지"),
    ("STX","Seagate Tech","기술"),("SRE","Sempra","유틸리티"),("NOW","ServiceNow","기술"),
    ("SHW","Sherwin-Williams","소재"),("SPG","Simon Property","리츠"),("SWKS","Skyworks","반도체"),
    ("SNA","Snap-on","산업재"),("SOLV","Solventum","헬스케어"),("SO","Southern Co","유틸리티"),
    ("LUV","Southwest Airlines","산업재"),("SWK","Stanley Black & Decker","산업재"),("SBUX","Starbucks","소비재"),
    ("STT","State Street","금융"),("STLD","Steel Dynamics","철강"),("STE","Steris","헬스케어"),
    ("SYK","Stryker","헬스케어"),("SYF","Synchrony Financial","금융"),("SNPS","Synopsys","기술"),
    ("SYY","Sysco","소비재"),("TMUS","T-Mobile US","통신/미디어"),("TDC","Teradata","기술"),
    ("TER","Teradyne","반도체"),("TSLA","Tesla","전기차"),("TXN","Texas Instruments","반도체"),
    ("TXT","Textron","산업재"),("TMO","Thermo Fisher","헬스케어"),("TJX","TJX Companies","소비재"),
    ("TSCO","Tractor Supply","소비재"),("TT","Trane Technologies","산업재"),("TDY","Teledyne","기술"),
    ("TRMB","Trimble","기술"),("TFC","Truist Financial","금융"),("TYL","Tyler Technologies","기술"),
    ("TSN","Tyson Foods","소비재"),("USB","U.S. Bancorp","금융"),("UBER","Uber","산업재"),
    ("UDR","UDR Inc","리츠"),("ULTA","Ulta Beauty","소비재"),("UNH","UnitedHealth","헬스케어"),
    ("UPS","UPS","산업재"),("URI","United Rentals","산업재"),("UNP","Union Pacific","산업재"),
    ("UAL","United Airlines","산업재"),("UHS","Universal Health","헬스케어"),("VLO","Valero Energy","에너지"),
    ("VTR","Ventas","리츠"),("VLTO","Veralto","산업재"),("VRSN","VeriSign","기술"),
    ("VRSK","Verisk Analytics","산업재"),("VZ","Verizon","통신/미디어"),("VRTX","Vertex Pharma","바이오"),
    ("VTRS","Viatris","제약"),("VIAV","Viavi Solutions","기술"),("V","Visa","금융"),
    ("VST","Vistra","유틸리티"),("VMC","Vulcan Materials","소재"),("WRB","W.R. Berkley","보험"),
    ("GWW","W.W. Grainger","산업재"),("WAB","Wabtec","산업재"),("WBA","Walgreens Boots","헬스케어"),
    ("WMT","Walmart","소비재"),("DIS","Walt Disney","통신/미디어"),("WBD","Warner Bros Discovery","통신/미디어"),
    ("WAT","Waters Corp","헬스케어"),("WEC","WEC Energy","유틸리티"),("WFC","Wells Fargo","금융"),
    ("WELL","Welltower","리츠"),("WST","West Pharma","헬스케어"),("WDC","Western Digital","기술"),
    ("WRK","WestRock","소재"),("WY","Weyerhaeuser","리츠"),("WMB","Williams Companies","에너지"),
    ("WTW","Willis Towers Watson","보험"),("WYNN","Wynn Resorts","소비재"),("XEL","Xcel Energy","유틸리티"),
    ("XYL","Xylem","산업재"),("YUM","Yum! Brands","소비재"),("ZBRA","Zebra Tech","기술"),
    ("ZBH","Zimmer Biomet","헬스케어"),("ZTS","Zoetis","헬스케어"),
]

# ── 한국 시총 상위 500 수집 (KRX API) ────────────────────
KRX_SECTOR_MAP = {
    "전기전자":"반도체/전자","화학":"화학","운수장비":"자동차",
    "금융업":"금융","의약품":"바이오","서비스업":"서비스",
    "철강금속":"철강","음식료품":"식품","통신업":"통신",
    "건설업":"건설","유통업":"유통","기계":"기계",
    "전기가스업":"에너지","운수창고":"물류","의료정밀":"헬스케어",
    "보험":"금융","증권":"금융","섬유의복":"섬유",
}

def get_krx_top500():
    print("  📋 KRX 종목 리스트 수집 중...")
    def fetch(mkt_id):
        url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        payload = {"bld":"dbms/MDC/STAT/standard/MDCSTAT01501","mktId":mkt_id,"share":"1","money":"1","csvxls_isNo":"false"}
        headers = {"Referer":"https://data.krx.co.kr/","User-Agent":"Mozilla/5.0","Content-Type":"application/x-www-form-urlencoded"}
        r = requests.post(url, data=payload, headers=headers, timeout=30)
        return pd.DataFrame(r.json()["OutBlock_1"])
    try:
        df = pd.concat([fetch("STK"), fetch("KSQ")], ignore_index=True)
        df["MKTCAP"] = pd.to_numeric(df["MKTCAP"], errors="coerce").fillna(0)
        df = df.sort_values("MKTCAP", ascending=False).head(500)
        result = []
        for _, row in df.iterrows():
            ticker = str(row["ISU_SRT_CD"]).zfill(6)
            name = str(row["ISU_ABBRV"])
            sector_raw = str(row.get("IDX_IND_NM","기타"))
            sector = KRX_SECTOR_MAP.get(sector_raw, sector_raw)
            cap_usd = float(row["MKTCAP"]) * 1e6 / 1350
            result.append({"ticker":ticker,"name":name,"sector":sector,"cap_usd":cap_usd,"cap_label":fmt_cap(cap_usd,"kr")})
        print(f"  ✅ {len(result)}개 종목 수집 완료")
        return result
    except Exception as e:
        print(f"  ⚠ KRX API 오류: {e} → 백업 리스트 사용")
        return [
            {"ticker":"005930","name":"삼성전자","sector":"반도체/전자","cap_usd":280e9,"cap_label":"378조"},
            {"ticker":"000660","name":"SK하이닉스","sector":"반도체/전자","cap_usd":110e9,"cap_label":"149조"},
            {"ticker":"005380","name":"현대차","sector":"자동차","cap_usd":33e9,"cap_label":"45조"},
            {"ticker":"373220","name":"LG에너지솔루션","sector":"2차전지","cap_usd":66e9,"cap_label":"90조"},
            {"ticker":"000270","name":"기아","sector":"자동차","cap_usd":29e9,"cap_label":"40조"},
        ]

# ── MA10 배치 계산 ─────────────────────────────────────────
def process_batch(batch_tickers, ticker_meta, market, batch_no, total):
    batch_str = " ".join(batch_tickers)
    print(f"  배치 {batch_no}: {batch_tickers[0]}~{batch_tickers[-1]} ({len(batch_tickers)}개)")
    success = 0
    try:
        hist = yf.download(batch_str, period="2y", interval="1mo",
                           group_by="ticker", auto_adjust=True, progress=False, threads=True)
        for ticker in batch_tickers:
            try:
                close = hist["Close"].dropna() if len(batch_tickers)==1 else hist[ticker]["Close"].dropna()
                if len(close) < 10: continue
                ma10 = float(close.iloc[-10:].mean())
                price = float(close.iloc[-1])
                gap_pct = round((price - ma10) / ma10 * 100, 2)
                meta = ticker_meta[ticker]
                pure = ticker.replace(".KS","")
                row = {
                    "ticker": pure,
                    "name": meta["name"],
                    "market": market,
                    "sector": meta["sector"],
                    "price": round(price, 2 if market=="us" else 0),
                    "ma10":  round(ma10,  2 if market=="us" else 0),
                    "gap_pct": gap_pct,
                    "market_cap_usd": meta.get("cap_usd", 0),
                    "market_cap_label": meta.get("cap_label", "-"),
                }
                upsert(row)
                success += 1
            except: pass
    except Exception as e:
        print(f"  ⚠ 배치 오류: {e}")
    return success

# ── 미국 실행 ─────────────────────────────────────────────
def run_us():
    print("\n🇺🇸 미국 S&P 500 업데이트 시작")
    tickers = [t[0] for t in SP500]
    meta = {t[0]: {"name":t[1],"sector":t[2],"cap_usd":0,"cap_label":"-"} for t in SP500}

    # 시가총액 별도 수집 (fast_info 사용)
    print("  📊 시가총액 수집 중...")
    for ticker, m in meta.items():
        try:
            fi = yf.Ticker(ticker).fast_info
            cap = getattr(fi, 'market_cap', 0) or 0
            m["cap_usd"] = cap
            m["cap_label"] = fmt_cap(cap, "us")
        except: pass

    batch_size = 25
    total_success = 0
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        total_success += process_batch(batch, meta, "us", i//batch_size+1, len(tickers)//batch_size+1)
        time.sleep(1)
    print(f"  ✅ 미국: {total_success}/{len(tickers)} 완료")

# ── 한국 실행 ─────────────────────────────────────────────
def run_kr():
    print("\n🇰🇷 한국 시총 상위 500 업데이트 시작")
    stocks = get_krx_top500()
    yf_tickers = [s["ticker"]+".KS" for s in stocks]
    meta = {s["ticker"]+".KS": {"name":s["name"],"sector":s["sector"],"cap_usd":s["cap_usd"],"cap_label":s["cap_label"]} for s in stocks}

    batch_size = 20
    total_success = 0
    for i in range(0, len(yf_tickers), batch_size):
        batch = yf_tickers[i:i+batch_size]
        total_success += process_batch(batch, meta, "kr", i//batch_size+1, len(yf_tickers)//batch_size+1)
        time.sleep(1)
    print(f"  ✅ 한국: {total_success}/{len(stocks)} 완료")

# ── 메인 ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("MA10 Signal 주가 업데이트 시작")
    print(f"실행 시각: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*55)
    run_us()
    run_kr()
    print("\n🎉 전체 업데이트 완료")
