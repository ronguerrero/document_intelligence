#!/usr/bin/env python3
"""Generate realistic financial document PDFs for AI extraction demo."""

import io
import json
import os
import urllib.request
import urllib.error
from datetime import date

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak,
)

token_raw = os.popen("databricks auth token --profile=lakedoom 2>/dev/null").read()
TOKEN = json.loads(token_raw)["access_token"]
HOST = "https://fevm-lakedoom-demo.cloud.databricks.com"
VOL = "/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents"

def upload(path, data):
    url = f"{HOST}/api/2.0/fs/files{path}"
    req = urllib.request.Request(url, data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/octet-stream"}, method="PUT")
    try:
        with urllib.request.urlopen(req) as r: return True
    except urllib.error.HTTPError as e:
        print(f"  Upload error: {e.code}"); return False

# Colors
NAVY = HexColor("#0A1628"); BLUE = HexColor("#1A5276"); DARK = HexColor("#2C3E50")
GOLD = HexColor("#D4AC0D"); GREEN = HexColor("#1E8449"); RED = HexColor("#C0392B")
GRAY = HexColor("#7F8C8D"); LGRAY = HexColor("#ECF0F1"); BLACK = HexColor("#1C2833")
WHITE = HexColor("#FFFFFF")

styles = getSampleStyleSheet()
title_s = ParagraphStyle("T", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=18, textColor=NAVY, alignment=TA_CENTER, spaceAfter=4)
sub_s = ParagraphStyle("Sub", parent=styles["Normal"], fontName="Helvetica", fontSize=10, textColor=GRAY, alignment=TA_CENTER, spaceAfter=20)
h1 = ParagraphStyle("H1", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=13, textColor=NAVY, spaceBefore=16, spaceAfter=6)
h2 = ParagraphStyle("H2", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=11, textColor=BLUE, spaceBefore=12, spaceAfter=4)
body = ParagraphStyle("B", parent=styles["Normal"], fontName="Helvetica", fontSize=9.5, leading=14, textColor=BLACK, spaceAfter=8)
small = ParagraphStyle("Sm", parent=styles["Normal"], fontName="Helvetica", fontSize=7.5, textColor=GRAY, alignment=TA_CENTER, spaceBefore=8)
label_s = ParagraphStyle("L", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=8.5, textColor=GRAY)
val_s = ParagraphStyle("V", parent=styles["Normal"], fontName="Helvetica", fontSize=9.5, textColor=BLACK)

def field_table(fields, cw1=2.2*inch, cw2=4.5*inch):
    data = [[Paragraph(l, label_s), Paragraph(str(v), val_s)] for l, v in fields]
    t = Table(data, colWidths=[cw1, cw2])
    t.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),("LINEBELOW",(0,0),(-1,-1),0.5,LGRAY)]))
    return t

def num_table(headers, rows, col_widths=None):
    """Table with header row."""
    hstyle = ParagraphStyle("TH", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=8.5, textColor=WHITE)
    cstyle = ParagraphStyle("TC", parent=styles["Normal"], fontName="Helvetica", fontSize=8.5, textColor=BLACK)
    data = [[Paragraph(h, hstyle) for h in headers]]
    for row in rows:
        data.append([Paragraph(str(c), cstyle) for c in row])
    if not col_widths:
        col_widths = [6.7*inch / len(headers)] * len(headers)
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), NAVY), ("TEXTCOLOR",(0,0),(-1,0), WHITE),
        ("ALIGN",(1,1),(-1,-1),"RIGHT"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),4), ("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("GRID",(0,0),(-1,-1),0.5, LGRAY),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, HexColor("#F8F9FA")]),
    ]))
    return t

# ═══════════════════════════════════════════════════════════════
# Document 1: Quarterly Earnings Report - TechNova Corp
# ═══════════════════════════════════════════════════════════════
def gen_earnings_report():
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=.75*inch, rightMargin=.75*inch, topMargin=.75*inch, bottomMargin=.75*inch)
    s = []
    s.append(Paragraph("TECHNOVA CORPORATION", title_s))
    s.append(Paragraph("Quarterly Earnings Report — Q1 2026", sub_s))
    s.append(HRFlowable(width="100%", thickness=2, color=GOLD, spaceAfter=12))

    s.append(Paragraph("Company Overview", h1))
    s.append(Paragraph("TechNova Corporation (NYSE: TNVA) is a leading enterprise software and cloud infrastructure company headquartered in Austin, Texas. The company provides AI-powered analytics platforms, cloud data management solutions, and cybersecurity services to Fortune 500 enterprises globally.", body))

    s.append(Paragraph("Financial Highlights", h1))
    s.append(num_table(
        ["Metric", "Q1 2026", "Q1 2025", "YoY Change"],
        [["Revenue", "$4.82B", "$3.91B", "+23.3%"],
         ["Gross Profit", "$3.38B", "$2.66B", "+27.1%"],
         ["Operating Income", "$1.45B", "$1.02B", "+42.2%"],
         ["Net Income", "$1.18B", "$812M", "+45.3%"],
         ["Earnings Per Share (Diluted)", "$2.94", "$2.01", "+46.3%"],
         ["Free Cash Flow", "$1.62B", "$1.15B", "+40.9%"],
         ["Gross Margin", "70.1%", "68.0%", "+210 bps"],
         ["Operating Margin", "30.1%", "26.1%", "+400 bps"]],
        [2.2*inch, 1.3*inch, 1.3*inch, 1.3*inch]
    ))

    s.append(Paragraph("Revenue Breakdown by Segment", h1))
    s.append(num_table(
        ["Segment", "Revenue", "% of Total", "YoY Growth"],
        [["Cloud Platform", "$2.41B", "50%", "+31.2%"],
         ["AI & Analytics", "$1.21B", "25%", "+28.7%"],
         ["Cybersecurity", "$723M", "15%", "+15.4%"],
         ["Professional Services", "$482M", "10%", "+8.1%"]],
        [2.2*inch, 1.3*inch, 1.3*inch, 1.3*inch]
    ))

    s.append(Paragraph("Geographic Revenue Distribution", h2))
    s.append(num_table(
        ["Region", "Revenue", "% of Total"],
        [["North America", "$2.89B", "60%"],
         ["Europe", "$1.11B", "23%"],
         ["Asia Pacific", "$578M", "12%"],
         ["Rest of World", "$241M", "5%"]],
        [2.5*inch, 1.5*inch, 1.5*inch]
    ))

    s.append(Paragraph("Balance Sheet Highlights", h1))
    s.append(field_table([
        ("Total Assets:", "$42.8B"),
        ("Cash & Equivalents:", "$8.92B"),
        ("Total Debt:", "$12.4B"),
        ("Shareholders' Equity:", "$22.1B"),
        ("Debt-to-Equity Ratio:", "0.56x"),
        ("Current Ratio:", "2.31x"),
    ]))

    s.append(Paragraph("Guidance — Q2 2026", h1))
    s.append(Paragraph("Management expects Q2 2026 revenue in the range of $5.05B to $5.15B, representing 21-23% year-over-year growth. Operating margin is expected to expand to 31-32%. The company raised its full-year 2026 revenue guidance to $20.5B-$21.0B from the prior range of $19.8B-$20.3B.", body))
    s.append(field_table([
        ("Q2 Revenue Guidance:", "$5.05B - $5.15B"),
        ("Q2 EPS Guidance:", "$3.10 - $3.20"),
        ("FY2026 Revenue Guidance:", "$20.5B - $21.0B"),
        ("FY2026 EPS Guidance:", "$12.80 - $13.20"),
    ]))

    s.append(Paragraph("Key Risk Factors", h1))
    s.append(Paragraph("Macroeconomic uncertainty and potential IT spending slowdown. Competitive pressure from hyperscalers (AWS, Azure, GCP) in cloud platform segment. Regulatory risk from EU AI Act and data sovereignty requirements in APAC markets. Foreign currency headwinds estimated at 150bps revenue impact. Customer concentration risk — top 10 clients represent 28% of revenue.", body))

    s.append(Spacer(1, .3*inch))
    s.append(HRFlowable(width="100%", thickness=0.5, color=NAVY))
    s.append(Paragraph("TechNova Corporation — Q1 2026 Earnings Report — Confidential", small))
    doc.build(s); buf.seek(0); return buf.read()

# ═══════════════════════════════════════════════════════════════
# Document 2: Investment Memo - Meridian Infrastructure Fund
# ═══════════════════════════════════════════════════════════════
def gen_investment_memo():
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=.75*inch, rightMargin=.75*inch, topMargin=.75*inch, bottomMargin=.75*inch)
    s = []
    s.append(Paragraph("CRESTVIEW CAPITAL PARTNERS", title_s))
    s.append(Paragraph("Investment Memorandum — Meridian Infrastructure Fund III", sub_s))
    s.append(HRFlowable(width="100%", thickness=2, color=BLUE, spaceAfter=12))

    s.append(Paragraph("Fund Overview", h1))
    s.append(field_table([
        ("Fund Name:", "Meridian Infrastructure Fund III, LP"),
        ("General Partner:", "Crestview Capital Partners LLC"),
        ("Fund Size:", "$3.5 Billion (target)"),
        ("Hard Cap:", "$4.0 Billion"),
        ("Vintage Year:", "2026"),
        ("Fund Life:", "12 years (2 x 1-year extensions)"),
        ("Investment Period:", "5 years"),
        ("Target Return (Net IRR):", "12-15%"),
        ("Target Multiple (Net MOIC):", "1.7x - 2.0x"),
        ("Management Fee:", "1.50% on committed capital (investment period), 1.25% on invested capital (post-investment period)"),
        ("Carried Interest:", "20% over 8% preferred return"),
        ("GP Commitment:", "$70M (2% of target)"),
    ]))

    s.append(Paragraph("Investment Strategy", h1))
    s.append(Paragraph("Meridian Infrastructure Fund III targets mid-market infrastructure investments across North America and Western Europe, focusing on essential-service assets with contracted or regulated revenue profiles. The fund seeks to acquire, develop, and optimize assets in digital infrastructure, energy transition, transportation, and water/waste management.", body))

    s.append(Paragraph("Target Sector Allocation", h2))
    s.append(num_table(
        ["Sector", "Target Allocation", "Typical Deal Size", "Target Yield"],
        [["Digital Infrastructure (Data Centers, Fiber)", "30-35%", "$150M - $400M", "8-12%"],
         ["Energy Transition (Solar, Storage, Grid)", "25-30%", "$100M - $350M", "9-14%"],
         ["Transportation (Toll Roads, Ports, Rail)", "20-25%", "$200M - $500M", "7-10%"],
         ["Water & Waste Management", "10-15%", "$75M - $200M", "8-11%"]],
        [2.0*inch, 1.2*inch, 1.5*inch, 1.2*inch]
    ))

    s.append(Paragraph("Track Record — Fund I & II", h1))
    s.append(num_table(
        ["Fund", "Vintage", "Size", "Net IRR", "Net MOIC", "DPI", "Status"],
        [["Fund I", "2018", "$1.8B", "16.2%", "1.82x", "1.45x", "Harvesting"],
         ["Fund II", "2022", "$2.6B", "14.8%", "1.41x", "0.32x", "Investing"]],
        [1.0*inch, .8*inch, .8*inch, .8*inch, .8*inch, .8*inch, 1.0*inch]
    ))

    s.append(Paragraph("Key Terms & Conditions", h1))
    s.append(Paragraph("Minimum commitment: $10M. Quarterly capital calls with 10 business days notice. Annual audited financials by Ernst & Young. Advisory committee comprising LPs with commitments exceeding $100M. Key-person clause covering Managing Partners David Chen and Sarah Blackwood. No-fault divorce provision requiring 75% LP vote.", body))

    s.append(Paragraph("Risk Factors", h1))
    s.append(Paragraph("Infrastructure investments are subject to regulatory and political risk, construction and development risk, interest rate sensitivity, and environmental/ESG compliance requirements. Illiquid investments with limited secondary market. Currency risk for European investments. Concentration risk if deployment pace exceeds pipeline development. Climate-related physical risk to transportation and energy assets.", body))

    s.append(Spacer(1, .3*inch))
    s.append(HRFlowable(width="100%", thickness=0.5, color=BLUE))
    s.append(Paragraph("Crestview Capital Partners — Confidential — Not for Distribution", small))
    doc.build(s); buf.seek(0); return buf.read()

# ═══════════════════════════════════════════════════════════════
# Document 3: Credit Research Note - Sovereign Bond Analysis
# ═══════════════════════════════════════════════════════════════
def gen_credit_research():
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=.75*inch, rightMargin=.75*inch, topMargin=.75*inch, bottomMargin=.75*inch)
    s = []
    s.append(Paragraph("ATLAS FIXED INCOME RESEARCH", title_s))
    s.append(Paragraph("Sovereign Credit Analysis — Republic of Colombia", sub_s))
    s.append(HRFlowable(width="100%", thickness=2, color=GREEN, spaceAfter=12))

    s.append(Paragraph("Rating Summary", h1))
    s.append(field_table([
        ("Issuer:", "Republic of Colombia"),
        ("Issuer Type:", "Sovereign"),
        ("Currency:", "USD / COP"),
        ("S&P Rating:", "BB+ (Stable)"),
        ("Moody's Rating:", "Baa2 (Negative)"),
        ("Fitch Rating:", "BB+ (Stable)"),
        ("Analyst:", "Maria Rodriguez, CFA"),
        ("Report Date:", "April 15, 2026"),
        ("Recommendation:", "OVERWEIGHT — Attractive spread vs. BB peers"),
    ]))

    s.append(Paragraph("Key Economic Indicators", h1))
    s.append(num_table(
        ["Indicator", "2024", "2025E", "2026E"],
        [["GDP Growth", "1.6%", "2.8%", "3.2%"],
         ["Inflation (CPI)", "9.3%", "5.8%", "4.2%"],
         ["Policy Rate", "13.25%", "9.50%", "7.75%"],
         ["Fiscal Deficit (% GDP)", "-4.3%", "-3.8%", "-3.2%"],
         ["Government Debt (% GDP)", "57.2%", "55.8%", "54.1%"],
         ["Current Account (% GDP)", "-2.7%", "-2.3%", "-2.0%"],
         ["FX Reserves", "$58.2B", "$61.4B", "$64.0B"],
         ["Unemployment Rate", "10.2%", "9.8%", "9.3%"]],
        [2.2*inch, 1.3*inch, 1.3*inch, 1.3*inch]
    ))

    s.append(Paragraph("Outstanding Bond Issuances", h1))
    s.append(num_table(
        ["Bond", "Coupon", "Maturity", "Outstanding", "Price", "YTW", "Z-Spread"],
        [["COLOM 3.875%", "3.875%", "04/2027", "$2.5B", "97.25", "5.12%", "+185bp"],
         ["COLOM 4.125%", "4.125%", "02/2029", "$3.0B", "94.50", "5.48%", "+210bp"],
         ["COLOM 5.000%", "5.000%", "06/2031", "$2.0B", "96.75", "5.62%", "+225bp"],
         ["COLOM 5.625%", "5.625%", "02/2034", "$3.5B", "98.10", "5.85%", "+245bp"],
         ["COLOM 7.375%", "7.375%", "09/2037", "$1.5B", "108.50", "6.15%", "+270bp"],
         ["COLOM 6.125%", "6.125%", "01/2041", "$2.0B", "99.25", "6.20%", "+275bp"]],
        [1.1*inch, .7*inch, .8*inch, .8*inch, .7*inch, .7*inch, .8*inch]
    ))

    s.append(Paragraph("Investment Thesis", h1))
    s.append(Paragraph("Colombia offers an attractive risk-adjusted return within the BB/BBB- sovereign space. The fiscal consolidation trajectory is credible, supported by mining royalty reform and tax collection improvements. The central bank's inflation-targeting framework has proven effective, with CPI on a clear downward path toward the 3% target. FX reserves provide adequate external vulnerability buffer at 7.2 months of import coverage. The 210-275bp spread over comparable investment-grade LatAm sovereigns (Brazil BBB-, Mexico BBB) appears wide given the improving fiscal trajectory.", body))

    s.append(Paragraph("Risks to Thesis", h1))
    s.append(Paragraph("Political risk from populist policy agenda. Commodity dependence (oil 25% of exports, coal 12%). Security situation and peace process uncertainty. Pension reform implementation risk. El Nino-related agricultural and energy sector disruption. Global risk-off scenarios impacting EM flows.", body))

    s.append(Spacer(1, .3*inch))
    s.append(HRFlowable(width="100%", thickness=0.5, color=GREEN))
    s.append(Paragraph("Atlas Fixed Income Research — For Institutional Investors Only", small))
    doc.build(s); buf.seek(0); return buf.read()

# ═══════════════════════════════════════════════════════════════
# Document 4: Private Equity Deal Sheet
# ═══════════════════════════════════════════════════════════════
def gen_pe_deal_sheet():
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=.75*inch, rightMargin=.75*inch, topMargin=.75*inch, bottomMargin=.75*inch)
    s = []
    s.append(Paragraph("SENTINEL EQUITY PARTNERS", title_s))
    s.append(Paragraph("Confidential Deal Sheet — Acquisition of NovaPharma Holdings", sub_s))
    s.append(HRFlowable(width="100%", thickness=2, color=DARK, spaceAfter=12))

    s.append(Paragraph("Transaction Summary", h1))
    s.append(field_table([
        ("Target Company:", "NovaPharma Holdings, Inc."),
        ("Sector:", "Healthcare / Specialty Pharmaceuticals"),
        ("Headquarters:", "Cambridge, Massachusetts"),
        ("Transaction Type:", "Leveraged Buyout (LBO)"),
        ("Enterprise Value:", "$2.8 Billion"),
        ("Equity Value:", "$1.65 Billion"),
        ("EV/EBITDA Multiple:", "11.2x (LTM EBITDA: $250M)"),
        ("EV/Revenue Multiple:", "3.5x (LTM Revenue: $800M)"),
        ("Sponsor:", "Sentinel Equity Partners Fund V"),
        ("Equity Check:", "$1.1B (Sentinel: $850M, Co-invest: $250M)"),
        ("Debt Financing:", "$1.7B (Term Loan B: $1.2B, Senior Notes: $500M)"),
        ("Expected Close:", "Q3 2026"),
    ]))

    s.append(Paragraph("Target Company Profile", h1))
    s.append(Paragraph("NovaPharma is a specialty pharmaceutical company focused on rare diseases and orphan drugs. The company has a portfolio of 8 commercialized products and a pipeline of 12 clinical-stage candidates. Revenue has grown at a 18% CAGR over the past 5 years, driven by strong demand for its rare disease portfolio and successful label expansions. EBITDA margins have expanded from 24% to 31% over the same period through operating leverage and manufacturing optimization.", body))

    s.append(Paragraph("Financial Summary", h2))
    s.append(num_table(
        ["Metric", "FY2023", "FY2024", "FY2025", "FY2026E"],
        [["Revenue", "$580M", "$680M", "$800M", "$935M"],
         ["EBITDA", "$145M", "$190M", "$250M", "$310M"],
         ["EBITDA Margin", "25.0%", "27.9%", "31.3%", "33.2%"],
         ["Capex", "$35M", "$42M", "$48M", "$55M"],
         ["Free Cash Flow", "$82M", "$115M", "$162M", "$208M"],
         ["Net Debt/EBITDA", "1.8x", "1.5x", "1.2x", "4.8x*"]],
        [1.5*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch]
    ))
    s.append(Paragraph("* Post-transaction leverage", body))

    s.append(Paragraph("Value Creation Plan", h1))
    s.append(Paragraph("1) Revenue acceleration through geographic expansion into EU and Japan ($150M+ revenue opportunity by 2030). 2) Pipeline acceleration — advance 3 Phase II candidates to Phase III, with two potential blockbusters (>$500M peak sales each). 3) Margin expansion through manufacturing consolidation (3 sites to 2) and procurement optimization, targeting 38%+ EBITDA margins by exit. 4) Tuck-in M&A — $200M earmarked for bolt-on acquisitions in adjacent rare disease areas. 5) Digital transformation of commercial operations using AI-powered HCP targeting.", body))

    s.append(Paragraph("Return Analysis", h1))
    s.append(num_table(
        ["Scenario", "Exit EV/EBITDA", "Exit Year", "Equity Value", "MOIC", "Gross IRR"],
        [["Base Case", "12.0x", "2031", "$3.2B", "2.9x", "24%"],
         ["Upside Case", "13.5x", "2030", "$4.1B", "3.7x", "32%"],
         ["Downside Case", "10.0x", "2032", "$2.1B", "1.9x", "14%"]],
        [1.2*inch, 1.1*inch, .9*inch, 1.1*inch, .8*inch, .9*inch]
    ))

    s.append(Spacer(1, .3*inch))
    s.append(HRFlowable(width="100%", thickness=0.5, color=DARK))
    s.append(Paragraph("Sentinel Equity Partners — Strictly Confidential — Do Not Distribute", small))
    doc.build(s); buf.seek(0); return buf.read()

# ═══════════════════════════════════════════════════════════════
# Document 5: Annual Report Summary - GreenShield ESG Fund
# ═══════════════════════════════════════════════════════════════
def gen_esg_report():
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=.75*inch, rightMargin=.75*inch, topMargin=.75*inch, bottomMargin=.75*inch)
    s = []
    s.append(Paragraph("GREENSHIELD ASSET MANAGEMENT", title_s))
    s.append(Paragraph("Annual Performance Report — ESG Impact Fund 2025", sub_s))
    s.append(HRFlowable(width="100%", thickness=2, color=GREEN, spaceAfter=12))

    s.append(Paragraph("Fund Performance Summary", h1))
    s.append(field_table([
        ("Fund Name:", "GreenShield ESG Impact Fund"),
        ("Fund Manager:", "GreenShield Asset Management"),
        ("Benchmark:", "MSCI World ESG Leaders Index"),
        ("Fund AUM:", "$12.4 Billion"),
        ("Inception Date:", "March 2019"),
        ("Share Class:", "Institutional (Class I)"),
        ("Base Currency:", "USD"),
        ("SFDR Classification:", "Article 9 — Dark Green"),
    ]))

    s.append(Paragraph("Performance Summary", h2))
    s.append(num_table(
        ["Period", "Fund Return", "Benchmark", "Alpha"],
        [["YTD 2025", "+14.2%", "+11.8%", "+240bps"],
         ["1 Year", "+18.7%", "+15.3%", "+340bps"],
         ["3 Year (Ann.)", "+12.1%", "+9.8%", "+230bps"],
         ["5 Year (Ann.)", "+14.5%", "+11.2%", "+330bps"],
         ["Since Inception (Ann.)", "+13.8%", "+10.9%", "+290bps"]],
        [2.0*inch, 1.3*inch, 1.3*inch, 1.3*inch]
    ))

    s.append(Paragraph("Top 10 Holdings", h1))
    s.append(num_table(
        ["Holding", "Sector", "Weight", "ESG Score", "Carbon Intensity"],
        [["NextEra Energy", "Utilities", "4.8%", "AA", "42 tCO2e/$M"],
         ["ASML Holdings", "Technology", "4.2%", "AAA", "18 tCO2e/$M"],
         ["Schneider Electric", "Industrials", "3.9%", "AAA", "35 tCO2e/$M"],
         ["Prologis", "Real Estate", "3.5%", "AA", "28 tCO2e/$M"],
         ["Danaher Corp", "Healthcare", "3.2%", "AA", "22 tCO2e/$M"],
         ["Vestas Wind Systems", "Industrials", "3.0%", "AAA", "15 tCO2e/$M"],
         ["Adobe Inc", "Technology", "2.8%", "AA", "12 tCO2e/$M"],
         ["Xylem Inc", "Industrials", "2.5%", "AAA", "31 tCO2e/$M"],
         ["Orsted", "Utilities", "2.3%", "AAA", "8 tCO2e/$M"],
         ["Linde PLC", "Materials", "2.1%", "AA", "145 tCO2e/$M"]],
        [1.4*inch, 1.0*inch, .8*inch, .8*inch, 1.4*inch]
    ))

    s.append(Paragraph("ESG Impact Metrics", h1))
    s.append(field_table([
        ("Weighted Avg ESG Score:", "AA (top 15th percentile)"),
        ("Carbon Intensity:", "48 tCO2e per $M revenue (vs. 185 benchmark)"),
        ("Renewable Energy Exposure:", "32% of portfolio"),
        ("Board Gender Diversity:", "38% female directors (weighted avg)"),
        ("UN SDG Alignment:", "SDG 7 (Clean Energy), SDG 9 (Innovation), SDG 13 (Climate Action)"),
        ("Companies Engaged:", "42 engagements on 18 ESG topics"),
        ("Proxy Votes Cast:", "1,247 votes at 156 meetings, 94% for ESG resolutions"),
    ]))

    s.append(Spacer(1, .3*inch))
    s.append(HRFlowable(width="100%", thickness=0.5, color=GREEN))
    s.append(Paragraph("GreenShield Asset Management — For Qualified Institutional Investors Only", small))
    doc.build(s); buf.seek(0); return buf.read()

# ── Generate and upload all PDFs ─────────────────────────────
docs = [
    ("technova_q1_2026_earnings.pdf", "TechNova Q1 2026 Earnings", gen_earnings_report),
    ("meridian_infra_fund_iii_memo.pdf", "Meridian Infrastructure Fund III Memo", gen_investment_memo),
    ("colombia_sovereign_credit.pdf", "Colombia Sovereign Credit Analysis", gen_credit_research),
    ("novapharma_lbo_deal_sheet.pdf", "NovaPharma LBO Deal Sheet", gen_pe_deal_sheet),
    ("greenshield_esg_annual_report.pdf", "GreenShield ESG Fund Report", gen_esg_report),
]

print(f"Generating {len(docs)} financial documents...")
for fname, label, gen_fn in docs:
    pdf = gen_fn()
    path = f"{VOL}/{fname}"
    ok = upload(path, pdf)
    print(f"  {'OK' if ok else 'FAIL'}: {label} ({len(pdf):,} bytes) -> {fname}")

print(f"\nDone! Files uploaded to {VOL}")
