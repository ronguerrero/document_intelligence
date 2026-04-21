# Databricks notebook source
# MAGIC %md
# MAGIC # Capital Markets AI: Intelligent Document Extraction
# MAGIC
# MAGIC ### Turning Unstructured Financial Documents into Structured Portfolio Data
# MAGIC
# MAGIC This notebook demonstrates how Databricks AI Functions — **`ai_parse_document()`** and **`ai_extract()`** — can automatically extract structured data from financial PDFs at scale.
# MAGIC
# MAGIC **Use Case:** A capital markets team manages a portfolio spanning equities, fixed income, private equity, and alternative investments. Portfolio data arrives as PDFs — earnings reports, investment memos, credit research notes, deal sheets, and fund reports. Manually processing these documents is slow, error-prone, and doesn't scale.
# MAGIC
# MAGIC **What we'll show:**
# MAGIC 1. **`ai_parse_document()`** — Parse any PDF into structured text, tables, and sections
# MAGIC 2. **`ai_extract()`** — Extract specific financial fields using a JSON schema
# MAGIC 3. **Scale it** — Process an entire document library in a single SQL query
# MAGIC 4. **Govern it** — Store extracted data in Delta tables under Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Our Document Library
# MAGIC
# MAGIC We have 5 financial documents stored in a Unity Catalog Volume:
# MAGIC
# MAGIC | Document | Type | Content |
# MAGIC |----------|------|---------|
# MAGIC | `technova_q1_2026_earnings.pdf` | Quarterly Earnings | Revenue, EPS, margins, guidance, risk factors |
# MAGIC | `meridian_infra_fund_iii_memo.pdf` | Investment Memo | Fund terms, target returns, sector allocation, track record |
# MAGIC | `colombia_sovereign_credit.pdf` | Credit Research | Sovereign ratings, economic indicators, bond yields, spreads |
# MAGIC | `novapharma_lbo_deal_sheet.pdf` | PE Deal Sheet | Enterprise value, LBO structure, return scenarios |
# MAGIC | `greenshield_esg_annual_report.pdf` | ESG Fund Report | Performance, top holdings, ESG scores, carbon intensity |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what's in our document library
# MAGIC SELECT path, length(content) AS file_size_bytes
# MAGIC FROM read_files(
# MAGIC   '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/',
# MAGIC   format => 'binaryFile'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse Documents with `ai_parse_document()`
# MAGIC
# MAGIC `ai_parse_document()` converts raw PDF bytes into structured text that AI can reason over. It handles tables, headers, footers, multi-column layouts, and embedded charts.
# MAGIC
# MAGIC Let's start with a single document — the TechNova earnings report:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse a single earnings report
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_parse_document(
# MAGIC     content,
# MAGIC     map('version', '2.0', 'descriptionElementTypes', '*')
# MAGIC   )::STRING AS parsed_content
# MAGIC FROM read_files(
# MAGIC   '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/technova_q1_2026_earnings.pdf',
# MAGIC   format => 'binaryFile'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract Structured Data with `ai_extract()`
# MAGIC
# MAGIC Now the magic — `ai_extract()` takes the parsed document and extracts specific fields into a structured JSON object based on a schema you define.
# MAGIC
# MAGIC ### 2a. Extract from Quarterly Earnings Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract key financial metrics from the earnings report
# MAGIC WITH parsed_docs AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     ai_parse_document(
# MAGIC       content,
# MAGIC       map('version', '2.0', 'descriptionElementTypes', '*')
# MAGIC     ) AS parsed
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/technova_q1_2026_earnings.pdf',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{
# MAGIC       "company_name": {"type": "string", "description": "Name of the company"},
# MAGIC       "ticker": {"type": "string", "description": "Stock ticker symbol"},
# MAGIC       "report_period": {"type": "string", "description": "Reporting period (e.g., Q1 2026)"},
# MAGIC       "revenue": {"type": "string", "description": "Total revenue for the period"},
# MAGIC       "revenue_yoy_growth": {"type": "string", "description": "Year-over-year revenue growth percentage"},
# MAGIC       "net_income": {"type": "string", "description": "Net income for the period"},
# MAGIC       "earnings_per_share": {"type": "string", "description": "Diluted earnings per share"},
# MAGIC       "gross_margin": {"type": "string", "description": "Gross margin percentage"},
# MAGIC       "operating_margin": {"type": "string", "description": "Operating margin percentage"},
# MAGIC       "free_cash_flow": {"type": "string", "description": "Free cash flow for the period"},
# MAGIC       "revenue_guidance_low": {"type": "string", "description": "Low end of next quarter revenue guidance"},
# MAGIC       "revenue_guidance_high": {"type": "string", "description": "High end of next quarter revenue guidance"},
# MAGIC       "fy_revenue_guidance": {"type": "string", "description": "Full year revenue guidance range"},
# MAGIC       "total_debt": {"type": "string", "description": "Total debt on balance sheet"},
# MAGIC       "cash_and_equivalents": {"type": "string", "description": "Cash and cash equivalents"},
# MAGIC       "key_risk_factors": {
# MAGIC         "type": "array",
# MAGIC         "description": "List of key risk factors mentioned",
# MAGIC         "items": {"type": "string"}
# MAGIC       }
# MAGIC     }',
# MAGIC     options => map('version', '2.0')
# MAGIC   )::STRING AS extracted
# MAGIC FROM parsed_docs
# MAGIC WHERE parsed IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Extract from Investment Memorandum

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract fund terms from the infrastructure fund memo
# MAGIC WITH parsed_docs AS (
# MAGIC   SELECT path,
# MAGIC     ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', '*')) AS parsed
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/meridian_infra_fund_iii_memo.pdf',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{
# MAGIC       "fund_name": {"type": "string", "description": "Full legal name of the fund"},
# MAGIC       "general_partner": {"type": "string", "description": "Name of the general partner"},
# MAGIC       "fund_size_target": {"type": "string", "description": "Target fund size"},
# MAGIC       "hard_cap": {"type": "string", "description": "Hard cap of the fund"},
# MAGIC       "vintage_year": {"type": "string", "description": "Vintage year"},
# MAGIC       "target_net_irr": {"type": "string", "description": "Target net IRR range"},
# MAGIC       "target_net_moic": {"type": "string", "description": "Target net MOIC range"},
# MAGIC       "management_fee": {"type": "string", "description": "Management fee terms"},
# MAGIC       "carried_interest": {"type": "string", "description": "Carried interest terms"},
# MAGIC       "gp_commitment": {"type": "string", "description": "GP commitment amount"},
# MAGIC       "investment_strategy": {"type": "string", "description": "Brief description of investment strategy"},
# MAGIC       "target_sectors": {
# MAGIC         "type": "array",
# MAGIC         "description": "Target sectors for investment",
# MAGIC         "items": {"type": "string"}
# MAGIC       },
# MAGIC       "prior_fund_track_record": {
# MAGIC         "type": "array",
# MAGIC         "description": "Track record of prior funds with IRR and MOIC",
# MAGIC         "items": {
# MAGIC           "type": "object",
# MAGIC           "properties": {
# MAGIC             "fund_name": {"type": "string"},
# MAGIC             "net_irr": {"type": "string"},
# MAGIC             "net_moic": {"type": "string"}
# MAGIC           }
# MAGIC         }
# MAGIC       }
# MAGIC     }',
# MAGIC     options => map('version', '2.0')
# MAGIC   )::STRING AS extracted
# MAGIC FROM parsed_docs
# MAGIC WHERE parsed IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Extract from Credit Research / Sovereign Bond Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract sovereign credit data
# MAGIC WITH parsed_docs AS (
# MAGIC   SELECT path,
# MAGIC     ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', '*')) AS parsed
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/colombia_sovereign_credit.pdf',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{
# MAGIC       "issuer": {"type": "string", "description": "Name of the sovereign issuer"},
# MAGIC       "sp_rating": {"type": "string", "description": "S&P credit rating and outlook"},
# MAGIC       "moodys_rating": {"type": "string", "description": "Moodys credit rating and outlook"},
# MAGIC       "fitch_rating": {"type": "string", "description": "Fitch credit rating and outlook"},
# MAGIC       "analyst_recommendation": {"type": "string", "description": "Analyst recommendation"},
# MAGIC       "gdp_growth_2026e": {"type": "string", "description": "Expected GDP growth for 2026"},
# MAGIC       "inflation_2026e": {"type": "string", "description": "Expected inflation for 2026"},
# MAGIC       "govt_debt_to_gdp": {"type": "string", "description": "Government debt as percentage of GDP"},
# MAGIC       "fx_reserves": {"type": "string", "description": "Foreign exchange reserves"},
# MAGIC       "outstanding_bonds": {
# MAGIC         "type": "array",
# MAGIC         "description": "List of outstanding bond issuances with yields",
# MAGIC         "items": {
# MAGIC           "type": "object",
# MAGIC           "properties": {
# MAGIC             "bond_name": {"type": "string"},
# MAGIC             "coupon": {"type": "string"},
# MAGIC             "maturity": {"type": "string"},
# MAGIC             "yield_to_worst": {"type": "string"},
# MAGIC             "z_spread": {"type": "string"}
# MAGIC           }
# MAGIC         }
# MAGIC       },
# MAGIC       "investment_thesis_summary": {"type": "string", "description": "One-paragraph summary of the investment thesis"}
# MAGIC     }',
# MAGIC     options => map('version', '2.0')
# MAGIC   )::STRING AS extracted
# MAGIC FROM parsed_docs
# MAGIC WHERE parsed IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Process ALL Documents at Scale
# MAGIC
# MAGIC Here's the real power — process the entire document library in one query. Each document type gets the same extraction schema, and Databricks AI handles the variability across formats.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract a universal portfolio-relevant schema from ALL documents at once
# MAGIC WITH parsed_docs AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     ai_parse_document(
# MAGIC       content,
# MAGIC       map('version', '2.0', 'descriptionElementTypes', '*')
# MAGIC     ) AS parsed
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   regexp_extract(path, '([^/]+)\.pdf$', 1) AS document_name,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{
# MAGIC       "document_type": {"type": "string", "description": "Type of document: earnings_report, investment_memo, credit_research, deal_sheet, or fund_report"},
# MAGIC       "entity_name": {"type": "string", "description": "Primary company, fund, or issuer name"},
# MAGIC       "asset_class": {"type": "string", "description": "Asset class: equity, fixed_income, private_equity, infrastructure, or multi_asset"},
# MAGIC       "key_metric_1_name": {"type": "string", "description": "Name of the most important financial metric"},
# MAGIC       "key_metric_1_value": {"type": "string", "description": "Value of the most important financial metric"},
# MAGIC       "key_metric_2_name": {"type": "string", "description": "Name of the second most important metric"},
# MAGIC       "key_metric_2_value": {"type": "string", "description": "Value of the second most important metric"},
# MAGIC       "recommendation_or_outlook": {"type": "string", "description": "Investment recommendation, rating, forward guidance, or outlook. For earnings use management guidance. For fund memos use target return. For credit research use analyst recommendation. For deal sheets use base case return. For fund reports use performance vs benchmark. Always provide a value."},
# MAGIC       "risk_summary": {"type": "string", "description": "One-sentence summary of the key risk"},
# MAGIC       "geography": {"type": "string", "description": "Primary geographic focus"}
# MAGIC     }',
# MAGIC     options => map('version', '2.0')
# MAGIC   )::STRING AS extracted
# MAGIC FROM parsed_docs
# MAGIC WHERE parsed IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Materialize as a Governed Delta Table
# MAGIC
# MAGIC Now let's take the extracted data and persist it as a governed Delta table in Unity Catalog — ready for dashboards, Genie, and downstream analytics.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a structured portfolio documents table from AI extraction
# MAGIC CREATE OR REPLACE TABLE lakedoom_demo_catalog.capital_markets_ai.extracted_portfolio_documents AS
# MAGIC WITH parsed_docs AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     ai_parse_document(
# MAGIC       content,
# MAGIC       map('version', '2.0', 'descriptionElementTypes', '*')
# MAGIC     ) AS parsed
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/lakedoom_demo_catalog/capital_markets_ai/financial_documents/',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC ),
# MAGIC extracted AS (
# MAGIC   SELECT
# MAGIC     path AS source_file,
# MAGIC     regexp_extract(path, '([^/]+)\\.pdf$', 1) AS document_name,
# MAGIC     ai_extract(
# MAGIC       parsed,
# MAGIC       '{
# MAGIC         "document_type": {"type": "string", "description": "Type: earnings_report, investment_memo, credit_research, deal_sheet, fund_report"},
# MAGIC         "entity_name": {"type": "string", "description": "Primary company, fund, or issuer name"},
# MAGIC         "asset_class": {"type": "string", "description": "Asset class: equity, fixed_income, private_equity, infrastructure, multi_asset"},
# MAGIC         "key_financial_metrics": {
# MAGIC           "type": "array",
# MAGIC           "description": "Top 5 financial metrics with names and values",
# MAGIC           "items": {
# MAGIC             "type": "object",
# MAGIC             "properties": {
# MAGIC               "metric_name": {"type": "string"},
# MAGIC               "metric_value": {"type": "string"}
# MAGIC             }
# MAGIC           }
# MAGIC         },
# MAGIC         "recommendation": {"type": "string", "description": "Investment recommendation, outlook, or forward guidance. For earnings reports use the management guidance or outlook statement. For fund memos use the target return and investment conviction. For credit research use the analyst recommendation. For deal sheets use the base case return expectation. For fund reports use the performance vs benchmark assessment. Always provide a value."},
# MAGIC         "risk_factors": {"type": "array", "description": "Key risk factors", "items": {"type": "string"}},
# MAGIC         "geography": {"type": "string", "description": "Primary geographic focus"},
# MAGIC         "one_line_summary": {"type": "string", "description": "One-sentence summary of the document"}
# MAGIC       }',
# MAGIC       options => map('version', '2.0')
# MAGIC     ) AS extracted
# MAGIC   FROM parsed_docs
# MAGIC   WHERE parsed IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   source_file,
# MAGIC   document_name,
# MAGIC   extracted:response:document_type::STRING AS document_type,
# MAGIC   extracted:response:entity_name::STRING AS entity_name,
# MAGIC   extracted:response:asset_class::STRING AS asset_class,
# MAGIC   extracted:response:recommendation::STRING AS recommendation,
# MAGIC   extracted:response:geography::STRING AS geography,
# MAGIC   extracted:response:one_line_summary::STRING AS summary,
# MAGIC   extracted:response:key_financial_metrics AS key_metrics,
# MAGIC   extracted:response:risk_factors AS risk_factors,
# MAGIC   current_timestamp() AS extracted_at
# MAGIC FROM extracted

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the structured table — this is now queryable by Genie, dashboards, and downstream models
# MAGIC SELECT
# MAGIC   document_name,
# MAGIC   document_type,
# MAGIC   entity_name,
# MAGIC   asset_class,
# MAGIC   recommendation,
# MAGIC   geography,
# MAGIC   summary
# MAGIC FROM lakedoom_demo_catalog.capital_markets_ai.extracted_portfolio_documents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What we demonstrated:
# MAGIC
# MAGIC | Step | Function | What it does |
# MAGIC |------|----------|-------------|
# MAGIC | **Parse** | `ai_parse_document()` | Converts raw PDF bytes into structured text — handles tables, headers, multi-column layouts |
# MAGIC | **Extract** | `ai_extract()` | Pulls specific fields into typed JSON using a schema you define — no training, no fine-tuning |
# MAGIC | **Scale** | SQL + `read_files()` | Process an entire document library in a single query |
# MAGIC | **Govern** | Delta + Unity Catalog | Persist extracted data as governed tables with lineage, access controls, and audit |
# MAGIC
# MAGIC ### Capital Markets Applications:
# MAGIC - **Earnings processing** — Auto-extract revenue, EPS, margins, guidance from 10-Qs/10-Ks at scale
# MAGIC - **Fund due diligence** — Parse investment memos, PPMs, and side letters to extract terms, fees, and track records
# MAGIC - **Credit surveillance** — Extract ratings, spreads, and economic indicators from research notes
# MAGIC - **Deal pipeline** — Structure deal sheets and CIMs into comparable datasets for investment committee
# MAGIC - **ESG compliance** — Pull ESG scores, carbon metrics, and SDG alignment from fund reports
# MAGIC
# MAGIC ### What's next:
# MAGIC - Schedule this as a **Databricks Workflow** to run whenever new documents land in the Volume
# MAGIC - Build an **AI/BI Dashboard** on the extracted table for portfolio-level document intelligence
# MAGIC - Connect **Genie** so analysts can ask: *"What was TechNova's revenue guidance for FY2026?"*
# MAGIC - Feed extracted data into **Mosaic AI** for portfolio risk models and signal generation