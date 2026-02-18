import re
import math
import json
import uuid
import asyncio
import logging
import time
import os
import random
import socket
import threading
import base64
import subprocess
from datetime import datetime

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import Response
from playwright.async_api import async_playwright
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import redis
from google.cloud import bigquery, storage
import vertexai
from vertexai.generative_models import GenerativeModel

# ==================== 설정 ====================
PROJECT_ID = "chillgram-deploy"
DATASET_ID = "raw"
TABLE_ID = "crawl_events"
GCS_BUCKET = "chillgram-deploy-analysis-pdfs"
VERTEX_LOCATION = "us-central1"

# 프록시 설정
PROXY_HOST = "kr.decodo.com"
PROXY_PORT = 10001
PROXY_USER = "user-spjhm4tfwt-sessionduration-30"
PROXY_PASS = "7bznkJZ07~y5xIkleP"
LOCAL_PROXY_PORT = 18080  # 8080은 uvicorn이 사용하므로 다른 포트

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

# Redis 연결 (연결 실패 시 graceful fallback)
try:
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    redis_client.ping()
    logger.info("Redis 연결 성공")
except Exception:
    redis_client = None
    logger.warning("Redis 연결 실패 — 인메모리 캐시로 대체합니다")

# Redis 대체용 인메모리 캐시
_memory_cache: dict[str, dict] = {}


# ==================== 유틸리티 ====================
def extract_product_id(coupang_url: str) -> str | None:
    """쿠팡 URL에서 product_id를 추출합니다."""
    match = re.search(r"/products/(\d+)", coupang_url)
    return match.group(1) if match else None


# ==================== Redis / 캐시 ====================
REFRESH_DAYS = 90  # 이 기간이 지나면 다시 크롤링

def check_duplicate(product_id: str) -> bool:
    """중복 체크 — 90일 이상 지난 데이터는 만료 처리."""
    status = get_status(product_id)
    if not status:
        return False

    # 진행 중인 작업이 있으면 중복으로 처리
    if status.get("status") in ("queued", "crawling", "saving", "analyzing", "generating_pdf"):
        return True

    # done 상태인데 90일 지났으면 만료 → 다시 크롤링
    if status.get("status") == "done":
        updated_at = status.get("updated_at", "")
        try:
            last_update = datetime.fromisoformat(updated_at)
            if (datetime.now() - last_update).days >= REFRESH_DAYS:
                logger.info(f"[REFRESH] {product_id}: {REFRESH_DAYS}일 경과 → 재크롤링")
                return False
        except (ValueError, TypeError):
            pass
        return True

    # error 상태면 재시도 허용
    if status.get("status") == "error":
        return False

    return True


def set_status(product_id: str, status: str, **extra):
    key = f"coupang:{product_id}:status"
    value = {"status": status, "updated_at": datetime.now().isoformat(), **extra}
    if redis_client:
        redis_client.set(key, json.dumps(value))
    else:
        _memory_cache[key] = value


def get_status(product_id: str) -> dict | None:
    key = f"coupang:{product_id}:status"
    if redis_client:
        raw = redis_client.get(key)
        return json.loads(raw) if raw else None
    return _memory_cache.get(key)


# ==================== BigQuery 저장 ====================
def save_to_bigquery(product_id: str, product_name: str, reviews: list[dict]):
    """크롤링한 리뷰를 BigQuery에 적재합니다."""
    logger.info(f"BigQuery 적재 시작: {len(reviews)}개 리뷰")
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    rows = []
    for r in reviews:
        data_json = json.dumps({
            "product_id": product_id,
            "product_name": product_name,
            "review": r.get("content", ""),
            "review_date": r.get("date", ""),
        }, ensure_ascii=False)
        rows.append({
            "data": data_json,
            "message_id": str(uuid.uuid4()),
            "publish_time": datetime.utcnow().isoformat(),
            "attributes": None,
            "subscription_name": "api_upload",
        })

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logger.error(f"BigQuery 적재 오류: {errors}")
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    logger.info(f"BigQuery 적재 완료: {len(rows)}건")


def fetch_reviews_from_bigquery(product_id: str) -> tuple[str, list[str]]:
    """BigQuery에서 해당 product_id의 리뷰를 조회합니다."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT data
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE JSON_EXTRACT_SCALAR(data, '$.product_id') = @product_id
        ORDER BY publish_time DESC
        LIMIT 1000
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("product_id", "STRING", product_id)
        ]
    )
    rows = client.query(query, job_config=job_config).result()

    product_name = ""
    review_texts = []
    for row in rows:
        try:
            data = json.loads(row.data)
            if not product_name:
                product_name = data.get("product_name", "")
            review_texts.append(data.get("review", ""))
        except (json.JSONDecodeError, TypeError):
            continue

    logger.info(f"BigQuery 조회: {len(review_texts)}개 리뷰 (product_id={product_id})")
    return product_name, review_texts


# ==================== Vertex AI 분석 ====================
def analyze_with_vertex(product_name: str, review_texts: list[str]) -> dict:
    """Vertex AI Gemini로 리뷰를 분석합니다."""
    logger.info(f"Vertex AI 분석 시작: {len(review_texts)}개 리뷰")

    vertexai.init(project=PROJECT_ID, location=VERTEX_LOCATION)
    model = GenerativeModel("gemini-2.5-flash")

    max_for_analysis = min(200, len(review_texts))
    sample = review_texts[:max_for_analysis]
    combined = "\n---\n".join(sample)

    prompt = f"""
당신은 전문 데이터 분석가입니다. 다음 "{product_name}" 제품 리뷰 {max_for_analysis}개를 종합 분석해주세요.

## 출력 형식: 반드시 아래 JSON 구조로만 출력하세요. 다른 텍스트 없이 JSON만 출력하세요.

{{
  "product_name": "{product_name}",
  "sentiment_analysis": {{
    "overall_sentiment_distribution": {{
      "positive": "00%",
      "neutral": "00%",
      "negative": "00%"
    }},
    "overall_sentiment_score": "0.0/10",
    "sentiment_trend_summary": "요약 문장"
  }},
  "keyword_analysis": {{
    "top_10_positive_keywords": [
      {{"keyword": "키워드", "frequency": 0}}
    ],
    "top_10_negative_keywords": [
      {{"keyword": "키워드", "frequency": 0}}
    ]
  }},
  "review_summary": {{
    "three_line_summary": ["요약1", "요약2", "요약3"],
    "representative_positive_reviews": ["리뷰1", "리뷰2", "리뷰3"],
    "representative_negative_reviews": ["리뷰1", "리뷰2", "리뷰3"]
  }},
  "insights": {{
    "top_3_customer_satisfaction_points": ["포인트1", "포인트2", "포인트3"],
    "top_3_customer_dissatisfaction_points": ["포인트1", "포인트2", "포인트3"],
    "improvement_suggestions": ["제안1", "제안2", "제안3"],
    "top_3_marketing_leverage_points": ["포인트1", "포인트2", "포인트3"]
  }},
  "action_items": {{
    "immediate_improvements": ["항목1", "항목2"],
    "marketing_messages": ["메시지1", "메시지2"],
    "competitive_advantages": ["강점1", "강점2"]
  }},
  "monthly_sentiment_trend": [
    {{"month": "2025-01", "positive": 70, "neutral": 20, "negative": 10}},
    {{"month": "2025-02", "positive": 65, "neutral": 25, "negative": 10}}
  ]
}}

## 리뷰 데이터
{combined}
"""

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.3,
                    "max_output_tokens": 16384,
                    "response_mime_type": "application/json",
                },
            )

            raw_text = response.text.strip()

            # JSON 파싱 (여러 방법 시도)
            analysis = None

            # 1차: 그대로 파싱
            try:
                analysis = json.loads(raw_text)
            except json.JSONDecodeError:
                pass

            # 2차: ```json ... ``` 블록 추출 후 파싱
            if analysis is None:
                json_match = re.search(r"```json\s*([\s\S]*?)```", raw_text)
                if json_match:
                    try:
                        analysis = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass

            # 3차: { } 블록 추출 후 파싱
            if analysis is None:
                json_match = re.search(r"\{[\s\S]*\}", raw_text)
                if json_match:
                    try:
                        analysis = json.loads(json_match.group())
                    except json.JSONDecodeError:
                        pass

            if analysis is not None:
                logger.info("Vertex AI 분석 완료")
                return analysis

            logger.warning(f"[시도 {attempt}/{max_retries}] JSON 파싱 실패: {raw_text[:200]}")

        except Exception as e:
            logger.warning(f"[시도 {attempt}/{max_retries}] Gemini 호출 실패: {e}")

        if attempt < max_retries:
            time.sleep(3)

    raise ValueError("Gemini 응답에서 JSON을 파싱할 수 없습니다 (3회 시도 실패)")


# ==================== 대시보드 HTML 생성 ====================
def generate_dashboard_html(analysis: dict) -> str:
    """분석 결과로 동적 HTML 대시보드를 생성합니다."""
    product_name = analysis.get("product_name", "제품")
    sentiment = analysis.get("sentiment_analysis", {})
    keywords = analysis.get("keyword_analysis", {})
    insights = analysis.get("insights", {})
    review_summary = analysis.get("review_summary", {})
    action_items = analysis.get("action_items", {})

    distribution = sentiment.get("overall_sentiment_distribution", {})
    positive = int(distribution.get("positive", "0%").replace("%", ""))
    neutral = int(distribution.get("neutral", "0%").replace("%", ""))
    negative = int(distribution.get("negative", "0%").replace("%", ""))
    score = sentiment.get("overall_sentiment_score", "0/10")
    score_num = float(score.split("/")[0])
    score_class = "score-high" if score_num >= 8 else "score-medium" if score_num >= 6 else "score-low"

    pos_keywords = keywords.get("top_10_positive_keywords", [])
    neg_keywords = keywords.get("top_10_negative_keywords", [])
    pos_kw_names = [k["keyword"] for k in pos_keywords]
    pos_kw_freq = [k["frequency"] for k in pos_keywords]
    neg_kw_names = [k["keyword"] for k in neg_keywords]
    neg_kw_freq = [k["frequency"] for k in neg_keywords]

    # 인사이트 리스트
    satisfaction = insights.get("top_3_customer_satisfaction_points", [])
    dissatisfaction = insights.get("top_3_customer_dissatisfaction_points", [])
    marketing = insights.get("top_3_marketing_leverage_points", [])
    improvements = insights.get("improvement_suggestions", [])

    # 리뷰 요약
    summary_lines = review_summary.get("three_line_summary", [])
    pos_reviews = review_summary.get("representative_positive_reviews", [])
    neg_reviews = review_summary.get("representative_negative_reviews", [])

    # 액션 아이템
    immediate = action_items.get("immediate_improvements", [])
    marketing_msg = action_items.get("marketing_messages", [])
    advantages = action_items.get("competitive_advantages", [])

    def li_items(items):
        return "\n".join(f"<li>{item}</li>" for item in items)

    def blockquote_items(items):
        return "\n".join(f'<blockquote>"{item}"</blockquote>' for item in items)

    # 리뷰 수 (분석에 포함된 리뷰 수)
    review_count = len(pos_reviews) + len(neg_reviews)
    total_keywords = sum(pos_kw_freq) + sum(neg_kw_freq)

    # 월별 감성 트렌드 데이터
    monthly_trend = analysis.get("monthly_sentiment_trend", [])
    trend_months = [t.get("month", "") for t in monthly_trend]
    trend_pos = [t.get("positive", 0) for t in monthly_trend]
    trend_neu = [t.get("neutral", 0) for t in monthly_trend]
    trend_neg = [t.get("negative", 0) for t in monthly_trend]

    # 워드클라우드 HTML 생성 (긍정 + 부정 키워드 합쳐서)
    wc_words = []
    all_freq = pos_kw_freq + neg_kw_freq
    max_freq = max(all_freq) if all_freq else 1
    pos_colors = ["#10B981", "#34D399", "#6EE7B7", "#A7F3D0"]
    neg_colors = ["#EF4444", "#F87171", "#FCA5A5", "#FECACA"]
    for name, freq in zip(pos_kw_names, pos_kw_freq):
        size = max(0.75, min(2.2, 0.75 + (freq / max_freq) * 1.45))
        color = random.choice(pos_colors)
        wc_words.append(f'<span class="wc-word" style="font-size:{size:.2f}rem;color:{color};opacity:{0.7 + 0.3 * freq / max_freq:.2f}">{name}</span>')
    for name, freq in zip(neg_kw_names, neg_kw_freq):
        size = max(0.75, min(2.2, 0.75 + (freq / max_freq) * 1.45))
        color = random.choice(neg_colors)
        wc_words.append(f'<span class="wc-word" style="font-size:{size:.2f}rem;color:{color};opacity:{0.7 + 0.3 * freq / max_freq:.2f}">{name}</span>')
    random.shuffle(wc_words)
    wordcloud_html = "\n".join(wc_words)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{product_name} - 리뷰 분석 리포트</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap" rel="stylesheet">
    <style>
        @page {{ size: A4 landscape; margin: 0; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        html, body {{
            font-family: 'Noto Sans KR', sans-serif;
            background: #1a1a2e;
            width: 1122px;
            height: 2379px;
            color: #fff;
            overflow: hidden;
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
        }}
        .page {{
            width: 1122px; height: 793px;
            padding: 20px 24px;
            overflow: hidden;
            page-break-after: always;
            display: flex; flex-direction: column;
        }}
        .page:last-child {{ page-break-after: avoid; }}

        /* 헤더 */
        .header {{ text-align: center; padding: 10px 0 8px; }}
        .header h1 {{
            font-size: 1.5rem; font-weight: 700;
            background: linear-gradient(90deg, #6366F1, #8B5CF6, #EC4899);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }}
        .header p {{ color: #94A3B8; font-size: 0.75rem; margin-top: 2px; }}

        /* 상품명 + 감성점수 바 */
        .product-bar {{
            display: flex; align-items: center; gap: 12px;
            padding: 8px 0 6px;
            border-bottom: 2px solid rgba(99,102,241,0.3);
            margin-bottom: 10px;
        }}
        .product-bar h2 {{ font-size: 1.05rem; font-weight: 600; }}
        .score-badge {{
            display: inline-block; padding: 4px 14px; border-radius: 16px;
            font-weight: 600; font-size: 0.85rem;
        }}
        .score-high {{ background: linear-gradient(135deg, #10B981, #059669); }}
        .score-medium {{ background: linear-gradient(135deg, #F59E0B, #D97706); }}
        .score-low {{ background: linear-gradient(135deg, #EF4444, #DC2626); }}

        /* 차트 3열 그리드 */
        .charts-row {{
            display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px;
            flex: 1; min-height: 0;
        }}
        .chart-box {{
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px; padding: 10px;
            display: flex; flex-direction: column;
        }}
        .chart-label {{
            font-size: 0.8rem; font-weight: 500; color: #E2E8F0;
            margin-bottom: 4px; display: flex; align-items: center; gap: 6px;
        }}
        .chart-inner {{ flex: 1; min-height: 0; }}

        /* 인사이트 3열 */
        .insights-row {{
            display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px;
            margin-top: 10px;
        }}
        .insight-card {{
            background: rgba(99,102,241,0.08); border: 1px solid rgba(99,102,241,0.2);
            border-radius: 10px; padding: 10px;
        }}
        .insight-card.purple {{ background: rgba(99,102,241,0.1); border-color: rgba(99,102,241,0.3); }}
        .insight-card.green {{ background: rgba(16,185,129,0.08); border-color: rgba(16,185,129,0.3); }}
        .insight-card.red {{ background: rgba(239,68,68,0.08); border-color: rgba(239,68,68,0.3); }}
        .insight-label {{
            font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.5px; margin-bottom: 6px;
        }}
        .insight-card.purple .insight-label {{ color: #A5B4FC; }}
        .insight-card.green .insight-label {{ color: #10B981; }}
        .insight-card.red .insight-label {{ color: #EF4444; }}
        ul {{ list-style: none; }}
        ul li {{
            padding: 3px 0; font-size: 0.72rem; color: #CBD5E1; line-height: 1.4;
            border-bottom: 1px solid rgba(255,255,255,0.04);
        }}
        ul li:last-child {{ border-bottom: none; }}
        ul li::before {{ content: "→ "; color: #6366F1; }}

        .footer {{
            text-align: center; padding: 6px 0 0; color: #64748B; font-size: 0.6rem;
        }}

        /* ===== PAGE 2 ===== */
        .page2-header {{
            text-align: center; padding: 10px 0 8px;
            border-bottom: 2px solid rgba(99,102,241,0.3);
            margin-bottom: 12px;
        }}
        .page2-header h1 {{
            font-size: 1.3rem; font-weight: 700;
            background: linear-gradient(90deg, #6366F1, #8B5CF6, #EC4899);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }}
        .page2-header p {{ color: #94A3B8; font-size: 0.72rem; margin-top: 2px; }}

        .page2-content {{
            display: grid; grid-template-columns: 1fr 1fr; gap: 14px;
            flex: 1; min-height: 0;
        }}

        /* 워드클라우드 */
        .wordcloud-box {{
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px; padding: 14px;
            display: flex; flex-direction: column;
        }}
        .section-label {{
            font-size: 0.85rem; font-weight: 600; color: #E2E8F0;
            margin-bottom: 10px; display: flex; align-items: center; gap: 6px;
        }}
        .wordcloud {{
            flex: 1; display: flex; flex-wrap: wrap;
            align-items: center; justify-content: center;
            gap: 6px 10px; padding: 8px;
        }}
        .wc-word {{
            display: inline-block; padding: 2px 6px;
            border-radius: 4px; line-height: 1.3;
            transition: opacity 0.2s;
        }}

        /* 시계열 트렌드 */
        .trend-box {{
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px; padding: 14px;
            display: flex; flex-direction: column;
        }}
        .trend-chart {{ flex: 1; min-height: 0; }}

        /* 하단 요약 카드 */
        .page2-bottom {{
            display: grid; grid-template-columns: 1fr 1fr; gap: 14px;
            margin-top: 12px;
        }}
        .summary-card {{
            background: rgba(99,102,241,0.06); border: 1px solid rgba(99,102,241,0.15);
            border-radius: 10px; padding: 12px;
        }}
        .summary-card h3 {{
            font-size: 0.78rem; font-weight: 600; color: #A5B4FC;
            margin-bottom: 6px;
        }}
        blockquote {{
            font-size: 0.7rem; color: #CBD5E1; line-height: 1.5;
            padding: 4px 0 4px 10px; margin: 3px 0;
            border-left: 2px solid rgba(99,102,241,0.4);
            font-style: italic;
        }}

        /* ===== PAGE 3 ===== */
        .page3-summary {{
            background: rgba(99,102,241,0.06); border: 1px solid rgba(99,102,241,0.15);
            border-radius: 10px; padding: 14px; margin-bottom: 12px;
        }}
        .page3-summary h3 {{
            font-size: 0.85rem; font-weight: 600; color: #A5B4FC;
            margin-bottom: 8px;
        }}
        .page3-summary ol {{
            list-style: none; counter-reset: summary;
        }}
        .page3-summary ol li {{
            counter-increment: summary;
            padding: 5px 0; font-size: 0.78rem; color: #E2E8F0; line-height: 1.5;
            border-bottom: 1px solid rgba(255,255,255,0.04);
        }}
        .page3-summary ol li:last-child {{ border-bottom: none; }}
        .page3-summary ol li::before {{
            content: counter(summary) ". ";
            color: #8B5CF6; font-weight: 600;
        }}

        .page3-grid {{
            display: grid; grid-template-columns: 1fr 1fr; gap: 14px;
            flex: 1; min-height: 0;
        }}
        .action-card {{
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
            border-radius: 10px; padding: 14px;
            display: flex; flex-direction: column;
        }}
        .action-card h3 {{
            font-size: 0.8rem; font-weight: 600; margin-bottom: 8px;
            display: flex; align-items: center; gap: 6px;
        }}
        .action-card.urgent h3 {{ color: #F87171; }}
        .action-card.improve h3 {{ color: #FBBF24; }}
        .action-card.message h3 {{ color: #34D399; }}
        .action-card.strength h3 {{ color: #60A5FA; }}
        .action-card ul li::before {{ content: "→ "; }}
        .action-card.urgent ul li::before {{ color: #EF4444; }}
        .action-card.improve ul li::before {{ color: #F59E0B; }}
        .action-card.message ul li::before {{ color: #10B981; }}
        .action-card.strength ul li::before {{ color: #3B82F6; }}
    </style>
</head>
<body>
<!-- ===== PAGE 1 ===== -->
<div class="page">
    <div class="header">
        <h1>리뷰 분석 대시보드</h1>
        <p>BigQuery + Vertex AI Gemini 2.5 Flash 분석 결과</p>
    </div>

    <div class="product-bar">
        <h2>{product_name}</h2>
        <span class="score-badge {score_class}">감성 점수: {score}</span>
    </div>

    <!-- 차트 3열: 감성분포 | 긍정키워드 | 부정키워드 -->
    <div class="charts-row">
        <div class="chart-box">
            <div class="chart-label">감성 분포</div>
            <div class="chart-inner" id="sentiment-chart"></div>
        </div>
        <div class="chart-box">
            <div class="chart-label">긍정 키워드 TOP 10</div>
            <div class="chart-inner" id="pos-keywords"></div>
        </div>
        <div class="chart-box">
            <div class="chart-label">부정 키워드 TOP 10</div>
            <div class="chart-inner" id="neg-keywords"></div>
        </div>
    </div>

    <!-- 인사이트 3열 -->
    <div class="insights-row">
        <div class="insight-card purple">
            <div class="insight-label">마케팅 활용 포인트</div>
            <ul>{li_items(marketing)}</ul>
        </div>
        <div class="insight-card green">
            <div class="insight-label">고객 만족 포인트</div>
            <ul>{li_items(satisfaction)}</ul>
        </div>
        <div class="insight-card red">
            <div class="insight-label">개선 필요 사항</div>
            <ul>{li_items(dissatisfaction)}</ul>
        </div>
    </div>

    <div class="footer">
        <p>CHILLGRAM Review Analysis &middot; Page 1/3 &middot; Generated by Vertex AI</p>
    </div>
</div>

<!-- ===== PAGE 2 ===== -->
<div class="page">
    <div class="page2-header">
        <h1>심층 분석 리포트</h1>
        <p>{product_name} &middot; 키워드 워드클라우드 &amp; 감성 트렌드</p>
    </div>

    <div class="page2-content">
        <div class="wordcloud-box">
            <div class="section-label">키워드 워드클라우드</div>
            <div class="wordcloud">
                {wordcloud_html}
            </div>
            <div style="text-align:center;font-size:0.6rem;color:#64748B;margin-top:4px;">
                <span style="color:#10B981;">● 긍정</span> &nbsp;
                <span style="color:#EF4444;">● 부정</span> &nbsp;
                글자 크기 = 빈도수
            </div>
        </div>
        <div class="trend-box">
            <div class="section-label">월별 감성 트렌드</div>
            <div class="trend-chart" id="trend-chart"></div>
        </div>
    </div>

    <div class="page2-bottom">
        <div class="summary-card">
            <h3>대표 긍정 리뷰</h3>
            {blockquote_items(pos_reviews[:3])}
        </div>
        <div class="summary-card">
            <h3>대표 부정 리뷰</h3>
            {blockquote_items(neg_reviews[:3])}
        </div>
    </div>

    <div class="footer">
        <p>CHILLGRAM Review Analysis &middot; Page 2/3 &middot; Generated by Vertex AI</p>
    </div>
</div>

<!-- ===== PAGE 3 ===== -->
<div class="page">
    <div class="page2-header">
        <h1>액션 플랜 & 전략 리포트</h1>
        <p>{product_name} &middot; 리뷰 기반 실행 전략</p>
    </div>

    <div class="page3-summary">
        <h3>리뷰 3줄 요약</h3>
        <ol>{li_items(summary_lines)}</ol>
    </div>

    <div class="page3-grid">
        <div class="action-card urgent">
            <h3>즉시 개선 항목</h3>
            <ul>{li_items(immediate)}</ul>
        </div>
        <div class="action-card improve">
            <h3>개선 제안</h3>
            <ul>{li_items(improvements)}</ul>
        </div>
        <div class="action-card message">
            <h3>마케팅 메시지 제안</h3>
            <ul>{li_items(marketing_msg)}</ul>
        </div>
        <div class="action-card strength">
            <h3>경쟁 우위</h3>
            <ul>{li_items(advantages)}</ul>
        </div>
    </div>

    <div class="footer">
        <p>CHILLGRAM Review Analysis &middot; Page 3/3 &middot; Generated by Vertex AI</p>
    </div>
</div>

<script>
Plotly.newPlot('sentiment-chart', [{{
    values: [{positive}, {neutral}, {negative}],
    labels: ['긍정 ({positive}%)', '중립 ({neutral}%)', '부정 ({negative}%)'],
    type: 'pie', hole: 0.55,
    marker: {{ colors: ['#10B981', '#F59E0B', '#EF4444'] }},
    textinfo: 'label', textfont: {{ color: '#fff', size: 10 }},
    hoverinfo: 'label+percent'
}}], {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{ color: '#fff' }}, showlegend: false,
    margin: {{ t: 10, b: 10, l: 10, r: 10 }},
    annotations: [{{ text: '<b>{score}</b>', font: {{ size: 20, color: '#fff' }}, showarrow: false }}]
}}, {{responsive: true, displayModeBar: false}});

Plotly.newPlot('pos-keywords', [{{
    x: {json.dumps(pos_kw_freq[::-1])},
    y: {json.dumps(pos_kw_names[::-1], ensure_ascii=False)},
    type: 'bar', orientation: 'h',
    marker: {{ color: 'rgba(16,185,129,0.8)', line: {{ color: '#10B981', width: 1 }} }},
    text: {json.dumps(pos_kw_freq[::-1])}, textposition: 'outside',
    textfont: {{ color: '#10B981', size: 9 }}
}}], {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{ color: '#CBD5E1' }},
    xaxis: {{ showgrid: false, zeroline: false, showticklabels: false }},
    yaxis: {{ showgrid: false, tickfont: {{ size: 9 }} }},
    margin: {{ t: 5, b: 5, l: 55, r: 30 }}, bargap: 0.2
}}, {{responsive: true, displayModeBar: false}});

Plotly.newPlot('neg-keywords', [{{
    x: {json.dumps(neg_kw_freq[::-1])},
    y: {json.dumps(neg_kw_names[::-1], ensure_ascii=False)},
    type: 'bar', orientation: 'h',
    marker: {{ color: 'rgba(239,68,68,0.8)', line: {{ color: '#EF4444', width: 1 }} }},
    text: {json.dumps(neg_kw_freq[::-1])}, textposition: 'outside',
    textfont: {{ color: '#EF4444', size: 9 }}
}}], {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{ color: '#CBD5E1' }},
    xaxis: {{ showgrid: false, zeroline: false, showticklabels: false }},
    yaxis: {{ showgrid: false, tickfont: {{ size: 9 }} }},
    margin: {{ t: 5, b: 5, l: 55, r: 30 }}, bargap: 0.2
}}, {{responsive: true, displayModeBar: false}});

// 시계열 트렌드 차트
Plotly.newPlot('trend-chart', [
    {{
        x: {json.dumps(trend_months)},
        y: {json.dumps(trend_pos)},
        name: '긍정', type: 'scatter', mode: 'lines+markers',
        line: {{ color: '#10B981', width: 2.5 }},
        marker: {{ size: 6, color: '#10B981' }},
        fill: 'tozeroy', fillcolor: 'rgba(16,185,129,0.1)'
    }},
    {{
        x: {json.dumps(trend_months)},
        y: {json.dumps(trend_neu)},
        name: '중립', type: 'scatter', mode: 'lines+markers',
        line: {{ color: '#F59E0B', width: 2 }},
        marker: {{ size: 5, color: '#F59E0B' }}
    }},
    {{
        x: {json.dumps(trend_months)},
        y: {json.dumps(trend_neg)},
        name: '부정', type: 'scatter', mode: 'lines+markers',
        line: {{ color: '#EF4444', width: 2 }},
        marker: {{ size: 5, color: '#EF4444' }}
    }}
], {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{ color: '#CBD5E1', size: 10 }},
    legend: {{ orientation: 'h', y: 1.08, x: 0.5, xanchor: 'center', font: {{ size: 10 }} }},
    xaxis: {{
        showgrid: false, color: '#64748B',
        tickfont: {{ size: 9 }}, tickangle: -30
    }},
    yaxis: {{
        showgrid: true, gridcolor: 'rgba(255,255,255,0.05)',
        color: '#64748B', ticksuffix: '%', tickfont: {{ size: 9 }},
        range: [0, 100]
    }},
    margin: {{ t: 30, b: 40, l: 40, r: 15 }}
}}, {{responsive: true, displayModeBar: false}});
</script>
</body>
</html>"""
    return html


# ==================== HTML → PDF (Playwright) ====================
async def html_to_pdf(html_content: str) -> bytes:
    """Playwright로 HTML을 PDF로 변환합니다."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(html_content, wait_until="networkidle")
        # Plotly 차트 렌더링 대기
        await page.wait_for_timeout(3000)
        pdf_bytes = await page.pdf(
            landscape=True,
            format="A4",
            print_background=True,
            margin={"top": "0", "bottom": "0", "left": "0", "right": "0"},
        )
        await browser.close()
    return pdf_bytes


# ==================== GCS 저장/조회 ====================
def save_pdf_to_gcs(product_id: str, pdf_bytes: bytes) -> str:
    """PDF를 GCS에 저장하고 경로를 반환합니다."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"pdfs/{product_id}/latest.pdf"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    logger.info(f"GCS 저장 완료: gs://{GCS_BUCKET}/{blob_path}")
    return blob_path


def get_pdf_from_gcs(product_id: str) -> bytes | None:
    """GCS에서 최신 PDF를 다운로드합니다."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"pdfs/{product_id}/latest.pdf")
    if not blob.exists():
        return None
    logger.info(f"GCS 조회: {blob.name}")
    return blob.download_as_bytes()


def save_analysis_to_gcs(product_id: str, analysis: dict) -> str:
    """분석 결과 JSON을 GCS에 저장합니다."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"analysis/{product_id}/latest.json"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(analysis, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    logger.info(f"분석 JSON 저장 완료: gs://{GCS_BUCKET}/{blob_path}")
    return blob_path


def get_analysis_from_gcs(product_id: str) -> dict | None:
    """GCS에서 최신 분석 JSON을 가져옵니다."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"analysis/{product_id}/latest.json")
    if not blob.exists():
        return None
    logger.info(f"분석 JSON 조회: {blob.name}")
    return json.loads(blob.download_as_string())


# ==================== 프록시 & Xvfb (run_crawler_test.py 방식) ====================
def setup_xvfb(display=":99"):
    os.environ["DISPLAY"] = display
    xvfb_proc = subprocess.Popen(
        ["Xvfb", display, "-screen", "0", "1920x1080x24", "-nolisten", "tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    logger.info(f"Xvfb 가상 디스플레이 시작됨 (DISPLAY={display})")
    return xvfb_proc


def _pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


def _handle_client(client_sock):
    try:
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client_sock.recv(4096)
            if not chunk:
                client_sock.close()
                return
            request += chunk

        first_line = request.split(b"\r\n")[0].decode("utf-8", errors="replace")
        method, target, _ = first_line.split(" ", 2)

        upstream = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=15)
        auth = base64.b64encode(f"{PROXY_USER}:{PROXY_PASS}".encode()).decode()

        if method == "CONNECT":
            connect_req = f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Authorization: Basic {auth}\r\nConnection: keep-alive\r\n\r\n"
            upstream.sendall(connect_req.encode())

            response = b""
            while b"\r\n\r\n" not in response:
                chunk = upstream.recv(4096)
                if not chunk:
                    break
                response += chunk

            if b"200" in response.split(b"\r\n")[0]:
                client_sock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                t1 = threading.Thread(target=_pipe, args=(client_sock, upstream), daemon=True)
                t2 = threading.Thread(target=_pipe, args=(upstream, client_sock), daemon=True)
                t1.start()
                t2.start()
                t1.join()
                t2.join()
            else:
                client_sock.sendall(response)
                client_sock.close()
                upstream.close()
        else:
            header_end = request.find(b"\r\n\r\n")
            headers_part = request[:header_end]
            body_part = request[header_end:]
            auth_header = f"Proxy-Authorization: Basic {auth}\r\n".encode()
            first_line_end = headers_part.find(b"\r\n") + 2
            modified_request = headers_part[:first_line_end] + auth_header + headers_part[first_line_end:] + body_part
            upstream.sendall(modified_request)

            t1 = threading.Thread(target=_pipe, args=(client_sock, upstream), daemon=True)
            t2 = threading.Thread(target=_pipe, args=(upstream, client_sock), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

    except Exception:
        try:
            client_sock.close()
        except Exception:
            pass


_proxy_server = None


def start_local_proxy():
    global _proxy_server
    if _proxy_server is not None:
        return _proxy_server

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", LOCAL_PROXY_PORT))
    server.listen(50)

    def accept_loop():
        while True:
            try:
                client_sock, _ = server.accept()
                t = threading.Thread(target=_handle_client, args=(client_sock,), daemon=True)
                t.start()
            except Exception:
                break

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    time.sleep(1)
    logger.info(f"로컬 프록시 포워더 시작됨 (localhost:{LOCAL_PROXY_PORT} → {PROXY_HOST}:{PROXY_PORT})")
    _proxy_server = server
    return server


# ==================== 크롤링 (undetected_chromedriver + 프록시) ====================
def get_total_review_pages(driver, max_reviews):
    # 방법 1: 셀렉터로 리뷰 수 찾기
    selectors = [
        "#sdpReview span.count",
        "#sdpReview div.twc-text-cou-blue",
        "span.count",
        "div.twc-text-cou-blue",
    ]
    for selector in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in els:
                count_text = el.text.strip().replace(",", "").replace("(", "").replace(")", "")
                match = re.search(r"(\d+)", count_text)
                if match and int(match.group(1)) > 0:
                    total_reviews = int(match.group(1))
                    original_reviews = total_reviews
                    total_reviews = min(total_reviews, max_reviews)
                    total_pages = math.ceil(total_reviews / 10)
                    logger.info(f"총 리뷰 수: {original_reviews}, 크롤링 대상: {total_reviews}, 페이지: {total_pages}")
                    return total_pages
        except Exception:
            continue

    # 방법 2: 페이지 전체 텍스트에서 괄호 안 숫자 찾기
    try:
        review_section = driver.find_element(By.ID, "sdpReview")
        section_text = review_section.text
        match = re.search(r"\((\d[\d,]*)\)", section_text)
        if match:
            total_reviews = int(match.group(1).replace(",", ""))
            total_reviews = min(total_reviews, max_reviews)
            total_pages = math.ceil(total_reviews / 10)
            logger.info(f"총 리뷰 수 (텍스트 추출): {total_reviews}, 페이지: {total_pages}")
            return total_pages
    except Exception:
        pass

    # fallback: max_reviews 기준으로 시도
    fallback_pages = math.ceil(max_reviews / 10)
    logger.warning(f"리뷰 수를 찾을 수 없어 fallback: {fallback_pages}페이지 시도")
    return fallback_pages


def crawl_all_coupang_reviews(product_url, max_reviews=1000):
    all_reviews = []
    count = 0

    # Xvfb & 프록시 시작
    xvfb_proc = setup_xvfb()
    start_local_proxy()

    logger.info("Chrome 브라우저 시작 중 (undetected-chromedriver + Proxy)...")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--proxy-server=http://127.0.0.1:{LOCAL_PROXY_PORT}")
    options.add_argument("--lang=ko-KR")

    driver = uc.Chrome(options=options, headless=False, version_main=144)

    try:
        # 쿠팡 메인 페이지 먼저 방문 (쿠키 획득)
        logger.info("쿠팡 메인 페이지 방문 중 (쿠키 획득)...")
        driver.get("https://www.coupang.com")
        time.sleep(5)
        logger.info(f"메인 페이지 타이틀: {driver.title}")

        # 상품 페이지 접속
        logger.info(f"페이지 접속 시도: {product_url}")
        driver.get(product_url)
        time.sleep(5)

        logger.info(f"페이지 타이틀: {driver.title}")

        # Access Denied 체크
        if "Access Denied" in driver.title:
            logger.error("Access Denied - 페이지 접근이 차단되었습니다")
            return []

        # 리뷰 섹션 찾기
        try:
            review_section = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "sdpReview"))
            )
        except Exception:
            logger.error("리뷰 섹션을 찾을 수 없습니다")
            return []

        driver.execute_script("arguments[0].scrollIntoView();", review_section)
        logger.info("리뷰 섹션 발견")
        time.sleep(1)

        # 베스트순 정렬
        try:
            best_btn = driver.find_element(By.XPATH, "//button[contains(text(), '베스트순')]")
            if best_btn:
                best_btn.click()
                time.sleep(1)
                logger.info("베스트순 정렬 적용됨")
        except Exception:
            pass

        num_pages = get_total_review_pages(driver, max_reviews)

        if num_pages == 0:
            logger.error("페이지 수가 0입니다.")
            return []

        for page_num in range(1, num_pages + 1):
            logger.info(f"{page_num}/{num_pages} 페이지 크롤링 중")

            if page_num != 1:
                try:
                    page_btns = driver.find_elements(
                        By.XPATH, f"//*[@id='sdpReview']//button[span[text()='{page_num}']]"
                    )
                    if not page_btns:
                        # "다음(>)" 버튼 클릭 — 페이지네이션 그룹 넘기기
                        # 페이지네이션 버튼 목록의 마지막 버튼이 "다음" 버튼
                        nav_btns = driver.find_elements(
                            By.CSS_SELECTOR, "#sdpReview button.twc-rounded-\\[50\\%\\]"
                        )
                        if nav_btns:
                            next_btn = nav_btns[-1]
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                            time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(1)
                        page_btns = driver.find_elements(
                            By.XPATH, f"//*[@id='sdpReview']//button[span[text()='{page_num}']]"
                        )

                    if page_btns:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_btns[0])
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", page_btns[0])
                        time.sleep(0.5)
                    else:
                        logger.warning(f"페이지 {page_num} 버튼을 찾을 수 없어 종료")
                        break
                except Exception as e:
                    logger.warning(f"페이지 {page_num} 클릭 실패: {e}")
                    continue

            reviews = driver.find_elements(By.CSS_SELECTOR, "#sdpReview article")

            for review in reviews:
                try:
                    try:
                        product_el = review.find_element(By.CSS_SELECTOR, "div[class*='twc-line-clamp']")
                        product_name = product_el.text.strip()
                    except Exception:
                        product_name = ""

                    full_text = review.text or ""

                    if product_name and product_name in full_text:
                        idx = full_text.find(product_name) + len(product_name)
                        review_text = full_text[idx:].strip()

                        for suffix in ["도움이 돼요신고하기", "신고하기", "도움이 돼요"]:
                            if review_text.endswith(suffix):
                                review_text = review_text[:-len(suffix)].strip()

                        if "맛 만족도" in review_text:
                            review_text = review_text[:review_text.find("맛 만족도")].strip()
                    else:
                        review_text = ""

                    if review_text and len(review_text) > 5:
                        all_reviews.append({
                            "product": product_name,
                            "content": review_text,
                        })
                        count += 1
                except Exception:
                    continue

        logger.info(f"크롤링 완료: {count}개 리뷰")

    except Exception as e:
        logger.error(f"크롤링 중 오류: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            xvfb_proc.terminate()
        except Exception:
            pass

    return all_reviews


# ==================== 비동기 파이프라인 ====================
async def background_pipeline(product_id: str, coupang_url: str, max_reviews: int):
    """등록 시 백그라운드로 실행되는 전체 파이프라인."""
    try:
        # 1. 크롤링 (동기 함수 → asyncio.to_thread)
        set_status(product_id, "crawling")
        logger.info(f"[PIPELINE] 크롤링 시작: {product_id}")
        reviews = await asyncio.to_thread(crawl_all_coupang_reviews, coupang_url, max_reviews)

        if not reviews:
            set_status(product_id, "error", message="리뷰를 찾을 수 없습니다")
            return

        product_name = reviews[0].get("product", "Unknown")

        # 2. BigQuery 적재
        set_status(product_id, "saving")
        logger.info(f"[PIPELINE] BigQuery 적재: {product_id}")
        await asyncio.to_thread(save_to_bigquery, product_id, product_name, reviews)

        # 3. Vertex AI 분석
        set_status(product_id, "analyzing")
        logger.info(f"[PIPELINE] Vertex AI 분석: {product_id}")
        review_texts = [r["content"] for r in reviews]
        analysis = await asyncio.to_thread(analyze_with_vertex, product_name, review_texts)

        # 3.5. 분석 JSON을 GCS에 중간 저장 (PDF 실패 시 여기서부터 재시작 가능)
        logger.info(f"[PIPELINE] 분석 JSON 저장: {product_id}")
        analysis_path = await asyncio.to_thread(save_analysis_to_gcs, product_id, analysis)

        # 4. 대시보드 HTML 생성
        set_status(product_id, "generating_pdf")
        logger.info(f"[PIPELINE] PDF 생성: {product_id}")
        html = generate_dashboard_html(analysis)

        # 5. PDF 변환
        pdf_bytes = await html_to_pdf(html)

        # 6. GCS 저장
        pdf_path = await asyncio.to_thread(save_pdf_to_gcs, product_id, pdf_bytes)

        # 7. 완료
        set_status(product_id, "done", pdf_path=pdf_path, review_count=len(reviews))
        logger.info(f"[PIPELINE] 완료: {product_id} ({len(reviews)}개 리뷰)")

    except Exception as e:
        logger.error(f"[PIPELINE] 오류: {product_id} — {e}")
        set_status(product_id, "error", message=str(e))


# ==================== API 엔드포인트 ====================
@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/register")
async def register_product(request: Request, background_tasks: BackgroundTasks):
    """쿠팡 URL을 등록하고 비동기로 크롤링+분석+PDF 생성을 시작합니다."""
    body = await request.json()
    coupang_url = body.get("coupang_url", "")
    max_reviews = body.get("max_reviews", 1000)

    product_id = extract_product_id(coupang_url)
    if not product_id:
        return {"status": "error", "message": "유효하지 않은 쿠팡 URL입니다"}

    if check_duplicate(product_id):
        return {"status": "already_exists", "product_id": product_id}

    # 비동기 파이프라인 시작
    background_tasks.add_task(background_pipeline, product_id, coupang_url, max_reviews)
    set_status(product_id, "queued")

    return {"status": "crawling_started", "product_id": product_id}


@app.get("/status/{product_id}")
def get_product_status(product_id: str):
    """크롤링/분석 진행 상태를 확인합니다."""
    status = get_status(product_id)
    if not status:
        return {"status": "not_found", "product_id": product_id}
    return {"product_id": product_id, **status}


@app.post("/analyze")
async def analyze_product(request: Request):
    """분석 완료된 제품의 PDF를 반환합니다."""
    body = await request.json()
    product_id = body.get("product_id", "")

    if not product_id:
        return {"status": "error", "message": "product_id가 필요합니다"}

    # GCS에서 PDF 먼저 확인 (캐시 상태와 무관하게)
    pdf_bytes = await asyncio.to_thread(get_pdf_from_gcs, product_id)
    if pdf_bytes:
        # PDF가 있으면 바로 반환
        pass
    else:
        # PDF가 없으면 상태 확인
        status_info = get_status(product_id)
        if status_info:
            current_status = status_info.get("status", "")
            if current_status in ("queued", "crawling", "saving", "analyzing", "generating_pdf"):
                return {
                    "status": "processing",
                    "current_step": current_status,
                    "message": f"아직 처리 중입니다 (현재 단계: {current_status})",
                }
        return {"status": "error", "message": "PDF 파일을 찾을 수 없습니다"}

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="analysis_{product_id}.pdf"'},
    )


@app.post("/regenerate-pdf")
async def regenerate_pdf(request: Request):
    """저장된 분석 JSON으로 PDF만 재생성합니다. 크롤링/분석 없이 PDF만 다시 만듭니다."""
    body = await request.json()
    product_id = body.get("product_id", "")

    if not product_id:
        return {"status": "error", "message": "product_id가 필요합니다"}

    # GCS에서 분석 JSON 가져오기
    analysis = await asyncio.to_thread(get_analysis_from_gcs, product_id)
    if not analysis:
        return {"status": "error", "message": "저장된 분석 JSON이 없습니다. /reanalyze를 사용하세요."}

    try:
        set_status(product_id, "generating_pdf")
        html = generate_dashboard_html(analysis)
        pdf_bytes = await html_to_pdf(html)
        pdf_path = await asyncio.to_thread(save_pdf_to_gcs, product_id, pdf_bytes)
        set_status(product_id, "done", pdf_path=pdf_path)
        logger.info(f"[REGENERATE] PDF 재생성 완료: {product_id}")

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="analysis_{product_id}.pdf"'},
        )
    except Exception as e:
        set_status(product_id, "error", message=f"PDF 재생성 실패: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/reanalyze")
async def reanalyze_product(request: Request):
    """BigQuery에 저장된 리뷰로 Vertex AI 분석부터 재시작합니다. 크롤링 없이 분석+PDF를 다시 만듭니다."""
    body = await request.json()
    product_id = body.get("product_id", "")

    if not product_id:
        return {"status": "error", "message": "product_id가 필요합니다"}

    # BigQuery에서 리뷰 가져오기
    product_name, review_texts = await asyncio.to_thread(fetch_reviews_from_bigquery, product_id)
    if not review_texts:
        return {"status": "error", "message": "BigQuery에 리뷰 데이터가 없습니다. /register로 크롤링을 먼저 하세요."}

    try:
        # Vertex AI 분석
        set_status(product_id, "analyzing")
        analysis = await asyncio.to_thread(analyze_with_vertex, product_name, review_texts)

        # 분석 JSON 저장
        await asyncio.to_thread(save_analysis_to_gcs, product_id, analysis)

        # PDF 생성
        set_status(product_id, "generating_pdf")
        html = generate_dashboard_html(analysis)
        pdf_bytes = await html_to_pdf(html)
        pdf_path = await asyncio.to_thread(save_pdf_to_gcs, product_id, pdf_bytes)

        set_status(product_id, "done", pdf_path=pdf_path, review_count=len(review_texts))
        logger.info(f"[REANALYZE] 재분석 완료: {product_id}")

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="analysis_{product_id}.pdf"'},
        )
    except Exception as e:
        set_status(product_id, "error", message=f"재분석 실패: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/crawl")
async def crawl_reviews(request: Request):
    """기존 호환용 크롤링 전용 엔드포인트."""
    body = await request.json()
    product_id = body.get("product_id")
    max_reviews = body.get("max_reviews", 1000)

    if not product_id:
        return {"error": "product_id is required"}

    product_url = f"https://www.coupang.com/vp/products/{product_id}"
    reviews = await asyncio.to_thread(crawl_all_coupang_reviews, product_url, max_reviews)

    if not reviews:
        return {"error": "리뷰를 찾을 수 없습니다"}

    product_name = reviews[0]["product"]
    data = [{"review": r["content"], "ts": r["date"]} for r in reviews]

    return {"name": product_name, "data": data}
