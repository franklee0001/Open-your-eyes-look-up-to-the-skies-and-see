"""
휴라이트 마케팅 성과 리포트 생성기 (한국어)
"""

import os
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)
from google.ads.googleads.client import GoogleAdsClient


GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CREDENTIALS_PATH = ".secrets/ga4.json"
ADS_CREDENTIALS_PATH = ".secrets/google-ads.yaml"

CONVERSION_EVENTS = ["contact_form_submit", "email_click", "phone_calls", "wechat_call", "kakao_click"]
TARGET_COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "Australia", "Japan", "Singapore", "United Arab Emirates", "South Korea"]

# 국가명 한국어 매핑
COUNTRY_KR = {
    "United States": "미국", "South Korea": "대한민국", "Canada": "캐나다", "United Kingdom": "영국",
    "Germany": "독일", "France": "프랑스", "Italy": "이탈리아", "Spain": "스페인", "Netherlands": "네덜란드",
    "Belgium": "벨기에", "Australia": "호주", "Japan": "일본", "Singapore": "싱가포르",
    "United Arab Emirates": "아랍에미리트", "Nepal": "네팔", "India": "인도", "Philippines": "필리핀",
    "Nigeria": "나이지리아", "Poland": "폴란드", "Türkiye": "튀르키예", "China": "중국",
    "Hong Kong": "홍콩", "Taiwan": "대만", "Thailand": "태국", "Vietnam": "베트남",
    "Indonesia": "인도네시아", "Malaysia": "말레이시아", "Brazil": "브라질", "Mexico": "멕시코",
    "Russia": "러시아", "South Africa": "남아프리카", "Egypt": "이집트", "Morocco": "모로코",
    "Saudi Arabia": "사우디아라비아", "Israel": "이스라엘", "Ireland": "아일랜드", "Switzerland": "스위스",
    "Austria": "오스트리아", "Sweden": "스웨덴", "Norway": "노르웨이", "Denmark": "덴마크",
    "Finland": "핀란드", "Portugal": "포르투갈", "Greece": "그리스", "Czechia": "체코",
    "Romania": "루마니아", "Hungary": "헝가리", "Bulgaria": "불가리아", "Croatia": "크로아티아",
    "New Zealand": "뉴질랜드", "Argentina": "아르헨티나", "Chile": "칠레", "Colombia": "콜롬비아",
    "Peru": "페루", "Pakistan": "파키스탄", "Bangladesh": "방글라데시", "Sri Lanka": "스리랑카",
    "Qatar": "카타르", "Kuwait": "쿠웨이트", "Bahrain": "바레인", "Oman": "오만",
    "Martinique": "마르티니크", "Venezuela": "베네수엘라", "(not set)": "(미설정)",
}

# 채널명 한국어 매핑
CHANNEL_KR = {
    "Organic Search": "자연 검색", "Paid Search": "유료 검색", "Direct": "직접 유입",
    "Referral": "추천 유입", "Organic Social": "자연 소셜", "Paid Social": "유료 소셜",
    "Email": "이메일", "Display": "디스플레이", "Unassigned": "미분류", "(not set)": "(미설정)",
}

# 이벤트명 한국어 매핑
EVENT_KR = {
    "contact_form_submit": "문의 폼 제출", "email_click": "이메일 클릭",
    "phone_calls": "전화 문의", "wechat_call": "위챗 문의", "kakao_click": "카카오톡 클릭",
}


class GA4Client:
    def __init__(self, property_id: str):
        creds = service_account.Credentials.from_service_account_file(GA4_CREDENTIALS_PATH, scopes=GA4_SCOPES)
        self.client = BetaAnalyticsDataClient(credentials=creds)
        self.property_id = property_id
    
    def run_report(self, dimensions: list, metrics: list, start_date: str, end_date: str, limit=10000) -> list:
        dim_list = [Dimension(name=d) for d in dimensions] if dimensions else []
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dim_list,
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
        )
        response = self.client.run_report(request)
        
        rows = []
        for row in response.rows:
            data = {}
            for i, dim in enumerate(dimensions):
                data[dim] = row.dimension_values[i].value
            for i, met in enumerate(metrics):
                val = row.metric_values[i].value
                try:
                    data[met] = int(val)
                except ValueError:
                    try:
                        data[met] = float(val)
                    except ValueError:
                        data[met] = val
            rows.append(data)
        return rows


class AdsClient:
    def __init__(self, customer_id: str):
        self.client = GoogleAdsClient.load_from_storage(ADS_CREDENTIALS_PATH)
        self.customer_id = customer_id
        self.service = self.client.get_service("GoogleAdsService")
    
    def run_query(self, query: str) -> list:
        response = self.service.search(customer_id=self.customer_id, query=query)
        return list(response)


class ReportGenerator:
    def __init__(self, property_id: str, customer_id: str, start_date: str, end_date: str):
        self.ga4 = GA4Client(property_id)
        self.ads = AdsClient(customer_id)
        self.start_date = start_date
        self.end_date = end_date
        self.data = {}
    
    def collect_all_data(self):
        print("데이터 수집 중...")
        print("  - 핵심 지표...")
        self.data["summary"] = self._get_summary_data()
        print("  - 리드 현황...")
        self.data["leads"] = self._get_lead_data()
        print("  - 채널 성과...")
        self.data["channels"] = self._get_channel_data()
        print("  - 지역별 분포...")
        self.data["geo"] = self._get_geo_data()
        print("  - 캠페인 성과...")
        self.data["campaigns"] = self._get_campaign_data()
        print("  - 웹사이트 현황...")
        self.data["pages"] = self._get_page_data()
        print("  - 일별 추이...")
        self.data["daily"] = self._get_daily_trend()
        print("  - 이상 징후 탐지...")
        self.data["anomalies"] = self._detect_anomalies()
        print("데이터 수집 완료!")
        return self.data
    
    def _get_summary_data(self) -> dict:
        rows = self.ga4.run_report([], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        sessions = sum(r.get("sessions", 0) for r in rows)
        conversions = sum(r.get("conversions", 0) for r in rows)
        users = sum(r.get("totalUsers", 0) for r in rows)
        cvr = (conversions / sessions * 100) if sessions > 0 else 0
        
        query = f"SELECT metrics.cost_micros, metrics.clicks, metrics.conversions, metrics.impressions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        ads_rows = self.ads.run_query(query)
        total_cost = sum(r.metrics.cost_micros for r in ads_rows) / 1_000_000
        total_clicks = sum(r.metrics.clicks for r in ads_rows)
        total_impressions = sum(r.metrics.impressions for r in ads_rows)
        ads_conversions = sum(r.metrics.conversions for r in ads_rows)
        cpa = total_cost / conversions if conversions > 0 else 0
        ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        return {
            "sessions": sessions, "users": users, "conversions": int(conversions), 
            "cvr": round(cvr, 2), "total_cost": round(total_cost, 0), 
            "total_clicks": total_clicks, "total_impressions": total_impressions,
            "ads_conversions": round(ads_conversions, 1), "cpa": round(cpa, 0), "ctr": round(ctr, 2)
        }
    
    def _get_lead_data(self) -> dict:
        rows = self.ga4.run_report(["eventName"], ["eventCount"], self.start_date, self.end_date)
        by_event = [r for r in rows if r.get("eventName") in CONVERSION_EVENTS]
        return {"by_event": by_event}
    
    def _get_channel_data(self) -> dict:
        rows = self.ga4.run_report(["sessionDefaultChannelGroup"], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        for r in rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        rows.sort(key=lambda x: x["conversions"], reverse=True)
        return {"by_channel": rows}
    
    def _get_geo_data(self) -> dict:
        # 전체 국가 (limit 높게)
        country_rows = self.ga4.run_report(["country"], ["sessions", "conversions"], self.start_date, self.end_date, limit=500)
        total_conv = sum(r["conversions"] for r in country_rows)
        for r in country_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
            r["pct"] = round(r["conversions"] / total_conv * 100, 1) if total_conv > 0 else 0
        country_rows.sort(key=lambda x: x["conversions"], reverse=True)
        
        # 전체 도시
        city_rows = self.ga4.run_report(["country", "city"], ["sessions", "conversions"], self.start_date, self.end_date, limit=500)
        for r in city_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        city_rows.sort(key=lambda x: x["conversions"], reverse=True)
        
        target_conv = sum(r["conversions"] for r in country_rows if r["country"] in TARGET_COUNTRIES)
        target_pct = round(target_conv / total_conv * 100, 1) if total_conv > 0 else 0
        
        return {
            "by_country": country_rows, 
            "by_city": city_rows, 
            "target_pct": target_pct, 
            "non_target_pct": round(100 - target_pct, 1),
            "total_countries": len([r for r in country_rows if r["conversions"] > 0])
        }
    
    def _get_campaign_data(self) -> dict:
        query = f"SELECT campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        rows = self.ads.run_query(query)
        
        campaigns = {}
        for r in rows:
            name = r.campaign.name
            if name not in campaigns:
                campaigns[name] = {"campaign": name, "impressions": 0, "clicks": 0, "cost": 0, "conversions": 0}
            campaigns[name]["impressions"] += r.metrics.impressions
            campaigns[name]["clicks"] += r.metrics.clicks
            campaigns[name]["cost"] += r.metrics.cost_micros / 1_000_000
            campaigns[name]["conversions"] += r.metrics.conversions
        
        result = []
        for c in campaigns.values():
            c["ctr"] = round(c["clicks"] / c["impressions"] * 100, 2) if c["impressions"] > 0 else 0
            c["cvr"] = round(c["conversions"] / c["clicks"] * 100, 2) if c["clicks"] > 0 else 0
            c["cpa"] = round(c["cost"] / c["conversions"], 0) if c["conversions"] > 0 else 0
            result.append(c)
        result.sort(key=lambda x: x["conversions"], reverse=True)
        return {"by_campaign": result}
    
    def _get_page_data(self) -> dict:
        top_pages = self.ga4.run_report(["pagePath"], ["screenPageViews", "averageSessionDuration"], self.start_date, self.end_date, limit=30)
        top_pages.sort(key=lambda x: x.get("screenPageViews", 0), reverse=True)
        
        landing = self.ga4.run_report(["landingPage"], ["sessions", "conversions"], self.start_date, self.end_date, limit=30)
        for r in landing:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        landing.sort(key=lambda x: x["conversions"], reverse=True)
        
        return {"top_pages": top_pages, "landing_pages": landing}
    
    def _get_daily_trend(self) -> dict:
        # GA4 일별 데이터
        daily_ga4 = self.ga4.run_report(["date"], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        daily_ga4.sort(key=lambda x: x["date"])
        
        # Ads 일별 데이터
        query = f"SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        ads_rows = self.ads.run_query(query)
        
        daily_ads = {}
        for r in ads_rows:
            d = r.segments.date
            if d not in daily_ads:
                daily_ads[d] = {"date": d, "impressions": 0, "clicks": 0, "cost": 0, "conversions": 0}
            daily_ads[d]["impressions"] += r.metrics.impressions
            daily_ads[d]["clicks"] += r.metrics.clicks
            daily_ads[d]["cost"] += r.metrics.cost_micros / 1_000_000
            daily_ads[d]["conversions"] += r.metrics.conversions
        
        return {"ga4": daily_ga4, "ads": list(daily_ads.values())}
    
    def _detect_anomalies(self) -> dict:
        anomalies = []
        ga4_conv = self.data["summary"]["conversions"]
        ads_conv = self.data["summary"]["ads_conversions"]
        if ga4_conv > 0 and ads_conv > 0:
            disc = abs(ga4_conv - ads_conv) / ga4_conv * 100
            if disc > 30:
                anomalies.append({"type": "전환 데이터 불일치", "detail": f"GA4 ({ga4_conv}건) vs Google Ads ({ads_conv:.0f}건) - {disc:.0f}% 차이", "severity": "high" if disc > 50 else "medium"})
        
        for c in self.data.get("geo", {}).get("by_country", []):
            if c["country"] not in TARGET_COUNTRIES and c["cvr"] > 10 and c["conversions"] > 10:
                country_kr = COUNTRY_KR.get(c["country"], c["country"])
                anomalies.append({"type": "의심 트래픽", "detail": f"{country_kr}: {c['conversions']}건 전환, CVR {c['cvr']}% (비정상적으로 높음)", "severity": "high"})
        
        city_data = self.data.get("geo", {}).get("by_city", [])
        if city_data:
            total = sum(c["conversions"] for c in city_data)
            for city in city_data[:3]:
                if total > 0:
                    pct = city["conversions"] / total * 100
                    if pct > 25 and city["country"] not in TARGET_COUNTRIES:
                        country_kr = COUNTRY_KR.get(city["country"], city["country"])
                        anomalies.append({"type": "특정 도시 집중", "detail": f"{city['city']} ({country_kr}): 전체 전환의 {pct:.0f}% 집중", "severity": "medium"})
        
        return {"items": anomalies, "count": len(anomalies)}
    
    def generate_html(self, output_path: str):
        html = self._build_html()
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"리포트 저장됨: {output_path}")
    
    def _build_html(self) -> str:
        # Chart data
        daily_labels = [r["date"] for r in self.data["daily"]["ga4"]]
        daily_sessions = [r["sessions"] for r in self.data["daily"]["ga4"]]
        daily_conversions = [r["conversions"] for r in self.data["daily"]["ga4"]]
        
        ads_daily = {r["date"]: r for r in self.data["daily"]["ads"]}
        daily_cost = [ads_daily.get(d.replace("-", ""), {}).get("cost", 0) for d in daily_labels]
        
        # Channel chart data
        channel_labels = [CHANNEL_KR.get(r["sessionDefaultChannelGroup"], r["sessionDefaultChannelGroup"]) for r in self.data["channels"]["by_channel"][:8]]
        channel_values = [r["conversions"] for r in self.data["channels"]["by_channel"][:8]]
        
        # Country chart data (top 10)
        country_labels = [COUNTRY_KR.get(r["country"], r["country"]) for r in self.data["geo"]["by_country"][:10]]
        country_values = [r["conversions"] for r in self.data["geo"]["by_country"][:10]]
        
        # Build leads table
        leads_rows = ""
        for r in self.data["leads"]["by_event"]:
            event_kr = EVENT_KR.get(r["eventName"], r["eventName"])
            leads_rows += f"<tr><td>{event_kr}</td><td class='num'>{r['eventCount']:,}</td></tr>"
        
        # Build channel table
        channel_rows = ""
        for r in self.data["channels"]["by_channel"]:
            ch_kr = CHANNEL_KR.get(r["sessionDefaultChannelGroup"], r["sessionDefaultChannelGroup"])
            channel_rows += f"<tr><td><strong>{ch_kr}</strong></td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Build country table (ALL countries)
        country_rows = ""
        for i, r in enumerate(self.data["geo"]["by_country"]):
            if r["conversions"] == 0:
                continue  # 전환 0인 국가는 제외
            c_kr = COUNTRY_KR.get(r["country"], r["country"])
            tag = '<span class="tag tag-target">타겟</span>' if r["country"] in TARGET_COUNTRIES else ('<span class="tag tag-suspect">확인필요</span>' if r["cvr"] > 10 and r["conversions"] > 5 else "")
            country_rows += f"<tr><td>{i+1}</td><td><strong>{c_kr}</strong> {tag}</td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td><td class='num'>{r['pct']}%</td></tr>"
        
        # Build city table (ALL cities with conversions)
        city_rows = ""
        for i, r in enumerate(self.data["geo"]["by_city"]):
            if r["conversions"] == 0:
                continue
            c_kr = COUNTRY_KR.get(r["country"], r["country"])
            city_rows += f"<tr><td>{i+1}</td><td><strong>{r['city']}</strong></td><td>{c_kr}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Build campaign table
        campaign_rows = ""
        for r in self.data["campaigns"]["by_campaign"]:
            campaign_rows += f"<tr><td><strong>{r['campaign']}</strong></td><td class='num'>₩{r['cost']:,.0f}</td><td class='num'>{r['impressions']:,}</td><td class='num'>{r['clicks']:,}</td><td class='num'>{r['ctr']}%</td><td class='num'>{r['conversions']:.1f}</td><td class='num'>{r['cvr']}%</td><td class='num'>₩{r['cpa']:,.0f}</td></tr>"
        
        # Build page tables
        page_rows = ""
        for r in self.data["pages"]["top_pages"][:20]:
            duration = r.get("averageSessionDuration", 0)
            page_rows += f"<tr><td title='{r['pagePath']}'>{r['pagePath'][:50]}{'...' if len(r['pagePath'])>50 else ''}</td><td class='num'>{r['screenPageViews']:,}</td><td class='num'>{duration:.0f}초</td></tr>"
        
        landing_rows = ""
        for r in self.data["pages"]["landing_pages"][:20]:
            landing_rows += f"<tr><td title='{r['landingPage']}'>{r['landingPage'][:50]}{'...' if len(r['landingPage'])>50 else ''}</td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Anomalies
        if self.data["anomalies"]["count"] == 0:
            anomaly_html = '<div class="alert alert-success"><strong>✓ 이상 징후 없음</strong><br>데이터 품질 정상</div>'
        else:
            anomaly_html = ""
            for a in self.data["anomalies"]["items"]:
                sev = "danger" if a["severity"] == "high" else "warning"
                anomaly_html += f'<div class="alert alert-{sev}"><strong>⚠️ {a["type"]}</strong><br>{a["detail"]}</div>'
        
        html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>휴라이트 마케팅 성과 리포트</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Malgun Gothic', sans-serif; line-height: 1.6; color: #2C3E50; background: #F5F6FA; }}
        .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 12px; margin-bottom: 30px; }}
        .header h1 {{ font-size: 32px; margin-bottom: 8px; }}
        .header .period {{ font-size: 16px; opacity: 0.9; }}
        .section {{ background: white; border-radius: 12px; padding: 30px; margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .section-title {{ font-size: 22px; font-weight: 700; margin-bottom: 24px; padding-bottom: 12px; border-bottom: 3px solid #667eea; display: flex; align-items: center; gap: 10px; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .kpi-card {{ background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); border-left: 5px solid #667eea; padding: 24px; border-radius: 8px; }}
        .kpi-card.highlight {{ border-left-color: #e74c3c; background: linear-gradient(135deg, #fff5f5 0%, #ffe3e3 100%); }}
        .kpi-label {{ font-size: 14px; color: #7F8C8D; margin-bottom: 8px; font-weight: 500; }}
        .kpi-value {{ font-size: 32px; font-weight: 800; color: #2C3E50; }}
        .kpi-unit {{ font-size: 16px; color: #7F8C8D; margin-left: 4px; }}
        .kpi-sub {{ font-size: 13px; color: #95a5a6; margin-top: 4px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ background: #34495E; color: white; padding: 14px 12px; text-align: left; font-weight: 600; position: sticky; top: 0; }}
        td {{ padding: 12px; border-bottom: 1px solid #ECF0F1; }}
        td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        tr:hover {{ background: #F8F9FA; }}
        .table-wrapper {{ max-height: 500px; overflow-y: auto; border: 1px solid #e0e0e0; border-radius: 8px; }}
        .chart-container {{ height: 350px; margin-bottom: 30px; }}
        .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 30px; }}
        @media (max-width: 1000px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
        .alert {{ padding: 16px 20px; border-radius: 8px; margin: 12px 0; }}
        .alert-success {{ background: #d4edda; border-left: 4px solid #28a745; }}
        .alert-warning {{ background: #fff3cd; border-left: 4px solid #ffc107; }}
        .alert-danger {{ background: #f8d7da; border-left: 4px solid #dc3545; }}
        .tag {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; margin-left: 8px; }}
        .tag-target {{ background: #d4edda; color: #155724; }}
        .tag-suspect {{ background: #f8d7da; color: #721c24; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }}
        @media (max-width: 1000px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
        .footer {{ text-align: center; padding: 30px; color: #7F8C8D; font-size: 13px; }}
        .stats-badge {{ display: inline-block; background: #667eea; color: white; padding: 4px 12px; border-radius: 20px; font-size: 13px; margin-left: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 휴라이트 마케팅 성과 리포트</h1>
            <div class="period">분석 기간: {self.start_date} ~ {self.end_date}</div>
            <div class="period">생성일시: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
        </div>
        
        <!-- 1. 핵심 지표 -->
        <div class="section">
            <h2 class="section-title">📊 1. 핵심 지표 (Executive Summary)</h2>
            <div class="kpi-grid">
                <div class="kpi-card"><div class="kpi-label">총 리드 (전환)</div><div class="kpi-value">{self.data["summary"]["conversions"]:,}<span class="kpi-unit">건</span></div></div>
                <div class="kpi-card"><div class="kpi-label">총 세션</div><div class="kpi-value">{self.data["summary"]["sessions"]:,}<span class="kpi-unit">회</span></div></div>
                <div class="kpi-card"><div class="kpi-label">전환율 (CVR)</div><div class="kpi-value">{self.data["s
exit
cat > src/daily_report_kr.py << 'ENDOFFILE'
"""
휴라이트 마케팅 성과 리포트 생성기 (한국어)
"""

import os
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)
from google.ads.googleads.client import GoogleAdsClient


GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CREDENTIALS_PATH = ".secrets/ga4.json"
ADS_CREDENTIALS_PATH = ".secrets/google-ads.yaml"

CONVERSION_EVENTS = ["contact_form_submit", "email_click", "phone_calls", "wechat_call", "kakao_click"]
TARGET_COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "Australia", "Japan", "Singapore", "United Arab Emirates", "South Korea"]

# 국가명 한국어 매핑
COUNTRY_KR = {
    "United States": "미국", "South Korea": "대한민국", "Canada": "캐나다", "United Kingdom": "영국",
    "Germany": "독일", "France": "프랑스", "Italy": "이탈리아", "Spain": "스페인", "Netherlands": "네덜란드",
    "Belgium": "벨기에", "Australia": "호주", "Japan": "일본", "Singapore": "싱가포르",
    "United Arab Emirates": "아랍에미리트", "Nepal": "네팔", "India": "인도", "Philippines": "필리핀",
    "Nigeria": "나이지리아", "Poland": "폴란드", "Türkiye": "튀르키예", "China": "중국",
    "Hong Kong": "홍콩", "Taiwan": "대만", "Thailand": "태국", "Vietnam": "베트남",
    "Indonesia": "인도네시아", "Malaysia": "말레이시아", "Brazil": "브라질", "Mexico": "멕시코",
    "Russia": "러시아", "South Africa": "남아프리카", "Egypt": "이집트", "Morocco": "모로코",
    "Saudi Arabia": "사우디아라비아", "Israel": "이스라엘", "Ireland": "아일랜드", "Switzerland": "스위스",
    "Austria": "오스트리아", "Sweden": "스웨덴", "Norway": "노르웨이", "Denmark": "덴마크",
    "Finland": "핀란드", "Portugal": "포르투갈", "Greece": "그리스", "Czechia": "체코",
    "Romania": "루마니아", "Hungary": "헝가리", "Bulgaria": "불가리아", "Croatia": "크로아티아",
    "New Zealand": "뉴질랜드", "Argentina": "아르헨티나", "Chile": "칠레", "Colombia": "콜롬비아",
    "Peru": "페루", "Pakistan": "파키스탄", "Bangladesh": "방글라데시", "Sri Lanka": "스리랑카",
    "Qatar": "카타르", "Kuwait": "쿠웨이트", "Bahrain": "바레인", "Oman": "오만",
    "Martinique": "마르티니크", "Venezuela": "베네수엘라", "(not set)": "(미설정)",
}

# 채널명 한국어 매핑
CHANNEL_KR = {
    "Organic Search": "자연 검색", "Paid Search": "유료 검색", "Direct": "직접 유입",
    "Referral": "추천 유입", "Organic Social": "자연 소셜", "Paid Social": "유료 소셜",
    "Email": "이메일", "Display": "디스플레이", "Unassigned": "미분류", "(not set)": "(미설정)",
}

# 이벤트명 한국어 매핑
EVENT_KR = {
    "contact_form_submit": "문의 폼 제출", "email_click": "이메일 클릭",
    "phone_calls": "전화 문의", "wechat_call": "위챗 문의", "kakao_click": "카카오톡 클릭",
}


class GA4Client:
    def __init__(self, property_id: str):
        creds = service_account.Credentials.from_service_account_file(GA4_CREDENTIALS_PATH, scopes=GA4_SCOPES)
        self.client = BetaAnalyticsDataClient(credentials=creds)
        self.property_id = property_id
    
    def run_report(self, dimensions: list, metrics: list, start_date: str, end_date: str, limit=10000) -> list:
        dim_list = [Dimension(name=d) for d in dimensions] if dimensions else []
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dim_list,
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
        )
        response = self.client.run_report(request)
        
        rows = []
        for row in response.rows:
            data = {}
            for i, dim in enumerate(dimensions):
                data[dim] = row.dimension_values[i].value
            for i, met in enumerate(metrics):
                val = row.metric_values[i].value
                try:
                    data[met] = int(val)
                except ValueError:
                    try:
                        data[met] = float(val)
                    except ValueError:
                        data[met] = val
            rows.append(data)
        return rows


class AdsClient:
    def __init__(self, customer_id: str):
        self.client = GoogleAdsClient.load_from_storage(ADS_CREDENTIALS_PATH)
        self.customer_id = customer_id
        self.service = self.client.get_service("GoogleAdsService")
    
    def run_query(self, query: str) -> list:
        response = self.service.search(customer_id=self.customer_id, query=query)
        return list(response)


class ReportGenerator:
    def __init__(self, property_id: str, customer_id: str, start_date: str, end_date: str):
        self.ga4 = GA4Client(property_id)
        self.ads = AdsClient(customer_id)
        self.start_date = start_date
        self.end_date = end_date
        self.data = {}
    
    def collect_all_data(self):
        print("데이터 수집 중...")
        print("  - 핵심 지표...")
        self.data["summary"] = self._get_summary_data()
        print("  - 리드 현황...")
        self.data["leads"] = self._get_lead_data()
        print("  - 채널 성과...")
        self.data["channels"] = self._get_channel_data()
        print("  - 지역별 분포...")
        self.data["geo"] = self._get_geo_data()
        print("  - 캠페인 성과...")
        self.data["campaigns"] = self._get_campaign_data()
        print("  - 웹사이트 현황...")
        self.data["pages"] = self._get_page_data()
        print("  - 일별 추이...")
        self.data["daily"] = self._get_daily_trend()
        print("  - 이상 징후 탐지...")
        self.data["anomalies"] = self._detect_anomalies()
        print("데이터 수집 완료!")
        return self.data
    
    def _get_summary_data(self) -> dict:
        rows = self.ga4.run_report([], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        sessions = sum(r.get("sessions", 0) for r in rows)
        conversions = sum(r.get("conversions", 0) for r in rows)
        users = sum(r.get("totalUsers", 0) for r in rows)
        cvr = (conversions / sessions * 100) if sessions > 0 else 0
        
        query = f"SELECT metrics.cost_micros, metrics.clicks, metrics.conversions, metrics.impressions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        ads_rows = self.ads.run_query(query)
        total_cost = sum(r.metrics.cost_micros for r in ads_rows) / 1_000_000
        total_clicks = sum(r.metrics.clicks for r in ads_rows)
        total_impressions = sum(r.metrics.impressions for r in ads_rows)
        ads_conversions = sum(r.metrics.conversions for r in ads_rows)
        cpa = total_cost / conversions if conversions > 0 else 0
        ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        return {
            "sessions": sessions, "users": users, "conversions": int(conversions), 
            "cvr": round(cvr, 2), "total_cost": round(total_cost, 0), 
            "total_clicks": total_clicks, "total_impressions": total_impressions,
            "ads_conversions": round(ads_conversions, 1), "cpa": round(cpa, 0), "ctr": round(ctr, 2)
        }
    
    def _get_lead_data(self) -> dict:
        rows = self.ga4.run_report(["eventName"], ["eventCount"], self.start_date, self.end_date)
        by_event = [r for r in rows if r.get("eventName") in CONVERSION_EVENTS]
        return {"by_event": by_event}
    
    def _get_channel_data(self) -> dict:
        rows = self.ga4.run_report(["sessionDefaultChannelGroup"], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        for r in rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        rows.sort(key=lambda x: x["conversions"], reverse=True)
        return {"by_channel": rows}
    
    def _get_geo_data(self) -> dict:
        # 전체 국가 (limit 높게)
        country_rows = self.ga4.run_report(["country"], ["sessions", "conversions"], self.start_date, self.end_date, limit=500)
        total_conv = sum(r["conversions"] for r in country_rows)
        for r in country_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
            r["pct"] = round(r["conversions"] / total_conv * 100, 1) if total_conv > 0 else 0
        country_rows.sort(key=lambda x: x["conversions"], reverse=True)
        
        # 전체 도시
        city_rows = self.ga4.run_report(["country", "city"], ["sessions", "conversions"], self.start_date, self.end_date, limit=500)
        for r in city_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        city_rows.sort(key=lambda x: x["conversions"], reverse=True)
        
        target_conv = sum(r["conversions"] for r in country_rows if r["country"] in TARGET_COUNTRIES)
        target_pct = round(target_conv / total_conv * 100, 1) if total_conv > 0 else 0
        
        return {
            "by_country": country_rows, 
            "by_city": city_rows, 
            "target_pct": target_pct, 
            "non_target_pct": round(100 - target_pct, 1),
            "total_countries": len([r for r in country_rows if r["conversions"] > 0])
        }
    
    def _get_campaign_data(self) -> dict:
        query = f"SELECT campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        rows = self.ads.run_query(query)
        
        campaigns = {}
        for r in rows:
            name = r.campaign.name
            if name not in campaigns:
                campaigns[name] = {"campaign": name, "impressions": 0, "clicks": 0, "cost": 0, "conversions": 0}
            campaigns[name]["impressions"] += r.metrics.impressions
            campaigns[name]["clicks"] += r.metrics.clicks
            campaigns[name]["cost"] += r.metrics.cost_micros / 1_000_000
            campaigns[name]["conversions"] += r.metrics.conversions
        
        result = []
        for c in campaigns.values():
            c["ctr"] = round(c["clicks"] / c["impressions"] * 100, 2) if c["impressions"] > 0 else 0
            c["cvr"] = round(c["conversions"] / c["clicks"] * 100, 2) if c["clicks"] > 0 else 0
            c["cpa"] = round(c["cost"] / c["conversions"], 0) if c["conversions"] > 0 else 0
            result.append(c)
        result.sort(key=lambda x: x["conversions"], reverse=True)
        return {"by_campaign": result}
    
    def _get_page_data(self) -> dict:
        top_pages = self.ga4.run_report(["pagePath"], ["screenPageViews", "averageSessionDuration"], self.start_date, self.end_date, limit=30)
        top_pages.sort(key=lambda x: x.get("screenPageViews", 0), reverse=True)
        
        landing = self.ga4.run_report(["landingPage"], ["sessions", "conversions"], self.start_date, self.end_date, limit=30)
        for r in landing:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        landing.sort(key=lambda x: x["conversions"], reverse=True)
        
        return {"top_pages": top_pages, "landing_pages": landing}
    
    def _get_daily_trend(self) -> dict:
        # GA4 일별 데이터
        daily_ga4 = self.ga4.run_report(["date"], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        daily_ga4.sort(key=lambda x: x["date"])
        
        # Ads 일별 데이터
        query = f"SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        ads_rows = self.ads.run_query(query)
        
        daily_ads = {}
        for r in ads_rows:
            d = r.segments.date
            if d not in daily_ads:
                daily_ads[d] = {"date": d, "impressions": 0, "clicks": 0, "cost": 0, "conversions": 0}
            daily_ads[d]["impressions"] += r.metrics.impressions
            daily_ads[d]["clicks"] += r.metrics.clicks
            daily_ads[d]["cost"] += r.metrics.cost_micros / 1_000_000
            daily_ads[d]["conversions"] += r.metrics.conversions
        
        return {"ga4": daily_ga4, "ads": list(daily_ads.values())}
    
    def _detect_anomalies(self) -> dict:
        anomalies = []
        ga4_conv = self.data["summary"]["conversions"]
        ads_conv = self.data["summary"]["ads_conversions"]
        if ga4_conv > 0 and ads_conv > 0:
            disc = abs(ga4_conv - ads_conv) / ga4_conv * 100
            if disc > 30:
                anomalies.append({"type": "전환 데이터 불일치", "detail": f"GA4 ({ga4_conv}건) vs Google Ads ({ads_conv:.0f}건) - {disc:.0f}% 차이", "severity": "high" if disc > 50 else "medium"})
        
        for c in self.data.get("geo", {}).get("by_country", []):
            if c["country"] not in TARGET_COUNTRIES and c["cvr"] > 10 and c["conversions"] > 10:
                country_kr = COUNTRY_KR.get(c["country"], c["country"])
                anomalies.append({"type": "의심 트래픽", "detail": f"{country_kr}: {c['conversions']}건 전환, CVR {c['cvr']}% (비정상적으로 높음)", "severity": "high"})
        
        city_data = self.data.get("geo", {}).get("by_city", [])
        if city_data:
            total = sum(c["conversions"] for c in city_data)
            for city in city_data[:3]:
                if total > 0:
                    pct = city["conversions"] / total * 100
                    if pct > 25 and city["country"] not in TARGET_COUNTRIES:
                        country_kr = COUNTRY_KR.get(city["country"], city["country"])
                        anomalies.append({"type": "특정 도시 집중", "detail": f"{city['city']} ({country_kr}): 전체 전환의 {pct:.0f}% 집중", "severity": "medium"})
        
        return {"items": anomalies, "count": len(anomalies)}
    
    def generate_html(self, output_path: str):
        html = self._build_html()
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"리포트 저장됨: {output_path}")
    
    def _build_html(self) -> str:
        # Chart data
        daily_labels = [r["date"] for r in self.data["daily"]["ga4"]]
        daily_sessions = [r["sessions"] for r in self.data["daily"]["ga4"]]
        daily_conversions = [r["conversions"] for r in self.data["daily"]["ga4"]]
        
        ads_daily = {r["date"]: r for r in self.data["daily"]["ads"]}
        daily_cost = [ads_daily.get(d.replace("-", ""), {}).get("cost", 0) for d in daily_labels]
        
        # Channel chart data
        channel_labels = [CHANNEL_KR.get(r["sessionDefaultChannelGroup"], r["sessionDefaultChannelGroup"]) for r in self.data["channels"]["by_channel"][:8]]
        channel_values = [r["conversions"] for r in self.data["channels"]["by_channel"][:8]]
        
        # Country chart data (top 10)
        country_labels = [COUNTRY_KR.get(r["country"], r["country"]) for r in self.data["geo"]["by_country"][:10]]
        country_values = [r["conversions"] for r in self.data["geo"]["by_country"][:10]]
        
        # Build leads table
        leads_rows = ""
        for r in self.data["leads"]["by_event"]:
            event_kr = EVENT_KR.get(r["eventName"], r["eventName"])
            leads_rows += f"<tr><td>{event_kr}</td><td class='num'>{r['eventCount']:,}</td></tr>"
        
        # Build channel table
        channel_rows = ""
        for r in self.data["channels"]["by_channel"]:
            ch_kr = CHANNEL_KR.get(r["sessionDefaultChannelGroup"], r["sessionDefaultChannelGroup"])
            channel_rows += f"<tr><td><strong>{ch_kr}</strong></td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Build country table (ALL countries)
        country_rows = ""
        for i, r in enumerate(self.data["geo"]["by_country"]):
            if r["conversions"] == 0:
                continue  # 전환 0인 국가는 제외
            c_kr = COUNTRY_KR.get(r["country"], r["country"])
            tag = '<span class="tag tag-target">타겟</span>' if r["country"] in TARGET_COUNTRIES else ('<span class="tag tag-suspect">확인필요</span>' if r["cvr"] > 10 and r["conversions"] > 5 else "")
            country_rows += f"<tr><td>{i+1}</td><td><strong>{c_kr}</strong> {tag}</td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td><td class='num'>{r['pct']}%</td></tr>"
        
        # Build city table (ALL cities with conversions)
        city_rows = ""
        for i, r in enumerate(self.data["geo"]["by_city"]):
            if r["conversions"] == 0:
                continue
            c_kr = COUNTRY_KR.get(r["country"], r["country"])
            city_rows += f"<tr><td>{i+1}</td><td><strong>{r['city']}</strong></td><td>{c_kr}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Build campaign table
        campaign_rows = ""
        for r in self.data["campaigns"]["by_campaign"]:
            campaign_rows += f"<tr><td><strong>{r['campaign']}</strong></td><td class='num'>₩{r['cost']:,.0f}</td><td class='num'>{r['impressions']:,}</td><td class='num'>{r['clicks']:,}</td><td class='num'>{r['ctr']}%</td><td class='num'>{r['conversions']:.1f}</td><td class='num'>{r['cvr']}%</td><td class='num'>₩{r['cpa']:,.0f}</td></tr>"
        
        # Build page tables
        page_rows = ""
        for r in self.data["pages"]["top_pages"][:20]:
            duration = r.get("averageSessionDuration", 0)
            page_rows += f"<tr><td title='{r['pagePath']}'>{r['pagePath'][:50]}{'...' if len(r['pagePath'])>50 else ''}</td><td class='num'>{r['screenPageViews']:,}</td><td class='num'>{duration:.0f}초</td></tr>"
        
        landing_rows = ""
        for r in self.data["pages"]["landing_pages"][:20]:
            landing_rows += f"<tr><td title='{r['landingPage']}'>{r['landingPage'][:50]}{'...' if len(r['landingPage'])>50 else ''}</td><td class='num'>{r['sessions']:,}</td><td class='num'>{r['conversions']}</td><td class='num'>{r['cvr']}%</td></tr>"
        
        # Anomalies
        if self.data["anomalies"]["count"] == 0:
            anomaly_html = '<div class="alert alert-success"><strong>✓ 이상 징후 없음</strong><br>데이터 품질 정상</div>'
        else:
            anomaly_html = ""
            for a in self.data["anomalies"]["items"]:
                sev = "danger" if a["severity"] == "high" else "warning"
                anomaly_html += f'<div class="alert alert-{sev}"><strong>⚠️ {a["type"]}</strong><br>{a["detail"]}</div>'
        
        html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>휴라이트 마케팅 성과 리포트</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Malgun Gothic', sans-serif; line-height: 1.6; color: #2C3E50; background: #F5F6FA; }}
        .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 12px; margin-bottom: 30px; }}
        .header h1 {{ font-size: 32px; margin-bottom: 8px; }}
        .header .period {{ font-size: 16px; opacity: 0.9; }}
        .section {{ background: white; border-radius: 12px; padding: 30px; margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .section-title {{ font-size: 22px; font-weight: 700; margin-bottom: 24px; padding-bottom: 12px; border-bottom: 3px solid #667eea; display: flex; align-items: center; gap: 10px; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .kpi-card {{ background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); border-left: 5px solid #667eea; padding: 24px; border-radius: 8px; }}
        .kpi-card.highlight {{ border-left-color: #e74c3c; background: linear-gradient(135deg, #fff5f5 0%, #ffe3e3 100%); }}
        .kpi-label {{ font-size: 14px; color: #7F8C8D; margin-bottom: 8px; font-weight: 500; }}
        .kpi-value {{ font-size: 32px; font-weight: 800; color: #2C3E50; }}
        .kpi-unit {{ font-size: 16px; color: #7F8C8D; margin-left: 4px; }}
        .kpi-sub {{ font-size: 13px; color: #95a5a6; margin-top: 4px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ background: #34495E; color: white; padding: 14px 12px; text-align: left; font-weight: 600; position: sticky; top: 0; }}
        td {{ padding: 12px; border-bottom: 1px solid #ECF0F1; }}
        td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        tr:hover {{ background: #F8F9FA; }}
        .table-wrapper {{ max-height: 500px; overflow-y: auto; border: 1px solid #e0e0e0; border-radius: 8px; }}
        .chart-container {{ height: 350px; margin-bottom: 30px; }}
        .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 30px; }}
        @media (max-width: 1000px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
        .alert {{ padding: 16px 20px; border-radius: 8px; margin: 12px 0; }}
        .alert-success {{ background: #d4edda; border-left: 4px solid #28a745; }}
        .alert-warning {{ background: #fff3cd; border-left: 4px solid #ffc107; }}
        .alert-danger {{ background: #f8d7da; border-left: 4px solid #dc3545; }}
        .tag {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; margin-left: 8px; }}
        .tag-target {{ background: #d4edda; color: #155724; }}
        .tag-suspect {{ background: #f8d7da; color: #721c24; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }}
        @media (max-width: 1000px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
        .footer {{ text-align: center; padding: 30px; color: #7F8C8D; font-size: 13px; }}
        .stats-badge {{ display: inline-block; background: #667eea; color: white; padding: 4px 12px; border-radius: 20px; font-size: 13px; margin-left: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 휴라이트 마케팅 성과 리포트</h1>
            <div class="period">분석 기간: {self.start_date} ~ {self.end_date}</div>
            <div class="period">생성일시: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
        </div>
        
        <!-- 1. 핵심 지표 -->
        <div class="section">
            <h2 class="section-title">📊 1. 핵심 지표 (Executive Summary)</h2>
            <div class="kpi-grid">
                <div class="kpi-card"><div class="kpi-label">총 리드 (전환)</div><div class="kpi-value">{self.data["summary"]["conversions"]:,}<span class="kpi-unit">건</span></div></div>
                <div class="kpi-card"><div class="kpi-label">총 세션</div><div class="kpi-value">{self.data["summary"]["sessions"]:,}<span class="kpi-unit">회</span></div></div>
                <div class="kpi-card"><div class="kpi-label">전환율 (CVR)</div><div class="kpi-value">{self.data["summary"]["cvr"]}<span class="kpi-unit">%</span></div></div>
                <div class="kpi-card"><div class="kpi-label">총 광고비</div><div class="kpi-value">₩{self.data["summary"]["total_cost"]:,.0f}</div><div class="kpi-sub">(약 {self.data["summary"]["total_cost"]/10000:.0f}만원)</div></div>
                <div class="kpi-card highlight"><div class="kpi-label">리드당 비용 (CPA)</div><div class="kpi-value">₩{self.data["summary"]["cpa"]:,.0f}</div></div>
                <div class="kpi-card"><div class="kpi-label">Ads 전환</div><div class="kpi-value">{self.data["summary"]["ads_conversions"]}<span class="kpi-unit">건</span></div></div>
                <div class="kpi-card"><div class="kpi-label">총 클릭</div><div class="kpi-value">{self.data["summary"]["total_clicks"]:,}<span class="kpi-unit">회</span></div></div>
                <div class="kpi-card"><div class="kpi-label">클릭률 (CTR)</div><div class="kpi-value">{self.data["summary"]["ctr"]}<span class="kpi-unit">%</span></div></div>
            </div>
            
            <!-- 일별 추이 차트 -->
            <div class="chart-row">
                <div class="chart-container">
                    <canvas id="dailySessionsChart"></canvas>
                </div>
                <div class="chart-container">
                    <canvas id="dailyConversionsChart"></canvas>
                </div>
            </div>
        </div>
        
        <!-- 2. 리드 현황 -->
        <div class="section">
            <h2 class="section-title">📥 2. 리드 현황 (Lead Acquisition)</h2>
            <div class="two-col">
                <div>
                    <h3 style="margin-bottom:15px;">전환 이벤트별 현황</h3>
                    <table><thead><tr><th>이벤트</th><th>건수</th></tr></thead><tbody>{leads_rows}</tbody></table>
                </div>
                <div class="chart-container">
                    <canvas id="leadsChart"></canvas>
                </div>
            </div>
        </div>
        
        <!-- 3. 채널 성과 -->
        <div class="section">
            <h2 class="section-title">📡 3. 채널 성과 (Channel Performance)</h2>
            <div class="chart-row">
                <div class="chart-container">
                    <canvas id="channelChart"></canvas>
                </div>
                <div>
                    <table><thead><tr><th>채널</th><th>세션</th><th>전환</th><th>전환율</th></tr></thead><tbody>{channel_rows}</tbody></table>
                </div>
            </div>
        </div>
        
        <!-- 4. 지역별 분포 -->
        <div class="section">
            <h2 class="section-title">🌍 4. 지역별 분포 (Geographic Distribution) <span class="stats-badge">전환 발생 국가: {self.data["geo"]["total_countries"]}개국</span></h2>
            <div class="kpi-grid" style="margin-bottom:25px;">
                <div class="kpi-card"><div class="kpi-label">타겟 국가 비중</div><div class="kpi-value">{self.data["geo"]["target_pct"]}<span class="kpi-unit">%</span></div></div>
                <div class="kpi-card highlight"><div class="kpi-label">비타겟 국가 비중</div><div class="kpi-value">{self.data["geo"]["non_target_pct"]}<span class="kpi-unit">%</span></div></div>
            </div>
            
            <div class="chart-row">
                <div class="chart-container">
                    <canvas id="countryChart"></canvas>
                </div>
                <div class="chart-container">
                    <canvas id="countryPieChart"></canvas>
                </div>
            </div>
            
            <div class="two-col">
                <div>
                    <h3 style="margin-bottom:15px;">국가별 전환 (전체)</h3>
                    <div class="table-wrapper">
                        <table><thead><tr><th>#</th><th>국가</th><th>세션</th><th>전환</th><th>전환율</th><th>비중</th></tr></thead><tbody>{country_rows}</tbody></table>
                    </div>
                </div>
                <div>
                    <h3 style="margin-bottom:15px;">도시별 전환 (전체)</h3>
                    <div class="table-wrapper">
                        <table><thead><tr><th>#</th><th>도시</th><th>국가</th><th>전환</th><th>전환율</th></tr></thead><tbody>{city_rows}</tbody></table>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- 5. 캠페인 성과 -->
        <div class="section">
            <h2 class="section-title">📢 5. 캠페인 성과 (Campaign Performance)</h2>
            <div class="table-wrapper">
                <table>
                    <thead><tr><th>캠페인</th><th>광고비</th><th>노출</th><th>클릭</th><th>CTR</th><th>전환</th><th>CVR</th><th>CPA</th></tr></thead>
                    <tbody>{campaign_rows}</tbody>
                </table>
            </div>
        </div>
        
        <!-- 6. 웹사이트 현황 -->
        <div class="section">
            <h2 class="section-title">🌐 6. 웹사이트 현황 (Website Engagement)</h2>
            <div class="two-col">
                <div>
                    <h3 style="margin-bottom:15px;">페이지별 조회수</h3>
                    <div class="table-wrapper">
                        <table><thead><tr><th>페이지</th><th>조회수</th><th>평균 체류</th></tr></thead><tbody>{page_rows}</tbody></table>
                    </div>
                </div>
                <div>
                    <h3 style="margin-bottom:15px;">랜딩페이지별 전환</h3>
                    <div class="table-wrapper">
                        <table><thead><tr><th>랜딩페이지</th><th>세션</th><th>전환</th><th>전환율</th></tr></thead><tbody>{landing_rows}</tbody></table>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- 7. 이상 징후 -->
        <div class="section">
            <h2 class="section-title">⚠️ 7. 데이터 품질 및 이상 징후 (Data Quality & Anomalies)</h2>
            {anomaly_html}
        </div>
        
        <div class="footer">
            <p>휴라이트(Hue Light Co., Ltd.) 마케팅 성과 리포트</p>
            <p>자동 생성: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
        </div>
    </div>
    
    <script>
        // 일별 세션 차트
        new Chart(document.getElementById('dailySessionsChart'), {{
            type: 'line',
            data: {{
                labels: {daily_labels},
                datasets: [{{
                    label: '세션',
                    data: {daily_sessions},
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    fill: true,
                    tension: 0.3
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: '일별 세션 추이' }} }}
            }}
        }});
        
        // 일별 전환 차트
        new Chart(document.getElementById('dailyConversionsChart'), {{
            type: 'line',
            data: {{
                labels: {daily_labels},
                datasets: [{{
                    label: '전환',
                    data: {daily_conversions},
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: true,
                    tension: 0.3
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: '일별 전환 추이' }} }}
            }}
        }});
        
        // 리드 이벤트 차트
        new Chart(document.getElementById('leadsChart'), {{
            type: 'doughnut',
            data: {{
                labels: {[EVENT_KR.get(r["eventName"], r["eventName"]) for r in self.data["leads"]["by_event"]]},
                datasets: [{{
                    data: {[r["eventCount"] for r in self.data["leads"]["by_event"]]},
                    backgroundColor: ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: '전환 이벤트 분포' }} }}
            }}
        }});
        
        // 채널 차트
        new Chart(document.getElementById('channelChart'), {{
            type: 'bar',
            data: {{
                labels: {channel_labels},
                datasets: [{{
                    label: '전환',
                    data: {channel_values},
                    backgroundColor: '#667eea'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: '채널별 전환' }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});
        
        // 국가 바 차트
        new Chart(document.getElementById('countryChart'), {{
            type: 'bar',
            data: {{
                labels: {country_labels},
                datasets: [{{
                    label: '전환',
                    data: {country_values},
                    backgroundColor: '#764ba2'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{ title: {{ display: true, text: 'Top 10 국가별 전환' }} }}
            }}
        }});
        
        // 국가 파이 차트 (타겟 vs 비타겟)
        new Chart(document.getElementById('countryPieChart'), {{
            type: 'pie',
            data: {{
                labels: ['타겟 국가', '비타겟 국가'],
                datasets: [{{
                    data: [{self.data["geo"]["target_pct"]}, {self.data["geo"]["non_target_pct"]}],
                    backgroundColor: ['#28a745', '#dc3545']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ title: {{ display: true, text: '타겟 국가 비중' }} }}
            }}
        }});
    </script>
</body>
</html>'''
        return html


def main():
    load_dotenv()
    property_id = os.getenv("PROPERTY_ID")
    customer_id = os.getenv("CUSTOMER_ID")
    if not property_id or not customer_id:
        print("오류: PROPERTY_ID와 CUSTOMER_ID가 필요합니다")
        return
    
    end_date = os.getenv("END_DATE", date.today().isoformat())
    start_date = os.getenv("START_DATE", (date.today() - timedelta(days=7)).isoformat())
    
    print(f"리포트 생성 중: {start_date} ~ {end_date}")
    generator = ReportGenerator(property_id, customer_id, start_date, end_date)
    generator.collect_all_data()
    
    output_path = f"reports/{end_date}/index.html"
    generator.generate_html(output_path)
    print(f"\n✅ 완료! 열기: {output_path}")


if __name__ == "__main__":
    main()
