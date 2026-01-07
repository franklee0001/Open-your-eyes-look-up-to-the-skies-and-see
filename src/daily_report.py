"""
Hue Light Marketing Performance Report Generator
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
from jinja2 import Template


GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CREDENTIALS_PATH = ".secrets/ga4.json"
ADS_CREDENTIALS_PATH = ".secrets/google-ads.yaml"

CONVERSION_EVENTS = ["contact_form_submit", "email_click", "phone_calls", "wechat_call", "kakao_click"]
TARGET_COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "Australia", "Japan", "Singapore", "United Arab Emirates", "South Korea"]


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
        print("Collecting data...")
        print("  - Executive Summary...")
        self.data["summary"] = self._get_summary_data()
        print("  - Lead Acquisition...")
        self.data["leads"] = self._get_lead_data()
        print("  - Channel Performance...")
        self.data["channels"] = self._get_channel_data()
        print("  - Geographic Distribution...")
        self.data["geo"] = self._get_geo_data()
        print("  - Campaign Performance...")
        self.data["campaigns"] = self._get_campaign_data()
        print("  - Website Engagement...")
        self.data["pages"] = self._get_page_data()
        print("  - Data Quality & Anomalies...")
        self.data["anomalies"] = self._detect_anomalies()
        print("Data collection complete!")
        return self.data
    
    def _get_summary_data(self) -> dict:
        rows = self.ga4.run_report([], ["sessions", "conversions", "totalUsers"], self.start_date, self.end_date)
        sessions = sum(r.get("sessions", 0) for r in rows)
        conversions = sum(r.get("conversions", 0) for r in rows)
        users = sum(r.get("totalUsers", 0) for r in rows)
        cvr = (conversions / sessions * 100) if sessions > 0 else 0
        
        query = f"SELECT metrics.cost_micros, metrics.clicks, metrics.conversions FROM campaign WHERE segments.date BETWEEN '{self.start_date}' AND '{self.end_date}' AND campaign.status != 'REMOVED'"
        ads_rows = self.ads.run_query(query)
        total_cost = sum(r.metrics.cost_micros for r in ads_rows) / 1_000_000
        total_clicks = sum(r.metrics.clicks for r in ads_rows)
        ads_conversions = sum(r.metrics.conversions for r in ads_rows)
        cpa = total_cost / conversions if conversions > 0 else 0
        
        return {"sessions": sessions, "users": users, "conversions": int(conversions), "cvr": round(cvr, 2), "total_cost": round(total_cost, 0), "total_clicks": total_clicks, "ads_conversions": round(ads_conversions, 1), "cpa": round(cpa, 0)}
    
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
        country_rows = self.ga4.run_report(["country"], ["sessions", "conversions"], self.start_date, self.end_date)
        total_conv = sum(r["conversions"] for r in country_rows)
        for r in country_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
            r["pct"] = round(r["conversions"] / total_conv * 100, 1) if total_conv > 0 else 0
        country_rows.sort(key=lambda x: x["conversions"], reverse=True)
        
        city_rows = self.ga4.run_report(["country", "city"], ["sessions", "conversions"], self.start_date, self.end_date, limit=50)
        for r in city_rows:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        city_rows.sort(key=lambda x: x["conversions"], reverse=True)
        city_rows = city_rows[:30]
        
        target_conv = sum(r["conversions"] for r in country_rows if r["country"] in TARGET_COUNTRIES)
        target_pct = round(target_conv / total_conv * 100, 1) if total_conv > 0 else 0
        
        return {"by_country": country_rows, "by_city": city_rows, "target_pct": target_pct, "non_target_pct": round(100 - target_pct, 1)}
    
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
        top_pages = self.ga4.run_report(["pagePath"], ["screenPageViews", "averageSessionDuration", "bounceRate"], self.start_date, self.end_date, limit=20)
        top_pages.sort(key=lambda x: x.get("screenPageViews", 0), reverse=True)
        
        landing = self.ga4.run_report(["landingPage"], ["sessions", "conversions"], self.start_date, self.end_date, limit=20)
        for r in landing:
            r["cvr"] = round(r["conversions"] / r["sessions"] * 100, 2) if r["sessions"] > 0 else 0
        landing.sort(key=lambda x: x["conversions"], reverse=True)
        
        return {"top_pages": top_pages, "landing_pages": landing}
    
    def _detect_anomalies(self) -> dict:
        anomalies = []
        ga4_conv = self.data["summary"]["conversions"]
        ads_conv = self.data["summary"]["ads_conversions"]
        if ga4_conv > 0 and ads_conv > 0:
            disc = abs(ga4_conv - ads_conv) / ga4_conv * 100
            if disc > 30:
                anomalies.append({"type": "Conversion Discrepancy", "detail": f"GA4 ({ga4_conv}) vs Ads ({ads_conv:.0f}) - {disc:.0f}% difference", "severity": "high" if disc > 50 else "medium"})
        
        for c in self.data.get("geo", {}).get("by_country", []):
            if c["country"] not in TARGET_COUNTRIES and c["cvr"] > 10 and c["conversions"] > 10:
                anomalies.append({"type": "Suspicious Traffic", "detail": f"{c['country']}: {c['conversions']} conversions, {c['cvr']}% CVR", "severity": "high"})
        
        city_data = self.data.get("geo", {}).get("by_city", [])
        if city_data:
            total = sum(c["conversions"] for c in city_data)
            for city in city_data[:3]:
                if total > 0:
                    pct = city["conversions"] / total * 100
                    if pct > 30 and city["country"] not in TARGET_COUNTRIES:
                        anomalies.append({"type": "City Concentration", "detail": f"{city['city']}, {city['country']}: {pct:.0f}% of conversions", "severity": "medium"})
        
        return {"items": anomalies, "count": len(anomalies)}
    
    def generate_html(self, output_path: str):
        html = HTML_TEMPLATE.replace("{{start_date}}", self.start_date)
        html = html.replace("{{end_date}}", self.end_date)
        html = html.replace("{{generated_at}}", datetime.now().strftime("%Y-%m-%d %H:%M"))
        
        # Summary
        html = html.replace("{{summary.conversions}}", str(self.data["summary"]["conversions"]))
        html = html.replace("{{summary.sessions}}", f"{self.data['summary']['sessions']:,}")
        html = html.replace("{{summary.cvr}}", str(self.data["summary"]["cvr"]))
        html = html.replace("{{summary.total_cost}}", f"{self.data['summary']['total_cost']:,.0f}")
        html = html.replace("{{summary.cpa}}", f"{self.data['summary']['cpa']:,.0f}")
        html = html.replace("{{summary.ads_conversions}}", str(self.data["summary"]["ads_conversions"]))
        
        # Geo
        html = html.replace("{{geo.target_pct}}", str(self.data["geo"]["target_pct"]))
        html = html.replace("{{geo.non_target_pct}}", str(self.data["geo"]["non_target_pct"]))
        
        # Build tables
        leads_rows = "".join([f"<tr><td>{r['eventName']}</td><td>{r['eventCount']}</td></tr>" for r in self.data["leads"]["by_event"]])
        html = html.replace("{{leads_rows}}", leads_rows)
        
        channel_rows = "".join([f"<tr><td><strong>{r['sessionDefaultChannelGroup']}</strong></td><td>{r['sessions']:,}</td><td>{r['conversions']}</td><td>{r['cvr']}%</td></tr>" for r in self.data["channels"]["by_channel"]])
        html = html.replace("{{channel_rows}}", channel_rows)
        
        country_rows = ""
        for r in self.data["geo"]["by_country"][:20]:
            tag = '<span class="tag tag-target">Target</span>' if r["country"] in TARGET_COUNTRIES else ('<span class="tag tag-suspect">Check</span>' if r["cvr"] > 10 and r["conversions"] > 5 else "")
            country_rows += f"<tr><td><strong>{r['country']}</strong> {tag}</td><td>{r['sessions']:,}</td><td>{r['conversions']}</td><td>{r['cvr']}%</td><td>{r['pct']}%</td></tr>"
        html = html.replace("{{country_rows}}", country_rows)
        
        city_rows = "".join([f"<tr><td><strong>{r['city']}</strong></td><td>{r['country']}</td><td>{r['conversions']}</td><td>{r['cvr']}%</td></tr>" for r in self.data["geo"]["by_city"][:20]])
        html = html.replace("{{city_rows}}", city_rows)
        
        campaign_rows = "".join([f"<tr><td><strong>{r['campaign']}</strong></td><td>{r['cost']:,.0f}</td><td>{r['impressions']:,}</td><td>{r['clicks']:,}</td><td>{r['ctr']}%</td><td>{r['conversions']:.1f}</td><td>{r['cvr']}%</td><td>{r['cpa']:,.0f}</td></tr>" for r in self.data["campaigns"]["by_campaign"]])
        html = html.replace("{{campaign_rows}}", campaign_rows)
        
        page_rows = "".join([f"<tr><td>{r['pagePath'][:60]}</td><td>{r['screenPageViews']:,}</td><td>{r.get('averageSessionDuration', 0):.0f}s</td></tr>" for r in self.data["pages"]["top_pages"][:15]])
        html = html.replace("{{page_rows}}", page_rows)
        
        landing_rows = "".join([f"<tr><td>{r['landingPage'][:60]}</td><td>{r['sessions']:,}</td><td>{r['conversions']}</td><td>{r['cvr']}%</td></tr>" for r in self.data["pages"]["landing_pages"][:15]])
        html = html.replace("{{landing_rows}}", landing_rows)
        
        if self.data["anomalies"]["count"] == 0:
            anomaly_html = '<div class="alert alert-low"><strong>✓ No anomalies detected</strong></div>'
        else:
            anomaly_html = "".join([f'<div class="alert alert-{a["severity"]}"><strong>{a["type"]}</strong><br>{a["detail"]}</div>' for a in self.data["anomalies"]["items"]])
        html = html.replace("{{anomaly_rows}}", anomaly_html)
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Report saved to: {output_path}")


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Marketing Performance Report</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6; color: #2C3E50; background: #F5F6FA; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 12px; margin-bottom: 30px; }
        .header h1 { font-size: 28px; margin-bottom: 8px; }
        .section { background: white; border-radius: 12px; padding: 30px; margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .section-title { font-size: 20px; font-weight: 600; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 2px solid #667eea; }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .kpi-card { background: #F8F9FA; border-left: 4px solid #667eea; padding: 20px; border-radius: 8px; }
        .kpi-label { font-size: 13px; color: #7F8C8D; margin-bottom: 6px; }
        .kpi-value { font-size: 28px; font-weight: 700; color: #2C3E50; }
        .kpi-unit { font-size: 14px; color: #7F8C8D; }
        table { width: 100%; border-collapse: collapse; font-size: 14px; }
        th { background: #34495E; color: white; padding: 12px; text-align: left; }
        td { padding: 10px 12px; border-bottom: 1px solid #ECF0F1; }
        tr:hover { background: #F8F9FA; }
        .alert { padding: 16px 20px; border-radius: 8px; margin: 16px 0; border-left: 4px solid; }
        .alert-high { background: #FDEDEE; border-color: #E74C3C; }
        .alert-medium { background: #FEF9E7; border-color: #F39C12; }
        .alert-low { background: #E8F8F5; border-color: #1ABC9C; }
        .tag { display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; margin-left: 8px; }
        .tag-target { background: #D5F5E3; color: #1E8449; }
        .tag-suspect { background: #FDEDEE; color: #C0392B; }
        .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }
        @media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Marketing Performance Report</h1>
            <div>Period: {{start_date}} ~ {{end_date}}</div>
            <div>Generated: {{generated_at}}</div>
        </div>
        
        <div class="section">
            <h2 class="section-title">1. Executive Summary</h2>
            <div class="kpi-grid">
                <div class="kpi-card"><div class="kpi-label">Total Leads</div><div class="kpi-value">{{summary.conversions}}</div></div>
                <div class="kpi-card"><div class="kpi-label">Total Sessions</div><div class="kpi-value">{{summary.sessions}}</div></div>
                <div class="kpi-card"><div class="kpi-label">Conversion Rate</div><div class="kpi-value">{{summary.cvr}}<span class="kpi-unit">%</span></div></div>
                <div class="kpi-card"><div class="kpi-label">Marketing Spend</div><div class="kpi-value">₩{{summary.total_cost}}</div></div>
                <div class="kpi-card"><div class="kpi-label">Cost Per Lead</div><div class="kpi-value">₩{{summary.cpa}}</div></div>
                <div class="kpi-card"><div class="kpi-label">Ads Conversions</div><div class="kpi-value">{{summary.ads_conversions}}</div></div>
            </div>
        </div>
        
        <div class="section">
            <h2 class="section-title">2. Lead Acquisition</h2>
            <table><thead><tr><th>Event</th><th>Count</th></tr></thead><tbody>{{leads_rows}}</tbody></table>
        </div>
        
        <div class="section">
            <h2 class="section-title">3. Channel Performance</h2>
            <table><thead><tr><th>Channel</th><th>Sessions</th><th>Conversions</th><th>CVR</th></tr></thead><tbody>{{channel_rows}}</tbody></table>
        </div>
        
        <div class="section">
            <h2 class="section-title">4. Geographic Distribution</h2>
            <div class="kpi-grid" style="margin-bottom:25px;">
                <div class="kpi-card"><div class="kpi-label">Target Countries</div><div class="kpi-value">{{geo.target_pct}}<span class="kpi-unit">%</span></div></div>
                <div class="kpi-card"><div class="kpi-label">Non-Target Countries</div><div class="kpi-value">{{geo.non_target_pct}}<span class="kpi-unit">%</span></div></div>
            </div>
            <div class="two-col">
                <div><h3 style="margin-bottom:15px;">By Country (Top 20)</h3><table><thead><tr><th>Country</th><th>Sessions</th><th>Conv.</th><th>CVR</th><th>Share</th></tr></thead><tbody>{{country_rows}}</tbody></table></div>
                <div><h3 style="margin-bottom:15px;">By City (Top 20)</h3><table><thead><tr><th>City</th><th>Country</th><th>Conv.</th><th>CVR</th></tr></thead><tbody>{{city_rows}}</tbody></table></div>
            </div>
        </div>
        
        <div class="section">
            <h2 class="section-title">5. Campaign Performance</h2>
            <table><thead><tr><th>Campaign</th><th>Spend (₩)</th><th>Impr.</th><th>Clicks</th><th>CTR</th><th>Conv.</th><th>CVR</th><th>CPA (₩)</th></tr></thead><tbody>{{campaign_rows}}</tbody></table>
        </div>
        
        <div class="section">
            <h2 class="section-title">6. Website Engagement</h2>
            <div class="two-col">
                <div><h3 style="margin-bottom:15px;">Top Pages by Views</h3><table><thead><tr><th>Page</th><th>Views</th><th>Avg. Duration</th></tr></thead><tbody>{{page_rows}}</tbody></table></div>
                <div><h3 style="margin-bottom:15px;">Landing Pages</h3><table><thead><tr><th>Landing Page</th><th>Sessions</th><th>Conv.</th><th>CVR</th></tr></thead><tbody>{{landing_rows}}</tbody></table></div>
            </div>
        </div>
        
        <div class="section">
            <h2 class="section-title">7. Data Quality & Anomalies</h2>
            {{anomaly_rows}}
        </div>
    </div>
</body>
</html>"""


def main():
    load_dotenv()
    property_id = os.getenv("PROPERTY_ID")
    customer_id = os.getenv("CUSTOMER_ID")
    if not property_id or not customer_id:
        print("ERROR: PROPERTY_ID and CUSTOMER_ID required")
        return
    
    end_date = os.getenv("END_DATE", date.today().isoformat())
    start_date = os.getenv("START_DATE", (date.today() - timedelta(days=7)).isoformat())
    
    print(f"Generating report for {start_date} to {end_date}")
    generator = ReportGenerator(property_id, customer_id, start_date, end_date)
    generator.collect_all_data()
    generator.generate_html(f"reports/{end_date}/index.html")
    print(f"\n✅ Done! Open: reports/{end_date}/index.html")


if __name__ == "__main__":
    main()
