"""
HueLight GA4/Google Ads 일일 리포트 생성기
"""

import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.ads.googleads.client import GoogleAdsClient
from jinja2 import Environment, FileSystemLoader, select_autoescape

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib import font_manager


def set_korean_font():
    preferred_fonts = []
    if sys.platform == "darwin":
        preferred_fonts.extend(["AppleGothic", "Apple SD Gothic Neo"])
    elif sys.platform.startswith("win"):
        preferred_fonts.extend(["Malgun Gothic"])
    elif sys.platform.startswith("linux"):
        preferred_fonts.extend(["Noto Sans CJK KR", "NanumGothic"])

    preferred_fonts.extend([
        "Apple SD Gothic Neo",
        "AppleGothic",
        "Noto Sans CJK KR",
        "Noto Sans KR",
        "NanumGothic",
        "Malgun Gothic",
        "DejaVu Sans",
    ])

    available = {font.name for font in font_manager.fontManager.ttflist}
    chosen = next((name for name in preferred_fonts if name in available), None)
    if chosen:
        plt.rcParams["font.family"] = chosen
    plt.rcParams["axes.unicode_minus"] = False


set_korean_font()

GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CREDENTIALS_PATH = ".secrets/ga4.json"
ADS_CREDENTIALS_PATH = ".secrets/google-ads.yaml"

REPORT_TITLE = "HueLight 퍼포먼스 대시보드"
FIXED_START_DATE = "2025-03-18"
TIMEZONE = "Asia/Seoul"
ACCENT_COLOR = "#2563eb"


def seoul_today() -> date:
    return datetime.now(ZoneInfo(TIMEZONE)).date()


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def format_int(value: float) -> str:
    return f"{int(round(value)):,}"


def format_float(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def format_currency(value: float) -> str:
    return f"₩{value:,.0f}"


def format_delta(current: float, previous: float) -> str | None:
    if previous == 0:
        return None
    delta = (current - previous) / previous * 100
    if delta > 0:
        arrow = "↑"
    elif delta < 0:
        arrow = "↓"
    else:
        arrow = "→"
    return f"{arrow}{abs(delta):.0f}%"


def iso_date_range(start: date, end: date) -> list[str]:
    days = (end - start).days + 1
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]


def safe_date_range(start: date, end: date) -> list[str]:
    if start > end:
        return []
    return iso_date_range(start, end)


def parse_ga4_date(value: str) -> str:
    if len(value) == 8:
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


class GA4Client:
    def __init__(self, property_id: str):
        creds = service_account.Credentials.from_service_account_file(
            GA4_CREDENTIALS_PATH, scopes=GA4_SCOPES
        )
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

    def run_query(self, query: str, fallback_query: str | None = None) -> list:
        try:
            response = self.service.search(customer_id=self.customer_id, query=query)
        except Exception:
            if not fallback_query:
                raise
            response = self.service.search(customer_id=self.customer_id, query=fallback_query)
        return list(response)


class ReportGenerator:
    def __init__(self, property_id: str, customer_id: str, start_date: str, end_date: str):
        self.ga4 = GA4Client(property_id)
        self.ads = AdsClient(customer_id)
        self.start_date = start_date
        self.end_date = end_date

    def collect_all_data(self) -> dict:
        print("데이터 수집 중...")
        start = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        all_dates = iso_date_range(start, end)
        last_30_start = max(start, end - timedelta(days=29))
        last_30_dates = iso_date_range(last_30_start, end)
        last_7_end = end - timedelta(days=1)
        last_7_start = max(start, last_7_end - timedelta(days=6))
        last_7_dates = safe_date_range(last_7_start, last_7_end)
        last_30_end = end - timedelta(days=1)
        last_30_complete_start = max(start, last_30_end - timedelta(days=29))
        last_30_complete_dates = safe_date_range(last_30_complete_start, last_30_end)
        prev_7_end = last_7_start - timedelta(days=1)
        prev_7_start = max(start, prev_7_end - timedelta(days=6))
        prev_7_dates = safe_date_range(prev_7_start, prev_7_end) if last_7_dates else []
        prev_30_end = last_30_complete_start - timedelta(days=1)
        prev_30_start = max(start, prev_30_end - timedelta(days=29))
        prev_30_dates = safe_date_range(prev_30_start, prev_30_end) if last_30_complete_dates else []

        ga4_daily, ga4_has_data = self._get_ga4_daily_series(self.start_date, self.end_date, all_dates)
        ads_daily, ads_has_data, has_conv_value = self._get_ads_daily_series(self.start_date, self.end_date, all_dates)

        summary = self._build_summary(
            ga4_daily,
            ads_daily,
            last_7_dates,
            prev_7_dates,
            last_30_complete_dates,
            prev_30_dates,
            all_dates,
            has_conv_value,
        )
        tables = self._build_tables(last_30_start.isoformat(), self.end_date)
        ai_summary = self._build_ai_summary(
            ga4_daily,
            ads_daily,
            last_7_dates,
            prev_7_dates,
            tables.get("top_landing"),
            tables.get("top_campaign"),
        )
        geo_map = self._get_geo_map(last_7_start, last_7_end) if last_7_dates else {
            "has_data": False,
            "chart_json": "[]",
            "start": last_7_start.isoformat(),
            "end": last_7_end.isoformat(),
            "total_active": 0,
            "total_active_display": format_int(0),
            "top10": [],
        }
        keyword_tables = self._get_ads_keyword_tables(last_7_start, last_7_end, last_30_complete_start, last_30_end)
        search_terms = self._get_ads_search_term_waste(last_7_start, last_7_end)
        wasted_summary = self._build_wasted_summary(last_7_dates, ads_daily, keyword_tables, search_terms)
        conversion_definitions = self._get_conversion_definitions(last_30_complete_start, last_30_end)
        landing_page_stats = self._get_ads_landing_page_stats(last_7_start, last_7_end)
        device_stats = self._get_device_stats(last_7_start, last_7_end)
        weekday_stats = self._get_weekday_conversions(last_7_start, last_7_end)
        heatmap_stats = self._get_hour_weekday_heatmap(last_7_start, last_7_end)
        today_line = self._build_today_line(self.end_date, ads_daily, wasted_summary)
        action_cards = self._build_action_cards(keyword_tables, search_terms)
        exec_summary = self._build_executive_summary(
            last_7_dates,
            prev_7_dates,
            ga4_daily,
            ads_daily,
            wasted_summary,
            keyword_tables,
            search_terms,
        )
        extra_chart_specs = self._build_extra_chart_specs(
            ga4_daily,
            ads_daily,
            last_7_dates,
            prev_7_dates,
            keyword_tables,
            search_terms,
            geo_map,
            landing_page_stats,
            device_stats,
            weekday_stats,
            heatmap_stats,
        )

        return {
            "summary": summary,
            "tables": tables,
            "charts": self._build_chart_data(ga4_daily, ads_daily, last_30_dates, ga4_has_data, ads_has_data),
            "ai_summary": ai_summary,
            "geo_map": geo_map,
            "keyword_tables": keyword_tables,
            "search_terms": search_terms,
            "wasted_summary": wasted_summary,
            "conversion_definitions": conversion_definitions,
            "exec_summary": exec_summary,
            "today_line": today_line,
            "action_cards": action_cards,
            "landing_page_stats": landing_page_stats,
            "device_stats": device_stats,
            "weekday_stats": weekday_stats,
            "heatmap_stats": heatmap_stats,
            "extra_chart_specs": extra_chart_specs,
        }

    def _get_ga4_daily_series(self, start_date: str, end_date: str, all_dates: list[str]) -> tuple[dict, bool]:
        rows = self.ga4.run_report(
            ["date"],
            ["sessions", "activeUsers"],
            start_date,
            end_date,
        )
        data = {d: {"sessions": 0, "activeUsers": 0} for d in all_dates}
        for row in rows:
            date_key = parse_ga4_date(row.get("date", ""))
            if date_key not in data:
                continue
            data[date_key]["sessions"] = row.get("sessions", 0)
            data[date_key]["activeUsers"] = row.get("activeUsers", 0)
        return data, bool(rows)

    def _get_ads_daily_series(self, start_date: str, end_date: str, all_dates: list[str]) -> tuple[dict, bool, bool]:
        query_with_value = (
            "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            "FROM campaign "
            f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}' "
            "AND campaign.status != 'REMOVED'"
        )
        query_fallback = (
            "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions "
            "FROM campaign "
            f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}' "
            "AND campaign.status != 'REMOVED'"
        )
        rows = self.ads.run_query(query_with_value, fallback_query=query_fallback)

        data = {
            d: {
                "cost": 0.0,
                "impressions": 0,
                "clicks": 0,
                "conversions": 0.0,
                "conversion_value": 0.0,
            }
            for d in all_dates
        }
        has_conv_value = False
        for row in rows:
            date_key = row.segments.date
            if date_key not in data:
                continue
            data[date_key]["cost"] += row.metrics.cost_micros / 1_000_000
            data[date_key]["impressions"] += row.metrics.impressions
            data[date_key]["clicks"] += row.metrics.clicks
            data[date_key]["conversions"] += row.metrics.conversions
            conv_value = getattr(row.metrics, "conversions_value", None)
            if conv_value is not None:
                has_conv_value = True
                data[date_key]["conversion_value"] += conv_value
        return data, bool(rows), has_conv_value

    def _build_summary(
        self,
        ga4_daily: dict,
        ads_daily: dict,
        last_7_dates: list[str],
        prev_7_dates: list[str],
        last_30_dates: list[str],
        prev_30_dates: list[str],
        all_dates: list[str],
        has_conv_value: bool,
    ) -> dict:
        def sum_series(dates: list[str]) -> dict:
            return {
                "ga4_sessions": sum(ga4_daily[d]["sessions"] for d in dates),
                "ga4_active_users": sum(ga4_daily[d]["activeUsers"] for d in dates),
                "ads_cost": sum(ads_daily[d]["cost"] for d in dates),
                "ads_impressions": sum(ads_daily[d]["impressions"] for d in dates),
                "ads_clicks": sum(ads_daily[d]["clicks"] for d in dates),
                "ads_conversions": sum(ads_daily[d]["conversions"] for d in dates),
                "ads_conversion_value": sum(ads_daily[d]["conversion_value"] for d in dates),
            }

        today_key = self.end_date
        yesterday_key = (datetime.strptime(self.end_date, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()

        yesterday_ga4 = ga4_daily.get(yesterday_key, {"sessions": 0, "activeUsers": 0})
        yesterday_ads = ads_daily.get(yesterday_key, {
            "cost": 0.0,
            "impressions": 0,
            "clicks": 0,
            "conversions": 0.0,
            "conversion_value": 0.0,
        })

        today_ga4 = ga4_daily.get(today_key, {"sessions": 0, "activeUsers": 0})
        today_ads = ads_daily.get(today_key, {
            "cost": 0.0,
            "impressions": 0,
            "clicks": 0,
            "conversions": 0.0,
            "conversion_value": 0.0,
        })

        yesterday_metrics = {
            "ga4_sessions": yesterday_ga4["sessions"],
            "ga4_active_users": yesterday_ga4["activeUsers"],
            "ads_cost": yesterday_ads["cost"],
            "ads_impressions": yesterday_ads["impressions"],
            "ads_clicks": yesterday_ads["clicks"],
            "ads_conversions": yesterday_ads["conversions"],
            "ads_ctr": safe_div(yesterday_ads["clicks"], yesterday_ads["impressions"]) * 100,
            "ads_cpc": safe_div(yesterday_ads["cost"], yesterday_ads["clicks"]),
            "ads_cpa": safe_div(yesterday_ads["cost"], yesterday_ads["conversions"]),
            "ads_roas": safe_div(yesterday_ads["conversion_value"], yesterday_ads["cost"]) if has_conv_value else None,
        }

        today_metrics = {
            "ga4_sessions": today_ga4["sessions"],
            "ga4_active_users": today_ga4["activeUsers"],
            "ads_cost": today_ads["cost"],
            "ads_impressions": today_ads["impressions"],
            "ads_clicks": today_ads["clicks"],
            "ads_conversions": today_ads["conversions"],
            "ads_ctr": safe_div(today_ads["clicks"], today_ads["impressions"]) * 100,
            "ads_cpc": safe_div(today_ads["cost"], today_ads["clicks"]),
            "ads_cpa": safe_div(today_ads["cost"], today_ads["conversions"]),
            "ads_roas": safe_div(today_ads["conversion_value"], today_ads["cost"]) if has_conv_value else None,
        }

        last_7_totals = sum_series(last_7_dates)
        last_7_totals["ads_ctr"] = safe_div(last_7_totals["ads_clicks"], last_7_totals["ads_impressions"]) * 100
        last_7_totals["ads_cpc"] = safe_div(last_7_totals["ads_cost"], last_7_totals["ads_clicks"])
        last_7_totals["ads_cpa"] = safe_div(last_7_totals["ads_cost"], last_7_totals["ads_conversions"])
        last_7_totals["ads_roas"] = (
            safe_div(last_7_totals["ads_conversion_value"], last_7_totals["ads_cost"]) if has_conv_value else None
        )

        prev_7_totals = sum_series(prev_7_dates)
        prev_7_totals["ads_ctr"] = safe_div(prev_7_totals["ads_clicks"], prev_7_totals["ads_impressions"]) * 100
        prev_7_totals["ads_cpc"] = safe_div(prev_7_totals["ads_cost"], prev_7_totals["ads_clicks"])
        prev_7_totals["ads_cpa"] = safe_div(prev_7_totals["ads_cost"], prev_7_totals["ads_conversions"])
        prev_7_totals["ads_roas"] = (
            safe_div(prev_7_totals["ads_conversion_value"], prev_7_totals["ads_cost"]) if has_conv_value else None
        )

        last_30_totals = sum_series(last_30_dates)
        last_30_totals["ads_ctr"] = safe_div(last_30_totals["ads_clicks"], last_30_totals["ads_impressions"]) * 100
        last_30_totals["ads_cpc"] = safe_div(last_30_totals["ads_cost"], last_30_totals["ads_clicks"])
        last_30_totals["ads_cpa"] = safe_div(last_30_totals["ads_cost"], last_30_totals["ads_conversions"])
        last_30_totals["ads_roas"] = (
            safe_div(last_30_totals["ads_conversion_value"], last_30_totals["ads_cost"]) if has_conv_value else None
        )

        prev_30_totals = sum_series(prev_30_dates)
        prev_30_totals["ads_ctr"] = safe_div(prev_30_totals["ads_clicks"], prev_30_totals["ads_impressions"]) * 100
        prev_30_totals["ads_cpc"] = safe_div(prev_30_totals["ads_cost"], prev_30_totals["ads_clicks"])
        prev_30_totals["ads_cpa"] = safe_div(prev_30_totals["ads_cost"], prev_30_totals["ads_conversions"])
        prev_30_totals["ads_roas"] = (
            safe_div(prev_30_totals["ads_conversion_value"], prev_30_totals["ads_cost"]) if has_conv_value else None
        )

        all_totals = sum_series(all_dates)
        all_totals["ads_ctr"] = safe_div(all_totals["ads_clicks"], all_totals["ads_impressions"]) * 100
        all_totals["ads_cpc"] = safe_div(all_totals["ads_cost"], all_totals["ads_clicks"])
        all_totals["ads_cpa"] = safe_div(all_totals["ads_cost"], all_totals["ads_conversions"])
        all_totals["ads_roas"] = (
            safe_div(all_totals["ads_conversion_value"], all_totals["ads_cost"]) if has_conv_value else None
        )

        day_before_key = (datetime.strptime(yesterday_key, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
        day_before_ga4 = ga4_daily.get(day_before_key, {"sessions": 0, "activeUsers": 0})
        day_before_ads = ads_daily.get(day_before_key, {
            "cost": 0.0,
            "impressions": 0,
            "clicks": 0,
            "conversions": 0.0,
            "conversion_value": 0.0,
        })
        day_before_metrics = {
            "ga4_sessions": day_before_ga4["sessions"],
            "ga4_active_users": day_before_ga4["activeUsers"],
            "ads_cost": day_before_ads["cost"],
            "ads_impressions": day_before_ads["impressions"],
            "ads_clicks": day_before_ads["clicks"],
            "ads_conversions": day_before_ads["conversions"],
            "ads_ctr": safe_div(day_before_ads["clicks"], day_before_ads["impressions"]) * 100,
            "ads_cpc": safe_div(day_before_ads["cost"], day_before_ads["clicks"]),
            "ads_cpa": safe_div(day_before_ads["cost"], day_before_ads["conversions"]),
            "ads_roas": safe_div(day_before_ads["conversion_value"], day_before_ads["cost"]) if has_conv_value else None,
        }

        yesterday_cards = self._format_cards_with_delta(yesterday_metrics, day_before_metrics, has_conv_value)
        today_cards = self._format_cards_with_delta(today_metrics, yesterday_metrics, has_conv_value)
        last_7_cards = self._format_cards_with_delta(last_7_totals, prev_7_totals, has_conv_value)
        last_30_cards = self._format_cards_with_delta(last_30_totals, prev_30_totals, has_conv_value)
        all_time_cards = self._format_cards(all_totals, has_conv_value, include_avg=True, days=len(all_dates))

        return {
            "yesterday_cards": yesterday_cards,
            "today_cards": today_cards,
            "last_7_cards": last_7_cards,
            "last_30_cards": last_30_cards,
            "all_time_cards": all_time_cards,
            "total_days": len(all_dates),
            "yesterday_date": yesterday_key,
            "last_7_range": f"{last_7_dates[0]} ~ {last_7_dates[-1]}" if last_7_dates else "-",
            "last_30_range": f"{last_30_dates[0]} ~ {last_30_dates[-1]}" if last_30_dates else "-",
        }

    def _format_cards(self, metrics: dict, has_conv_value: bool, include_avg: bool = False, days: int = 1) -> list[dict]:
        labels = [
            ("ga4_sessions", "GA4 세션", "회"),
            ("ga4_active_users", "GA4 활성 사용자", "명"),
            ("ads_cost", "광고 비용", "원"),
            ("ads_impressions", "광고 노출", "회"),
            ("ads_clicks", "광고 클릭", "회"),
            ("ads_conversions", "광고 전환", "건"),
            ("ads_ctr", "광고 클릭률", "%"),
            ("ads_cpc", "클릭당 비용", "원"),
            ("ads_cpa", "전환당 비용", "원"),
        ]
        if has_conv_value:
            labels.append(("ads_roas", "광고 수익률", "배"))

        cards = []
        for key, label, unit in labels:
            value = metrics.get(key)
            if value is None:
                continue
            if key in {"ads_cost", "ads_cpc", "ads_cpa"}:
                display = format_currency(value)
                unit_text = ""
            elif key == "ads_roas":
                display = format_float(value, 2)
                unit_text = unit
            elif key in {"ads_ctr"}:
                display = format_float(value, 2)
                unit_text = unit
            else:
                display = format_int(value)
                unit_text = unit

            sub = None
            if include_avg and key in {
                "ga4_sessions",
                "ga4_active_users",
                "ads_cost",
                "ads_impressions",
                "ads_clicks",
                "ads_conversions",
            }:
                avg_value = safe_div(metrics.get(key, 0), days)
                if key == "ads_cost":
                    sub = f"평균 {format_currency(avg_value)}/일"
                else:
                    sub = f"평균 {format_float(avg_value, 1)}{unit}/일"

            cards.append({
                "label": label,
                "value": display,
                "unit": unit_text,
                "sub": sub,
            })
        return cards

    def _format_cards_with_delta(
        self,
        metrics: dict,
        compare_metrics: dict | None,
        has_conv_value: bool,
    ) -> list[dict]:
        cards = []
        for card in self._format_cards(metrics, has_conv_value):
            key = None
            for k, label, _ in [
                ("ga4_sessions", "GA4 세션", "회"),
                ("ga4_active_users", "GA4 활성 사용자", "명"),
                ("ads_cost", "광고 비용", "원"),
                ("ads_impressions", "광고 노출", "회"),
                ("ads_clicks", "광고 클릭", "회"),
                ("ads_conversions", "광고 전환", "건"),
                ("ads_ctr", "광고 클릭률", "%"),
                ("ads_cpc", "클릭당 비용", "원"),
                ("ads_cpa", "전환당 비용", "원"),
                ("ads_roas", "광고 수익률", "배"),
            ]:
                if label == card["label"]:
                    key = k
                    break
            delta = None
            if key and compare_metrics is not None:
                current = metrics.get(key, 0)
                previous = compare_metrics.get(key, 0)
                if isinstance(current, (int, float)) and isinstance(previous, (int, float)):
                    delta = format_delta(current, previous)
            card["delta"] = delta
            cards.append(card)
        return cards

    def _build_ai_summary(
        self,
        ga4_daily: dict,
        ads_daily: dict,
        last_7_dates: list[str],
        prev_7_dates: list[str],
        top_landing: dict | None,
        top_campaign: dict | None,
    ) -> dict:
        def sum_ga4(dates: list[str]) -> dict:
            return {
                "sessions": sum(ga4_daily[d]["sessions"] for d in dates) if dates else 0,
                "active_users": sum(ga4_daily[d]["activeUsers"] for d in dates) if dates else 0,
            }

        def sum_ads(dates: list[str]) -> dict:
            return {
                "cost": sum(ads_daily[d]["cost"] for d in dates) if dates else 0.0,
                "impressions": sum(ads_daily[d]["impressions"] for d in dates) if dates else 0,
                "clicks": sum(ads_daily[d]["clicks"] for d in dates) if dates else 0,
                "conversions": sum(ads_daily[d]["conversions"] for d in dates) if dates else 0.0,
            }

        def delta_pct(current: float, prev: float) -> float | None:
            if prev <= 0:
                return None
            return (current - prev) / prev * 100

        def trend_text(current: float, prev: float, unit: str = "", is_currency: bool = False) -> str:
            if is_currency:
                current_text = format_currency(current)
            elif unit == "%":
                current_text = f"{format_float(current, 2)}%"
            else:
                current_text = format_int(current)
                if unit:
                    current_text = f"{current_text}{unit}"
            delta = delta_pct(current, prev)
            if delta is None:
                return f"{current_text} (비교 데이터 없음)"
            direction = "증가" if delta >= 0 else "감소"
            return f"{current_text} (전주 대비 {abs(delta):.1f}% {direction})"

        last_7_ga4 = sum_ga4(last_7_dates)
        prev_7_ga4 = sum_ga4(prev_7_dates)
        last_7_ads = sum_ads(last_7_dates)
        prev_7_ads = sum_ads(prev_7_dates)

        last_ctr = safe_div(last_7_ads["clicks"], last_7_ads["impressions"]) * 100
        prev_ctr = safe_div(prev_7_ads["clicks"], prev_7_ads["impressions"]) * 100
        last_cpc = safe_div(last_7_ads["cost"], last_7_ads["clicks"])
        prev_cpc = safe_div(prev_7_ads["cost"], prev_7_ads["clicks"])
        last_cpa = safe_div(last_7_ads["cost"], last_7_ads["conversions"])
        prev_cpa = safe_div(prev_7_ads["cost"], prev_7_ads["conversions"])

        summary_parts = [
            f"세션 {trend_text(last_7_ga4['sessions'], prev_7_ga4['sessions'], unit='회')}",
            f"활성 사용자 {trend_text(last_7_ga4['active_users'], prev_7_ga4['active_users'], unit='명')}",
            f"광고 비용 {trend_text(last_7_ads['cost'], prev_7_ads['cost'], is_currency=True)}",
            f"전환 {trend_text(last_7_ads['conversions'], prev_7_ads['conversions'], unit='건')}",
        ]
        summary_text = (
            f"최근 7일 기준 {', '.join(summary_parts)}입니다. "
            "전주 대비 변화를 기준으로 핵심 지표를 요약했습니다."
        )

        landing_text = "데이터 없음"
        if top_landing:
            landing_text = f"{top_landing.get('landingPagePlusQueryString', '-')[:60]} (세션 {format_int(top_landing.get('sessions', 0))})"

        campaign_text = "데이터 없음"
        if top_campaign:
            campaign_text = f"{top_campaign.get('campaign', '-')[:60]} (비용 {format_currency(top_campaign.get('cost', 0.0))})"

        insights = [
            f"세션/활성 사용자: {trend_text(last_7_ga4['sessions'], prev_7_ga4['sessions'], unit='회')} / "
            f"{trend_text(last_7_ga4['active_users'], prev_7_ga4['active_users'], unit='명')}",
            f"광고 효율: CTR {trend_text(last_ctr, prev_ctr, unit='%')}, "
            f"CPC {trend_text(last_cpc, prev_cpc, is_currency=True)}, "
            f"CPA {trend_text(last_cpa, prev_cpa, is_currency=True)}",
            f"상위 성과: 랜딩페이지 {landing_text}, 캠페인 {campaign_text}",
        ]

        return {
            "text": summary_text,
            "insights": insights,
        }

    def _get_geo_map(self, start_date: date, end_date: date) -> dict:
        start = start_date.isoformat()
        end = end_date.isoformat()
        if start_date > end_date:
            return {
                "has_data": False,
                "chart_json": "[]",
                "start": start,
                "end": end,
                "total_active": 0,
                "total_active_display": format_int(0),
            }
        rows = self.ga4.run_report(
            ["country"],
            ["activeUsers", "sessions"],
            start,
            end,
            limit=200,
        )
        data_rows = []
        total_active = 0
        for row in rows:
            country = row.get("country", "")
            active_users = row.get("activeUsers", 0)
            try:
                active_users = int(active_users)
            except ValueError:
                active_users = 0
            if not country:
                continue
            total_active += active_users
            data_rows.append([country, active_users])
        data_rows.sort(key=lambda x: x[1], reverse=True)
        top10 = data_rows[:10]
        chart_data = [["국가", "활성 사용자"]] + data_rows
        return {
            "has_data": bool(data_rows),
            "chart_json": json.dumps(chart_data, ensure_ascii=False),
            "start": start,
            "end": end,
            "total_active": total_active,
            "total_active_display": format_int(total_active),
            "top10": [
                {"country": row[0], "active": row[1], "active_display": format_int(row[1])}
                for row in top10
            ],
        }

    def _get_ads_keyword_rows(self, start_date: date, end_date: date) -> list[dict]:
        if start_date > end_date:
            return []
        start = start_date.isoformat()
        end = end_date.isoformat()
        query = (
            "SELECT campaign.name, ad_group.name, ad_group_criterion.keyword.text, "
            "ad_group_criterion.keyword.match_type, metrics.impressions, metrics.clicks, "
            "metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, "
            "metrics.cost_per_conversion "
            "FROM keyword_view "
            f"WHERE segments.date BETWEEN '{start}' AND '{end}' "
            "AND campaign.advertising_channel_type = 'SEARCH' "
            "AND ad_group_criterion.status != 'REMOVED'"
        )
        rows = self.ads.run_query(query)
        results = []
        for row in rows:
            keyword = row.ad_group_criterion.keyword
            match_type = str(keyword.match_type)
            results.append({
                "campaign": row.campaign.name,
                "ad_group": row.ad_group.name,
                "keyword": keyword.text,
                "match_type": match_type,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "ctr": row.metrics.ctr,
                "avg_cpc_micros": row.metrics.average_cpc,
                "cost_micros": row.metrics.cost_micros,
                "conversions": row.metrics.conversions,
                "cpa_micros": row.metrics.cost_per_conversion,
            })
        return results

    def _format_match_type(self, match_type: str) -> str:
        mapping = {
            "BROAD": "확장",
            "PHRASE": "구문",
            "EXACT": "일치",
        }
        return mapping.get(match_type, "기타")

    def _short_label(self, value: str, limit: int = 20) -> str:
        if len(value) <= limit:
            return value
        return f"{value[:limit]}…"

    def _format_keyword_table(self, rows: list[dict]) -> dict:
        headers = [
            "캠페인",
            "광고그룹",
            "키워드",
            "매칭",
            "노출",
            "클릭",
            "클릭률",
            "클릭당 비용",
            "비용",
            "전환",
            "전환율",
            "전환당 비용",
        ]
        table_rows = []
        for row in rows:
            cost = row["cost_micros"] / 1_000_000
            conversions = row["conversions"]
            ctr = row["ctr"] * 100 if row["ctr"] else safe_div(row["clicks"], row["impressions"]) * 100
            avg_cpc = row["avg_cpc_micros"] / 1_000_000 if row["avg_cpc_micros"] else safe_div(cost, row["clicks"])
            cpa = row["cpa_micros"] / 1_000_000 if row["cpa_micros"] else safe_div(cost, conversions)
            conv_rate = safe_div(conversions, row["clicks"]) * 100
            table_rows.append([
                row["campaign"],
                row["ad_group"],
                row["keyword"],
                self._format_match_type(row["match_type"]),
                format_int(row["impressions"]),
                format_int(row["clicks"]),
                f"{format_float(ctr, 2)}%",
                format_currency(avg_cpc),
                format_currency(cost),
                format_float(conversions, 1),
                f"{format_float(conv_rate, 2)}%",
                format_currency(cpa) if conversions else "-",
            ])
        return {"headers": headers, "rows": table_rows}

    def _format_keyword_waste_table(self, rows: list[dict]) -> dict:
        headers = [
            "캠페인",
            "광고그룹",
            "키워드",
            "매칭",
            "비용",
            "클릭",
            "클릭률",
            "클릭당 비용",
        ]
        table_rows = []
        for row in rows:
            cost = row["cost_micros"] / 1_000_000
            ctr = row["ctr"] * 100 if row["ctr"] else safe_div(row["clicks"], row["impressions"]) * 100
            avg_cpc = row["avg_cpc_micros"] / 1_000_000 if row["avg_cpc_micros"] else safe_div(cost, row["clicks"])
            table_rows.append([
                row["campaign"],
                row["ad_group"],
                row["keyword"],
                self._format_match_type(row["match_type"]),
                format_currency(cost),
                format_int(row["clicks"]),
                f"{format_float(ctr, 2)}%",
                format_currency(avg_cpc),
            ])
        return {"headers": headers, "rows": table_rows}

    def _get_ads_keyword_tables(
        self,
        last_7_start: date,
        last_7_end: date,
        last_30_start: date,
        last_30_end: date,
    ) -> dict:
        rows_7 = self._get_ads_keyword_rows(last_7_start, last_7_end)
        rows_30 = self._get_ads_keyword_rows(last_30_start, last_30_end)

        rows_7_sorted = sorted(rows_7, key=lambda r: r["cost_micros"], reverse=True)[:20]
        rows_30_sorted = sorted(rows_30, key=lambda r: r["cost_micros"], reverse=True)[:20]
        wasted_7 = [r for r in rows_7 if r["conversions"] == 0 and r["cost_micros"] > 0]
        wasted_7_sorted = sorted(wasted_7, key=lambda r: r["cost_micros"], reverse=True)[:20]

        return {
            "last_7": {
                "start": last_7_start.isoformat(),
                "end": last_7_end.isoformat(),
                "top_cost": self._format_keyword_table(rows_7_sorted),
                "wasted": self._format_keyword_waste_table(wasted_7_sorted),
                "rows": rows_7,
            },
            "last_30": {
                "start": last_30_start.isoformat(),
                "end": last_30_end.isoformat(),
                "top_cost": self._format_keyword_table(rows_30_sorted),
                "rows": rows_30,
            },
        }

    def _get_ads_search_term_waste(self, start_date: date, end_date: date) -> dict:
        if start_date > end_date:
            return {"start": start_date.isoformat(), "end": end_date.isoformat(), "rows": [], "table": {"headers": [], "rows": []}}
        start = start_date.isoformat()
        end = end_date.isoformat()
        query = (
            "SELECT campaign.name, ad_group.name, search_term_view.search_term, "
            "metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.cost_per_conversion "
            "FROM search_term_view "
            f"WHERE segments.date BETWEEN '{start}' AND '{end}' "
            "AND campaign.advertising_channel_type = 'SEARCH' "
            "AND metrics.conversions = 0 "
            "AND metrics.cost_micros > 0"
        )
        rows = self.ads.run_query(query)
        results = []
        for row in rows:
            results.append({
                "campaign": row.campaign.name,
                "ad_group": row.ad_group.name,
                "term": row.search_term_view.search_term,
                "clicks": row.metrics.clicks,
                "cost_micros": row.metrics.cost_micros,
                "conversions": row.metrics.conversions,
                "cpa_micros": row.metrics.cost_per_conversion,
            })
        results_sorted = sorted(results, key=lambda r: r["cost_micros"], reverse=True)[:20]
        table_rows = []
        for row in results_sorted:
            cost = row["cost_micros"] / 1_000_000
            cpa = row["cpa_micros"] / 1_000_000 if row["cpa_micros"] else safe_div(cost, row["conversions"])
            table_rows.append([
                row["campaign"],
                row["ad_group"],
                row["term"],
                format_int(row["clicks"]),
                format_currency(cost),
                format_float(row["conversions"], 1),
                format_currency(cpa) if row["conversions"] else "-",
            ])
        return {
            "start": start,
            "end": end,
            "rows": results,
            "table": {
                "headers": ["캠페인", "광고그룹", "검색어", "클릭", "비용", "전환", "전환당 비용"],
                "rows": table_rows,
            },
        }

    def _get_ads_landing_page_stats(self, start_date: date, end_date: date) -> list[dict]:
        if start_date > end_date:
            return []
        query = (
            "SELECT campaign.advertising_channel_type, landing_page_view.unexpanded_final_url, "
            "metrics.conversions, metrics.cost_micros, metrics.cost_per_conversion "
            "FROM landing_page_view "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}' "
            "AND campaign.advertising_channel_type = 'SEARCH'"
        )
        try:
            rows = self.ads.run_query(query)
        except Exception:
            return []
        results = []
        for row in rows:
            url = row.landing_page_view.unexpanded_final_url or ""
            results.append({
                "url": url,
                "conversions": row.metrics.conversions,
                "cost_micros": row.metrics.cost_micros,
                "cpa_micros": row.metrics.cost_per_conversion,
            })
        return results

    def _get_device_stats(self, start_date: date, end_date: date) -> list[dict]:
        if start_date > end_date:
            return []
        query = (
            "SELECT segments.device, metrics.conversions, metrics.cost_micros "
            "FROM customer "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
        )
        try:
            rows = self.ads.run_query(query)
        except Exception:
            return []
        mapping = {
            "MOBILE": "모바일",
            "DESKTOP": "데스크톱",
            "TABLET": "태블릿",
        }
        results = []
        for row in rows:
            device = mapping.get(str(row.segments.device), "기타")
            results.append({
                "device": device,
                "conversions": row.metrics.conversions,
                "cost_micros": row.metrics.cost_micros,
            })
        return results

    def _get_weekday_conversions(self, start_date: date, end_date: date) -> list[dict]:
        if start_date > end_date:
            return []
        query = (
            "SELECT segments.day_of_week, metrics.conversions "
            "FROM customer "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
        )
        try:
            rows = self.ads.run_query(query)
        except Exception:
            return []
        mapping = {
            "MONDAY": "월",
            "TUESDAY": "화",
            "WEDNESDAY": "수",
            "THURSDAY": "목",
            "FRIDAY": "금",
            "SATURDAY": "토",
            "SUNDAY": "일",
        }
        results = []
        for row in rows:
            results.append({
                "weekday": mapping.get(str(row.segments.day_of_week), "기타"),
                "conversions": row.metrics.conversions,
            })
        return results

    def _get_hour_weekday_heatmap(self, start_date: date, end_date: date) -> dict:
        if start_date > end_date:
            return {"labels": [], "matrix": []}
        query = (
            "SELECT segments.day_of_week, segments.hour, metrics.conversions "
            "FROM customer "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
        )
        try:
            rows = self.ads.run_query(query)
        except Exception:
            return {"labels": [], "matrix": []}
        mapping = {
            "MONDAY": "월",
            "TUESDAY": "화",
            "WEDNESDAY": "수",
            "THURSDAY": "목",
            "FRIDAY": "금",
            "SATURDAY": "토",
            "SUNDAY": "일",
        }
        weekdays = ["월", "화", "수", "목", "금", "토", "일"]
        matrix = [[0 for _ in range(24)] for _ in weekdays]
        for row in rows:
            day = mapping.get(str(row.segments.day_of_week))
            hour = row.segments.hour
            if day is None:
                continue
            try:
                hour = int(hour)
            except Exception:
                continue
            if 0 <= hour <= 23:
                matrix[weekdays.index(day)][hour] += row.metrics.conversions
        return {"labels": weekdays, "matrix": matrix}

    def _build_today_line(self, today_key: str, ads_daily: dict, wasted_summary: dict) -> str:
        today_ads = ads_daily.get(today_key, {"cost": 0.0, "conversions": 0.0})
        top_item = wasted_summary["top_items"][0] if wasted_summary["top_items"] else "낭비 항목 없음"
        return (
            f"오늘 광고는 전환 {format_float(today_ads['conversions'], 1)}건, "
            f"비용 {format_currency(today_ads['cost'])}, "
            f"낭비 키워드 {top_item}가 큼."
        )

    def _build_action_cards(self, keyword_tables: dict, search_terms: dict) -> list[dict]:
        wasted_items = search_terms["rows"] if search_terms["rows"] else [
            row for row in keyword_tables["last_7"]["rows"]
            if row["conversions"] == 0 and row["cost_micros"] > 0
        ]
        pause_candidates = ", ".join(
            self._short_label(item.get("term") or item.get("keyword") or "낭비 항목")
            for item in wasted_items[:3]
        ) or "없음"

        negative_candidates = ", ".join(
            self._short_label(item.get("term") or "")
            for item in search_terms["rows"][:3]
        ) or "데이터 없음"

        best_keywords = []
        for row in keyword_tables["last_7"]["rows"]:
            if row["conversions"] > 0:
                cost = row["cost_micros"] / 1_000_000
                cpa = safe_div(cost, row["conversions"])
                best_keywords.append((row["keyword"], cpa))
        best_keywords.sort(key=lambda x: x[1])
        budget_candidates = ", ".join(
            f"{self._short_label(name)}({format_currency(cpa)})"
            for name, cpa in best_keywords[:3]
        ) or "데이터 없음"

        return [
            {"title": "Pause 후보", "desc": pause_candidates},
            {"title": "네거티브 후보", "desc": negative_candidates},
            {"title": "예산 강화 후보", "desc": budget_candidates},
        ]

    def _build_wasted_summary(self, last_7_dates: list[str], ads_daily: dict, keyword_tables: dict, search_terms: dict) -> dict:
        total_cost = sum(ads_daily[d]["cost"] for d in last_7_dates) if last_7_dates else 0.0
        wasted_cost = 0.0
        source = "검색어"
        wasted_items = []
        if search_terms["rows"]:
            wasted_items = sorted(search_terms["rows"], key=lambda r: r["cost_micros"], reverse=True)
            wasted_cost = sum(row["cost_micros"] for row in wasted_items) / 1_000_000
        else:
            source = "키워드"
            wasted_items = [
                row for row in keyword_tables["last_7"]["rows"]
                if row["conversions"] == 0 and row["cost_micros"] > 0
            ]
            wasted_items = sorted(wasted_items, key=lambda r: r["cost_micros"], reverse=True)
            wasted_cost = sum(row["cost_micros"] for row in wasted_items) / 1_000_000

        wasted_share = safe_div(wasted_cost, total_cost) * 100 if total_cost else 0.0
        top_items = []
        top_sum = 0.0
        for item in wasted_items[:3]:
            label = item.get("term") or item.get("keyword") or "낭비 항목"
            top_items.append(self._short_label(label))
            top_sum += item["cost_micros"] / 1_000_000
        return {
            "start": search_terms["start"],
            "end": search_terms["end"],
            "total_cost": total_cost,
            "wasted_cost": wasted_cost,
            "wasted_share": wasted_share,
            "source": source,
            "total_cost_display": format_currency(total_cost),
            "wasted_cost_display": format_currency(wasted_cost),
            "wasted_share_display": f"{format_float(wasted_share, 1)}%",
            "top_items": top_items,
            "top_sum_display": format_currency(top_sum),
            "top_count": len(top_items),
        }

    def _get_conversion_definitions(self, start_date: date, end_date: date) -> dict:
        def format_type(value: str) -> str:
            mapping = {
                "WEBPAGE": "웹페이지",
                "UPLOAD_CLICKS": "오프라인 클릭",
                "UPLOAD_CALLS": "오프라인 전화",
                "UPLOAD_CALLS_CONVERSION": "오프라인 전화 전환",
                "APP_DOWNLOAD": "앱 다운로드",
                "APP_INSTALLS": "앱 설치",
                "APP_IN_APP_ACTION": "앱 내 전환",
                "PHONE_CALL_LEAD": "전화 리드",
                "SUBMIT_LEAD_FORM": "리드 폼 제출",
                "STORE_VISITS": "매장 방문",
                "IMPORT": "가져오기",
            }
            return mapping.get(value, "기타")

        def format_category(value: str) -> str:
            mapping = {
                "DEFAULT": "기본",
                "PURCHASE": "구매",
                "SUBMIT_LEAD_FORM": "리드",
                "CONTACT": "문의",
                "PAGE_VIEW": "페이지뷰",
                "SIGNUP": "가입",
                "DOWNLOAD": "다운로드",
                "ADD_TO_CART": "장바구니",
                "BEGIN_CHECKOUT": "결제 시작",
                "SUBSCRIBE": "구독",
            }
            return mapping.get(value, "기타")

        actions_query = (
            "SELECT conversion_action.name, conversion_action.type, conversion_action.category, "
            "conversion_action.status, conversion_action.primary_for_goal "
            "FROM conversion_action "
            "WHERE conversion_action.status = 'ENABLED'"
        )
        action_rows = self.ads.run_query(actions_query)
        actions = []
        for row in action_rows:
            action = row.conversion_action
            actions.append({
                "name": action.name,
                "type": format_type(str(action.type)),
                "category": format_category(str(action.category)),
                "status": "사용",
                "primary": "예" if action.primary_for_goal else "아니오",
            })

        conversions_query = (
            "SELECT segments.conversion_action_name, metrics.conversions "
            "FROM customer "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
        )
        conversions_fallback = (
            "SELECT segments.conversion_action_name, metrics.conversions "
            "FROM campaign "
            f"WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}' "
            "AND campaign.status != 'REMOVED'"
        )
        conversion_rows = self.ads.run_query(conversions_query, fallback_query=conversions_fallback)
        conv_by_action = {}
        for row in conversion_rows:
            name = row.segments.conversion_action_name
            if not name:
                continue
            if name not in conv_by_action:
                conv_by_action[name] = {"conversions": 0.0}
            conv_by_action[name]["conversions"] += row.metrics.conversions

        conv_rows = []
        for name, data in conv_by_action.items():
            conv_rows.append({
                "name": name,
                "conversions": data["conversions"],
            })
        conv_rows.sort(key=lambda r: r["conversions"], reverse=True)
        conv_table_rows = [
            [
                row["name"],
                format_float(row["conversions"], 1),
            ]
            for row in conv_rows[:20]
        ]

        return {
            "actions": {
                "headers": ["전환 액션", "유형", "카테고리", "상태", "기본 목표"],
                "rows": [
                    [action["name"], action["type"], action["category"], action["status"], action["primary"]]
                    for action in actions
                ],
            },
            "performance": {
                "headers": ["전환 액션", "전환"],
                "rows": conv_table_rows,
            },
        }

    def _build_executive_summary(
        self,
        last_7_dates: list[str],
        prev_7_dates: list[str],
        ga4_daily: dict,
        ads_daily: dict,
        wasted_summary: dict,
        keyword_tables: dict,
        search_terms: dict,
    ) -> dict:
        if not last_7_dates:
            date_range = "- ~ -"
        else:
            date_range = f"{last_7_dates[0]} ~ {last_7_dates[-1]}"

        def sum_ga4(dates: list[str]) -> tuple[float, float]:
            sessions = sum(ga4_daily[d]["sessions"] for d in dates) if dates else 0
            active = sum(ga4_daily[d]["activeUsers"] for d in dates) if dates else 0
            return sessions, active

        def sum_ads(dates: list[str]) -> tuple[float, float]:
            cost = sum(ads_daily[d]["cost"] for d in dates) if dates else 0.0
            conversions = sum(ads_daily[d]["conversions"] for d in dates) if dates else 0.0
            return cost, conversions

        def delta_pct(current: float, prev: float) -> str:
            if prev <= 0:
                return "n/a"
            delta = (current - prev) / prev * 100
            sign = "+" if delta >= 0 else ""
            return f"{sign}{delta:.0f}%"

        last_sessions, last_active = sum_ga4(last_7_dates)
        prev_sessions, prev_active = sum_ga4(prev_7_dates)
        last_cost, last_conversions = sum_ads(last_7_dates)
        prev_cost, prev_conversions = sum_ads(prev_7_dates)
        last_cpa = safe_div(last_cost, last_conversions)
        prev_cpa = safe_div(prev_cost, prev_conversions)

        change_line = (
            f"지난 7일 vs 전주: 세션 {delta_pct(last_sessions, prev_sessions)}, "
            f"활성 {delta_pct(last_active, prev_active)}, "
            f"비용 {delta_pct(last_cost, prev_cost)}, "
            f"전환 {delta_pct(last_conversions, prev_conversions)}, "
            f"전환당 비용 {delta_pct(last_cpa, prev_cpa)}"
        )

        wasted_count = wasted_summary["top_count"]
        wasted_sum_line = (
            f"낭비 경고: 전환 0 비용 TOP {wasted_count}개 합계 {wasted_summary['top_sum_display']}"
            if wasted_count
            else "낭비 경고: 전환 0 비용 항목이 없습니다."
        )

        pause_items = []
        wasted_items = search_terms["rows"] if search_terms["rows"] else [
            row for row in keyword_tables["last_7"]["rows"]
            if row["conversions"] == 0 and row["cost_micros"] > 0
        ]
        for item in wasted_items[:3]:
            label = item.get("term") or item.get("keyword") or "낭비 항목"
            pause_items.append(self._short_label(label))
        pause_line = "즉시 조치: Pause 후보 3개: " + (", ".join(pause_items) if pause_items else "없음")

        return {
            "range": date_range,
            "lines": [change_line, wasted_sum_line, pause_line],
        }

    def _build_extra_chart_specs(
        self,
        ga4_daily: dict,
        ads_daily: dict,
        last_7_dates: list[str],
        prev_7_dates: list[str],
        keyword_tables: dict,
        search_terms: dict,
        geo_map: dict,
        landing_page_stats: list[dict],
        device_stats: list[dict],
        weekday_stats: list[dict],
        heatmap_stats: dict,
    ) -> dict:
        specs: dict[str, dict] = {}

        def sum_ga4(dates: list[str]) -> tuple[float, float]:
            sessions = sum(ga4_daily[d]["sessions"] for d in dates) if dates else 0
            active = sum(ga4_daily[d]["activeUsers"] for d in dates) if dates else 0
            return sessions, active

        def sum_ads(dates: list[str]) -> tuple[float, float]:
            cost = sum(ads_daily[d]["cost"] for d in dates) if dates else 0.0
            conversions = sum(ads_daily[d]["conversions"] for d in dates) if dates else 0.0
            return cost, conversions

        if last_7_dates and prev_7_dates:
            last_sessions, last_active = sum_ga4(last_7_dates)
            prev_sessions, prev_active = sum_ga4(prev_7_dates)
            last_cost, last_conversions = sum_ads(last_7_dates)
            prev_cost, prev_conversions = sum_ads(prev_7_dates)
            last_cpa = safe_div(last_cost, last_conversions)
            prev_cpa = safe_div(prev_cost, prev_conversions)
            values = [
                (last_sessions, prev_sessions),
                (last_active, prev_active),
                (last_cost, prev_cost),
                (last_conversions, prev_conversions),
                (last_cpa, prev_cpa),
            ]
            deltas = []
            for current, previous in values:
                if previous == 0:
                    deltas.append(0)
                else:
                    deltas.append((current - previous) / previous * 100)
            specs["week_scoreboard"] = {
                "type": "bar",
                "title": "이번 주 한 장 요약 (전주 대비 %)",
                "labels": ["세션", "활성", "비용", "전환", "전환당 비용"],
                "values": deltas,
                "has_data": True,
            }

        if geo_map.get("top10"):
            labels = [row["country"] for row in geo_map["top10"]]
            values = [row["active"] for row in geo_map["top10"]]
            specs["countries_top10"] = {
                "type": "bar",
                "title": "국가별 상위 10",
                "labels": labels,
                "values": values,
                "has_data": True,
            }

        wasted_keywords = [
            row for row in keyword_tables["last_7"]["rows"]
            if row["conversions"] == 0 and row["cost_micros"] > 0
        ]
        wasted_keywords = sorted(wasted_keywords, key=lambda r: r["cost_micros"], reverse=True)[:10]
        if wasted_keywords:
            specs["waste_keywords_top10"] = {
                "type": "barh",
                "title": "전환 0 키워드 비용 TOP 10",
                "labels": [self._short_label(row["keyword"]) for row in wasted_keywords],
                "values": [row["cost_micros"] / 1_000_000 for row in wasted_keywords],
                "has_data": True,
            }

        wasted_queries = sorted(search_terms["rows"], key=lambda r: r["cost_micros"], reverse=True)[:10]
        if wasted_queries:
            specs["waste_queries_top10"] = {
                "type": "barh",
                "title": "전환 0 검색어 비용 TOP 10",
                "labels": [self._short_label(row["term"]) for row in wasted_queries],
                "values": [row["cost_micros"] / 1_000_000 for row in wasted_queries],
                "has_data": True,
            }

        best_keywords = []
        for row in keyword_tables["last_7"]["rows"]:
            if row["conversions"] > 0:
                cost = row["cost_micros"] / 1_000_000
                cpa = safe_div(cost, row["conversions"])
                best_keywords.append((row["keyword"], cpa))
        best_keywords.sort(key=lambda x: x[1])
        if best_keywords:
            top = best_keywords[:10]
            specs["growth_keywords_cpa_top10"] = {
                "type": "barh",
                "title": "전환당 비용 낮은 키워드 TOP 10",
                "labels": [self._short_label(row[0]) for row in top],
                "values": [row[1] for row in top],
                "has_data": True,
            }

        landing_rows = [
            row for row in landing_page_stats
            if row["conversions"] > 0 and row["cost_micros"] > 0
        ]
        landing_rows.sort(key=lambda r: r["conversions"], reverse=True)
        if landing_rows:
            top = landing_rows[:10]
            specs["growth_landing_cpa_top10"] = {
                "type": "dual_bar",
                "title": "랜딩페이지 전환/전환당 비용 TOP 10",
                "labels": [self._short_label(row["url"]) for row in top],
                "values_left": [row["conversions"] for row in top],
                "values_right": [row["cost_micros"] / 1_000_000 / row["conversions"] for row in top],
                "has_data": True,
            }

        if device_stats:
            device_map = {}
            for row in device_stats:
                device = row["device"]
                if device not in device_map:
                    device_map[device] = {"conversions": 0.0, "cost_micros": 0}
                device_map[device]["conversions"] += row["conversions"]
                device_map[device]["cost_micros"] += row["cost_micros"]
            labels = list(device_map.keys())
            conversions = [device_map[d]["conversions"] for d in labels]
            cpa = [
                safe_div(device_map[d]["cost_micros"] / 1_000_000, device_map[d]["conversions"])
                for d in labels
            ]
            specs["device_cpa_compare"] = {
                "type": "device_compare",
                "title": "기기별 전환/전환당 비용",
                "labels": labels,
                "values_left": conversions,
                "values_right": cpa,
                "has_data": True,
            }

        if weekday_stats:
            weekday_order = ["월", "화", "수", "목", "금", "토", "일"]
            values_map = {row["weekday"]: row["conversions"] for row in weekday_stats}
            specs["weekday_conversions"] = {
                "type": "bar",
                "title": "요일별 전환",
                "labels": weekday_order,
                "values": [values_map.get(day, 0) for day in weekday_order],
                "has_data": True,
            }

        if heatmap_stats.get("matrix"):
            total = sum(sum(row) for row in heatmap_stats["matrix"])
            if total > 0:
                specs["hour_weekday_heatmap"] = {
                    "type": "heatmap",
                    "title": "시간대별 전환(요일)",
                    "labels": heatmap_stats["labels"],
                    "matrix": heatmap_stats["matrix"],
                    "has_data": True,
                }

        return specs

    def _build_tables(self, start_date: str, end_date: str) -> dict:
        landing_rows = self.ga4.run_report(
            ["landingPagePlusQueryString"],
            ["sessions", "activeUsers"],
            start_date,
            end_date,
            limit=100,
        )
        landing_rows.sort(key=lambda x: x.get("sessions", 0), reverse=True)
        landing_rows = landing_rows[:10]
        top_landing = landing_rows[0] if landing_rows else None

        source_rows = self.ga4.run_report(
            ["sessionSource", "sessionMedium"],
            ["sessions", "activeUsers"],
            start_date,
            end_date,
            limit=200,
        )
        source_rows.sort(key=lambda x: x.get("sessions", 0), reverse=True)
        source_rows = source_rows[:10]

        campaign_query = (
            "SELECT campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions "
            "FROM campaign "
            f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}' "
            "AND campaign.status != 'REMOVED'"
        )
        campaign_rows = self.ads.run_query(campaign_query)

        campaigns = {}
        for row in campaign_rows:
            name = row.campaign.name
            if name not in campaigns:
                campaigns[name] = {
                    "campaign": name,
                    "impressions": 0,
                    "clicks": 0,
                    "cost": 0.0,
                    "conversions": 0.0,
                }
            campaigns[name]["impressions"] += row.metrics.impressions
            campaigns[name]["clicks"] += row.metrics.clicks
            campaigns[name]["cost"] += row.metrics.cost_micros / 1_000_000
            campaigns[name]["conversions"] += row.metrics.conversions

        campaign_list = []
        for campaign in campaigns.values():
            campaign["ctr"] = safe_div(campaign["clicks"], campaign["impressions"]) * 100
            campaign["cpc"] = safe_div(campaign["cost"], campaign["clicks"])
            campaign["cpa"] = safe_div(campaign["cost"], campaign["conversions"])
            campaign_list.append(campaign)
        campaign_list.sort(key=lambda x: x["cost"], reverse=True)
        campaign_list = campaign_list[:10]
        top_campaign = campaign_list[0] if campaign_list else None

        return {
            "landing_pages": {
                "headers": ["랜딩페이지", "세션", "활성 사용자"],
                "rows": [
                    [
                        row.get("landingPagePlusQueryString", "")[:80],
                        format_int(row.get("sessions", 0)),
                        format_int(row.get("activeUsers", 0)),
                    ]
                    for row in landing_rows
                ],
            },
            "source_medium": {
                "headers": ["소스", "미디엄", "세션", "활성 사용자"],
                "rows": [
                    [
                        row.get("sessionSource", ""),
                        row.get("sessionMedium", ""),
                        format_int(row.get("sessions", 0)),
                        format_int(row.get("activeUsers", 0)),
                    ]
                    for row in source_rows
                ],
            },
            "ads_campaigns": {
                "headers": ["캠페인", "비용", "노출", "클릭", "전환", "클릭률", "클릭당 비용", "전환당 비용"],
                "rows": [
                    [
                        row["campaign"],
                        format_currency(row["cost"]),
                        format_int(row["impressions"]),
                        format_int(row["clicks"]),
                        format_float(row["conversions"], 1),
                        f"{format_float(row['ctr'], 2)}%",
                        format_currency(row["cpc"]),
                        format_currency(row["cpa"]),
                    ]
                    for row in campaign_list
                ],
            },
            "top_landing": top_landing,
            "top_campaign": top_campaign,
            "campaigns_raw": campaign_list,
        }

    def _build_chart_data(
        self,
        ga4_daily: dict,
        ads_daily: dict,
        last_30_dates: list[str],
        ga4_has_data: bool,
        ads_has_data: bool,
    ) -> list[dict]:
        charts = []
        chart_specs = [
            (
                "ga4_sessions_30d.png",
                "GA4 세션 추이 (최근 30일)",
                [ga4_daily[d]["sessions"] for d in last_30_dates],
                "세션",
                "count",
                ga4_has_data,
            ),
            (
                "ga4_active_users_30d.png",
                "GA4 활성 사용자 추이 (최근 30일)",
                [ga4_daily[d]["activeUsers"] for d in last_30_dates],
                "활성 사용자",
                "count",
                ga4_has_data,
            ),
            (
                "ads_cost_30d.png",
                "광고 비용 추이 (최근 30일)",
                [ads_daily[d]["cost"] for d in last_30_dates],
                "비용",
                "currency",
                ads_has_data,
            ),
            (
                "ads_conversions_30d.png",
                "광고 전환 추이 (최근 30일)",
                [ads_daily[d]["conversions"] for d in last_30_dates],
                "전환",
                "count",
                ads_has_data,
            ),
            (
                "ads_ctr_30d.png",
                "광고 클릭률 추이 (최근 30일)",
                [
                    safe_div(ads_daily[d]["clicks"], ads_daily[d]["impressions"]) * 100
                    for d in last_30_dates
                ],
                "클릭률",
                "percent",
                ads_has_data,
            ),
            (
                "ads_cpc_30d.png",
                "클릭당 비용 추이 (최근 30일)",
                [
                    safe_div(ads_daily[d]["cost"], ads_daily[d]["clicks"])
                    for d in last_30_dates
                ],
                "클릭당 비용",
                "currency",
                ads_has_data,
            ),
        ]

        for filename, title, values, y_label, value_type, has_data in chart_specs:
            charts.append({
                "filename": filename,
                "title": title,
                "values": values,
                "y_label": y_label,
                "value_type": value_type,
                "has_data": has_data,
                "dates": last_30_dates,
            })
        return charts

    def generate_charts(self, output_dir: Path, charts: list[dict]):
        output_dir.mkdir(parents=True, exist_ok=True)
        for chart in charts:
            if not chart.get("has_data"):
                continue
            path = output_dir / chart["filename"]
            self._plot_chart(
                path,
                chart["dates"],
                chart["values"],
                chart["title"],
                chart["value_type"],
                chart["has_data"],
            )

    def _plot_chart(
        self,
        path: Path,
        dates: list[str],
        values: list[float],
        title: str,
        value_type: str,
        has_data: bool,
    ):
        fig, ax = plt.subplots(figsize=(6.8, 3.2), dpi=150)
        ax.plot(dates, values, color=ACCENT_COLOR, linewidth=2)
        ax.fill_between(dates, values, color=ACCENT_COLOR, alpha=0.12)
        ax.set_title(title, fontsize=11, loc="left", pad=10)
        ax.grid(axis="y", color="#e5e7eb", linewidth=0.8)
        ax.tick_params(axis="x", labelrotation=45, labelsize=8)
        ax.tick_params(axis="y", labelsize=8)

        tick_step = max(1, len(dates) // 6)
        ax.set_xticks(dates[::tick_step])

        if value_type == "currency":
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"₩{int(x):,}"))
        elif value_type == "percent":
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.1f}%"))
        else:
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

        fig.tight_layout()
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _plot_bar_chart(self, path: Path, labels: list[str], values: list[float], title: str, horizontal: bool = False):
        fig, ax = plt.subplots(figsize=(7.2, 3.8), dpi=150)
        if horizontal:
            ax.barh(labels, values)
            ax.invert_yaxis()
        else:
            ax.bar(labels, values)
        ax.set_title(title, fontsize=13, loc="left", pad=8)
        ax.tick_params(axis="x", labelrotation=20, labelsize=9)
        ax.tick_params(axis="y", labelsize=9)
        fig.tight_layout()
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _plot_dual_bar_chart(
        self,
        path: Path,
        labels: list[str],
        values_left: list[float],
        values_right: list[float],
        title: str,
        left_label: str,
        right_label: str,
    ):
        fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.8), dpi=150)
        axes[0].barh(labels, values_left)
        axes[0].invert_yaxis()
        axes[0].set_title(left_label, fontsize=12, loc="left")
        axes[0].tick_params(axis="y", labelsize=8)
        axes[0].tick_params(axis="x", labelsize=8)
        axes[1].barh(labels, values_right)
        axes[1].invert_yaxis()
        axes[1].set_title(right_label, fontsize=12, loc="left")
        axes[1].tick_params(axis="y", labelsize=8)
        axes[1].tick_params(axis="x", labelsize=8)
        fig.suptitle(title, fontsize=13)
        fig.tight_layout()
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _plot_heatmap(self, path: Path, matrix: list[list[float]], labels: list[str], title: str):
        fig, ax = plt.subplots(figsize=(9, 3.8), dpi=150)
        im = ax.imshow(matrix, aspect="auto")
        ax.set_title(title, fontsize=13, loc="left", pad=8)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=9)
        ax.set_xticks(range(0, 24, 3))
        ax.set_xticklabels([str(h) for h in range(0, 24, 3)], fontsize=9)
        fig.colorbar(im, ax=ax, fraction=0.02, pad=0.02)
        fig.tight_layout()
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def generate_extra_charts(self, output_dir: Path, specs: dict) -> dict:
        output_dir.mkdir(parents=True, exist_ok=True)
        chart_paths: dict[str, str] = {}
        for key, spec in specs.items():
            if not spec.get("has_data"):
                continue
            filename = {
                "week_scoreboard": "week_scoreboard.png",
                "waste_keywords_top10": "waste_keywords_top10.png",
                "waste_queries_top10": "waste_queries_top10.png",
                "growth_keywords_cpa_top10": "growth_keywords_cpa_top10.png",
                "growth_landing_cpa_top10": "growth_landing_cpa_top10.png",
                "device_cpa_compare": "device_cpa_compare.png",
                "countries_top10": "countries_top10.png",
                "weekday_conversions": "weekday_conversions.png",
                "hour_weekday_heatmap": "hour_weekday_heatmap.png",
            }.get(key)
            if not filename:
                continue
            path = output_dir / filename
            if spec["type"] == "bar":
                self._plot_bar_chart(path, spec["labels"], spec["values"], spec["title"], horizontal=False)
            elif spec["type"] == "barh":
                self._plot_bar_chart(path, spec["labels"], spec["values"], spec["title"], horizontal=True)
            elif spec["type"] == "dual_bar":
                self._plot_dual_bar_chart(
                    path,
                    spec["labels"],
                    spec["values_left"],
                    spec["values_right"],
                    spec["title"],
                    "전환",
                    "전환당 비용",
                )
            elif spec["type"] == "device_compare":
                self._plot_dual_bar_chart(
                    path,
                    spec["labels"],
                    spec["values_left"],
                    spec["values_right"],
                    spec["title"],
                    "전환",
                    "전환당 비용",
                )
            elif spec["type"] == "heatmap":
                self._plot_heatmap(path, spec["matrix"], spec["labels"], spec["title"])
            chart_paths[key] = filename
        return chart_paths

    def render_report(self, output_path: Path, context: dict):
        env = Environment(
            loader=FileSystemLoader("templates"),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("report_ko.html")
        html = template.render(**context)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding="utf-8")
        print(f"리포트 저장됨: {output_path}")


def build_render_context(
    report_data: dict,
    chart_prefix: str,
    logo_png_path: str,
    logo_svg_path: str,
    logo_url: str | None,
    start_date: str,
    end_date: str,
) -> dict:
    build_env = os.getenv("BUILD_ENV", "local")
    git_sha = os.getenv("GIT_SHA", "")[:7]
    charts = []
    for chart in report_data["charts"]:
        if not chart.get("has_data"):
            continue
        charts.append({
            "title": chart["title"],
            "path": f"{chart_prefix}{chart['filename']}",
        })
    extra_charts = {
        key: f"{chart_prefix}{filename}" for key, filename in report_data.get("extra_charts", {}).items()
    }
    return {
        "report_title": REPORT_TITLE,
        "period_start": start_date,
        "period_end": end_date,
        "generated_at": datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M"),
        "summary": report_data["summary"],
        "tables": report_data["tables"],
        "charts": charts,
        "logo_png_path": logo_png_path,
        "logo_svg_path": logo_svg_path,
        "logo_url": logo_url,
        "ai_summary": report_data["ai_summary"],
        "geo_map": report_data["geo_map"],
        "keyword_tables": report_data["keyword_tables"],
        "search_terms": report_data["search_terms"],
        "wasted_summary": report_data["wasted_summary"],
        "conversion_definitions": report_data["conversion_definitions"],
        "exec_summary": report_data["exec_summary"],
        "today_line": report_data["today_line"],
        "action_cards": report_data["action_cards"],
        "extra_charts": extra_charts,
        "build_metadata": {
            "env": build_env,
            "git_sha": git_sha,
            "generated_at": datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M"),
        },
    }


def main():
    load_dotenv()
    property_id = os.getenv("PROPERTY_ID")
    customer_id = os.getenv("CUSTOMER_ID")
    logo_url = os.getenv("LOGO_URL", "").strip() or None
    if not property_id or not customer_id:
        print("오류: PROPERTY_ID와 CUSTOMER_ID가 필요합니다.")
        return

    start_date = FIXED_START_DATE
    end_date = seoul_today().isoformat()

    print(f"리포트 생성: {start_date} ~ {end_date}")
    generator = ReportGenerator(property_id, customer_id, start_date, end_date)
    report_data = generator.collect_all_data()

    report_dir = Path(f"reports/{end_date}")
    generator.generate_charts(report_dir, report_data["charts"])
    report_data["extra_charts"] = generator.generate_extra_charts(report_dir, report_data.get("extra_chart_specs", {}))

    report_context = build_render_context(
        report_data,
        chart_prefix="",
        logo_png_path="../../assets/huelight-logo.png",
        logo_svg_path="../../assets/huelight-logo.svg",
        logo_url=logo_url,
        start_date=start_date,
        end_date=end_date,
    )
    generator.render_report(report_dir / "index.html", report_context)

    root_context = build_render_context(
        report_data,
        chart_prefix=f"reports/{end_date}/",
        logo_png_path="assets/huelight-logo.png",
        logo_svg_path="assets/huelight-logo.svg",
        logo_url=logo_url,
        start_date=start_date,
        end_date=end_date,
    )
    generator.render_report(Path("index.html"), root_context)
    print("\n✅ 완료: index.html 및 reports 히스토리 업데이트")


if __name__ == "__main__":
    main()
