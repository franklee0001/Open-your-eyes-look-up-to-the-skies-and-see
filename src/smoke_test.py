import os
from datetime import date, timedelta

from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.ads.googleads.client import GoogleAdsClient


GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CREDENTIALS_PATH = ".secrets/ga4.json"
ADS_CREDENTIALS_PATH = ".secrets/google-ads.yaml"


def yesterday_iso() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def run_ga4_smoke(property_id: str):
    creds = service_account.Credentials.from_service_account_file(
        GA4_CREDENTIALS_PATH, scopes=GA4_SCOPES
    )
    client = BetaAnalyticsDataClient(credentials=creds)
    yday = yesterday_iso()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="landingPagePlusQueryString"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[Metric(name="sessions"), Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date=yday, end_date=yday)],
        limit=5,
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        dims = [v.value for v in row.dimension_values]
        mets = [v.value for v in row.metric_values]
        rows.append({
            "landingPagePlusQueryString": dims[0],
            "sessionSource": dims[1],
            "sessionMedium": dims[2],
            "sessions": mets[0],
            "activeUsers": mets[1],
        })
    return rows


def run_ads_smoke(customer_id: str):
    client = GoogleAdsClient.load_from_storage(ADS_CREDENTIALS_PATH)
    service = client.get_service("GoogleAdsService")
    query = (
        "SELECT segments.date, campaign.name, metrics.impressions, metrics.clicks, "
        "metrics.cost_micros, metrics.conversions "
        "FROM campaign "
        "WHERE segments.date DURING YESTERDAY "
        "LIMIT 5"
    )
    response = service.search(customer_id=customer_id, query=query)
    rows = []
    for row in response:
        rows.append({
            "date": row.segments.date,
            "campaign": row.campaign.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost_micros": row.metrics.cost_micros,
            "conversions": row.metrics.conversions,
        })
    return rows


def print_rows(title: str, rows):
    for row in rows[:5]:
        print(f"  {title} {row}")


def main():
    load_dotenv()
    property_id = os.getenv("PROPERTY_ID")
    customer_id = os.getenv("CUSTOMER_ID")

    if not property_id:
        print("[GA4] FAIL missing PROPERTY_ID")
    else:
        try:
            ga4_rows = run_ga4_smoke(property_id)
            print(f"[GA4] OK ({len(ga4_rows)} rows)")
            print_rows("", ga4_rows)
        except Exception as exc:
            print(f"[GA4] FAIL {exc.__class__.__name__}: {exc}")

    if not customer_id:
        print("[ADS] FAIL missing CUSTOMER_ID")
    else:
        try:
            ads_rows = run_ads_smoke(customer_id)
            print(f"[ADS] OK ({len(ads_rows)} rows)")
            print_rows("", ads_rows)
        except Exception as exc:
            import traceback
            print(f"[ADS] FAIL {type(exc).__name__}: {exc}")
            traceback.print_exc()


if __name__ == "__main__":
    main()