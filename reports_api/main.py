from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from jose import jwt, JWTError
from clickhouse_driver import Client as ClickHouseClient
import os
import io
import csv

app = FastAPI(title="Reports API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

security = HTTPBearer()

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_USER = os.getenv("CLICKHOUSE_USER", "ch_user")
CH_PASS = os.getenv("CLICKHOUSE_PASS", "ch_password")
CH_DB   = os.getenv("CLICKHOUSE_DB", "reports_olap")

def decode_token(token: str):
    try:
        data = jwt.decode(
            token,
            key="",
            options={"verify_signature": False, "verify_aud": False},
        )
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    email = data.get("email")
    if not email:
        raise HTTPException(status_code=403, detail="No email in token")

    return email


def get_current_user_email(creds: HTTPAuthorizationCredentials = Depends(security)):
    token = creds.credentials
    user_email = decode_token(token)
    return user_email


@app.get("/reports")
def get_report(current_user_email: str = Depends(get_current_user_email)):
    ch = ClickHouseClient(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database=CH_DB,
    )

    rows = ch.execute("""
        SELECT
            serial_number,
            event_time,
            reaction_latency,
            movement_accuracy,
            battery_level_percent,
            battery_temp
        FROM user_telemetry_history
        WHERE user_email = %(email)s
        ORDER BY event_time DESC
        """,
        {"email": current_user_email}
    )

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "serial_number",
        "event_time",
        "reaction_latency",
        "movement_accuracy",
        "battery_level_percent",
        "battery_temp",
    ])

    for (
        serial_number,
        event_time,
        reaction_latency,
        movement_accuracy,
        battery_level_percent,
        battery_temp
    ) in rows:
        writer.writerow([
            serial_number,
            event_time.isoformat(),
            f"{reaction_latency:.1f}",
            f"{movement_accuracy:.1f}",
            f"{battery_level_percent:.1f}",
            f"{battery_temp:.1f}",
        ])

    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))

    return StreamingResponse(
        csv_bytes,
        media_type="text/csv",
        headers={
             "Content-Disposition": 'attachment; filename="report.csv"',
        },
    )
