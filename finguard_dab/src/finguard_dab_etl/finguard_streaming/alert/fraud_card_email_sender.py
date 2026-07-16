from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============================================================================
# CONFIGURATION
# ============================================================================
EMAIL_FROM = "nikhithareddye08@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SECRETS_SCOPE = "finguard-scope"
SECRETS_KEY = "gmail_api_key"
GMAIL_API_KEY = dbutils.secrets.get(scope=SECRETS_SCOPE, key=SECRETS_KEY)

# ============================================================================
# EMAIL TEMPLATE FUNCTIONS
# ============================================================================
def format_fraud_alert_email(alert_row):
    """
    Generate email subject and HTML body from fraud alert data.
    
    Args:
        alert_row: Row object containing fraud alert details
        
    Returns:
        tuple: (subject: str, body: str)
    """
    subject = f"🚨 URGENT: Fraud Alert - Card Ending {alert_row.card_number[-4:]}"
    
    body = f"""
    <html>
    <body>
        <h2 style="color: red;">⚠️ Fraud Card Alert</h2>
        <p>Dear {alert_row.customer_name},</p>
        <p><strong>We have detected a transaction using a card flagged for fraud.</strong></p>
        
        <h3>Alert Details:</h3>
        <ul>
            <li><strong>Alert ID:</strong> {alert_row.alert_id}</li>
            <li><strong>Alert Type:</strong> {alert_row.alert_type}</li>
            <li><strong>Risk Level:</strong> <span style="color: red;">{alert_row.risk_level}</span></li>
            <li><strong>Reason:</strong> {alert_row.reason_description}</li>
        </ul>
        
        <h3>Transaction Details:</h3>
        <ul>
            <li><strong>Card Number:</strong> ****-****-****-{alert_row.card_number[-4:]}</li>
            <li><strong>Amount:</strong> {alert_row.currency} {alert_row.amount:,.2f}</li>
            <li><strong>Merchant:</strong> {alert_row.merchant_name}</li>
            <li><strong>Category:</strong> {alert_row.merchant_category}</li>
            <li><strong>Location:</strong> {alert_row.transaction_city}, {alert_row.transaction_country}</li>
            <li><strong>Timestamp:</strong> {alert_row.transaction_timestamp}</li>
        </ul>
        
        <h3>Recommended Action:</h3>
        <p style="background-color: #ffcccc; padding: 10px; border-left: 4px solid red;">
            <strong>Immediate action required:</strong> Please contact our fraud prevention team immediately at 1-800-FINGUARD 
            or reply to this email. Your card may be blocked for your security.
        </p>
        
        <p><strong>If this transaction was authorized by you, please confirm with us as soon as possible.</strong></p>
        
        <br>
        <p>Best regards,<br><b>FinGuard Fraud Prevention Team</b></p>
        <p style="font-size: 10px; color: gray;">Alert ID: {alert_row.alert_id} | Watchlist ID: {alert_row.watchlist_id}</p>
    </body>
    </html>
    """
    
    return subject, body


# ============================================================================
# EMAIL SENDING FUNCTIONS
# ============================================================================
def send_email_via_smtp(to_email, subject, body_html, gmail_api_key):
    """
    Send email via Gmail SMTP server.
    
    Args:
        to_email: Recipient email address
        subject: Email subject line
        body_html: HTML email body content
        gmail_api_key: Gmail app password
        
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html"))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, gmail_api_key)
            server.send_message(msg)
        
        return (True, None)
    except Exception as e:
        return (False, str(e))


def send_single_fraud_alert_email(alert_row, gmail_api_key):
    """
    Send email alert for a single fraud card transaction.
    
    Args:
        alert_row: Row object containing fraud alert details
        gmail_api_key: Gmail app password for authentication
        
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    subject, body = format_fraud_alert_email(alert_row)
    return send_email_via_smtp(alert_row.customer_email, subject, body, gmail_api_key)


# ============================================================================
# PIPELINE COMPONENTS
# ============================================================================
@dp.foreach_batch_sink(name="fraud_card_email_sender")
def send_fraud_email_alerts(df, batch_id):
    """
    ForEachBatch sink handler that sends email alerts for fraud card transactions.
    
    Args:
        df: DataFrame containing fraud card alerts for this micro-batch
        batch_id: Integer ID of the micro-batch
    """
    # Retrieve Gmail API key from secrets
    try:
        gmail_api_key = GMAIL_API_KEY
    except Exception as e:
        print(f"⚠️ Batch {batch_id}: Failed to retrieve API key: {e}")
        return
    
    # Collect alerts
    alerts = df.collect()
    if not alerts:
        print(f"ℹ️ Batch {batch_id}: No fraud alerts to send.")
        return
    
    print(f"🚨 Batch {batch_id}: Processing {len(alerts)} fraud alert(s)...")
    
    # Send emails
    success_count = 0
    error_count = 0
    
    for alert in alerts:
        success, error_msg = send_single_fraud_alert_email(alert, gmail_api_key)
        
        if success:
            success_count += 1
            print(f"  ✅ Sent to {alert.customer_email} (Alert: {alert.alert_id})")
        else:
            error_count += 1
            print(f"  ❌ Failed Alert {alert.alert_id}: {error_msg}")
    
    print(f"📊 Batch {batch_id}: {success_count} sent, {error_count} failed")


@dp.append_flow(
    target="fraud_card_email_sender",
    name="fraud_card_email_flow",
    comment="Streams fraud card alerts and sends urgent email notifications in real-time"
)
def stream_fraud_alerts_to_email():
    """
    Stream fraud card alerts to the email sender sink.
    """
    return spark.readStream.table("finguard.gold.fraud_card_alert")