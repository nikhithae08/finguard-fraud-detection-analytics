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
def format_alert_email(alert_row):
    """
    Generate email subject and HTML body from alert data.
    
    Args:
        alert_row: Row object containing alert details
        
    Returns:
        tuple: (subject: str, body: str)
    """
    subject = f"🚨 High-Value Transaction Alert - {alert_row.alert_id}"
    
    body = f"""
    <html>
    <body>
        <h2>High-Value Transaction Alert</h2>
        <p>Dear {alert_row.customer_name},</p>
        <p>We detected a transaction that exceeds your configured transaction limit.</p>
        
        <h3>Transaction Details:</h3>
        <ul>
            <li><strong>Alert ID:</strong> {alert_row.alert_id}</li>
            <li><strong>Amount:</strong> {alert_row.currency} {alert_row.transaction_amount:,.2f}</li>
            <li><strong>Your Limit:</strong> {alert_row.currency} {alert_row.transaction_limit:,.2f}</li>
            <li><strong>Merchant:</strong> {alert_row.merchant_name}</li>
            <li><strong>Category:</strong> {alert_row.merchant_category}</li>
            <li><strong>Timestamp:</strong> {alert_row.transaction_timestamp}</li>
        </ul>
        
        <p>If you did not authorize this transaction, please contact us immediately.</p>
        
        <br>
        <p>Best regards,<br><b>FinGuard Security Team</b></p>
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


def send_single_alert_email(alert_row, gmail_api_key):
    """
    Send email alert for a single high-value transaction.
    
    Args:
        alert_row: Row object containing alert details
        gmail_api_key: Gmail app password for authentication
        
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    subject, body = format_alert_email(alert_row)
    return send_email_via_smtp(alert_row.customer_email, subject, body, gmail_api_key)


# ============================================================================
# PIPELINE COMPONENTS
# ============================================================================
@dp.foreach_batch_sink(name="email_alert_sender")
def send_email_alerts(df, batch_id):
    """
    ForEachBatch sink handler that sends email alerts for high-value transactions.
    
    Args:
        df: DataFrame containing high-value transaction alerts for this micro-batch
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
        print(f"ℹ️ Batch {batch_id}: No alerts to send.")
        return
    
    print(f"📧 Batch {batch_id}: Processing {len(alerts)} alert(s)...")
    
    # Send emails
    success_count = 0
    error_count = 0
    
    for alert in alerts:
        success, error_msg = send_single_alert_email(alert, gmail_api_key)
        
        if success:
            success_count += 1
            print(f"  ✅ Sent to {alert.customer_email} (Alert: {alert.alert_id})")
        else:
            error_count += 1
            print(f"  ❌ Failed Alert {alert.alert_id}: {error_msg}")
    
    print(f"📊 Batch {batch_id}: {success_count} sent, {error_count} failed")


@dp.append_flow(
    target="email_alert_sender",
    name="high_value_transaction_email_flow",
    comment="Streams high-value transaction alerts and sends email notifications in real-time"
)
def stream_alerts_to_email():
    """
    Stream high-value transaction alerts to the email sender sink.
    """
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")