import os
import json
import uuid
import datetime
from datetime import timezone
import requests
from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

# --- Configuration for MySQL (Railway) ---
DB_USER = os.getenv('MYSQL_USER', 'root') 
DB_PASS = os.getenv('MYSQL_PASSWORD', 'RoVdEbtOMpxeuMBKLnqcVPVTsNofXOtu')
DB_HOST = os.getenv('MYSQL_HOST', 'yamabiko.proxy.rlwy.net') 
DB_PORT = os.getenv('MYSQL_PORT', '50624') 
DB_NAME = os.getenv('MYSQL_DATABASE', 'railway')

# --- Configuration for SAP OData Callback (PHASE 4) ---
# This OData URL is used to push the CAMT.054 confirmation back to SAP.
SAP_ODATA_URL = os.getenv('SAP_ODATA_URL', "https://s4h2023.sapdemo.com:44323/sap/opu/odata/sap/Z_BANK_STATEMENT_SRV/BankStatementSet") 
SAP_USER = os.getenv('SAP_USER', '702374') 
SAP_PASSWORD = os.getenv('SAP_PASSWORD', 'Welcome123')

# --- Database Connection Utilities ---

def get_db_connection():
    """Establishes and returns a MySQL database connection."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Error as e:
        print(f"MySQL Database connection failed: {e}")
        return None

def init_db():
    """Simple check to ensure database connection is working."""
    conn = get_db_connection()
    if conn: 
        print("Database connection successfully established and checked.")
        conn.close()
    else:
        print("Initial database connection failed.")

init_db()

# --- PHASE 3: CAMT.054 XML Generation (Confirmation back to SAP) ---

def generate_camt_054_xml(payment_details):
    """Generates a CAMT.054 XML confirmation for the debit transaction."""
    current_time = datetime.datetime.now(timezone.utc).isoformat().split('+')[0] + 'Z'
    today_date = datetime.date.today().isoformat()
    
    amount = payment_details['paymentAmount']
    customer_acc = payment_details['customerAccount']
    currency = payment_details['currency']
    end_to_end_id = payment_details['endToEndId']
    
    message_id = f"CAMT-{uuid.uuid4()}" 
    statement_id = f"STMT-{today_date}-{customer_acc}"

    # CdtDbtInd=DBIT is required to signal a debit (money leaving the SAP House Bank).
    camt_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.02">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr>
      <MsgId>{message_id}</MsgId>
      <CreDtTm>{current_time}</CreDtTm>
    </GrpHdr>
    <Ntfctn>
      <Id>{statement_id}</Id>
      <ElctrncSeqNb>1</ElctrncSeqNb>
      <Acct>
        <Id>
          <Othr>
            <Id>{customer_acc}</Id>
          </Othr>
        </Id>
      </Acct>
      <Ntry>
        <Amt Ccy="{currency}">{amount:.2f}</Amt>
        <CdtDbtInd>DBIT</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><Dt>{today_date}</Dt></BookgDt>
        <ValDt><Dt>{today_date}</Dt></ValDt>
        <BkTxCd><Prtry><Id>NTRF</Id></Prtry></BkTxCd>
        <NtryDtls>
          <TxDtls>
            <Refs>
              <EndToEndId>{end_to_end_id}</EndToEndId>
            </Refs>
            <AmtDtls>
              <InstdAmt Ccy="{currency}">{amount:.2f}</InstdAmt>
            </AmtDtls>
            <RltdPties>
              <DbtrAcct>
                <Id><Othr><Id>{customer_acc}</Id></Othr></Id>
              </DbtrAcct>
            </RltdPties>
            <RmtInf>
                <Strd>
                    <CdtrRefInf>
                        <Ref>{end_to_end_id}</Ref>
                    </CdtrRefInf>
                </Strd>
            </RmtInf>
          </TxDtls>
        </NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""
    return camt_xml

# --- PHASE 4: SQL Bank Pushes Statement to SAP OData ---

def push_camt_to_sap_odata(camt_xml):
    """
    Sends the generated CAMT.054 XML back to SAP via the OData service.
    """
    print(f"Attempting to push CAMT statement to SAP OData URL: {SAP_ODATA_URL}")
    
    headers = {
        # Note: The SAP OData service expects a JSON structure with the XML string inside, 
        # but since your service uses a custom entity with 'XmlData' field, 
        # we will assume the structure needed for the Bank Statement OData service:
        'Content-Type': 'application/json',
        'Accept': 'application/json' 
    }
    
    # Wrap the XML into the JSON payload that matches the OData entity structure
    odata_payload = {
        "StatementId": "", # Left blank, filled by SAP
        "XmlData": camt_xml
    }
    
    try:
        response = requests.post(
            SAP_ODATA_URL, 
            data=json.dumps(odata_payload), # Send JSON payload
            headers=headers,
            auth=(SAP_USER, SAP_PASSWORD),
            verify=False 
        )
        
        response.raise_for_status() 
        print(f"SAP OData Push SUCCESS. Status: {response.status_code}")
        # print(f"SAP Response Body: {response.text}") # Uncomment for debugging
        return True, response.status_code
        
    except requests.exceptions.RequestException as err:
        print(f"SAP OData Push FAILED: {err}")
        # print(f"SAP Response Body: {getattr(err.response, 'text', 'No response body')}") # Uncomment for debugging
        http_code = getattr(err.response, 'status_code', 500) if err.response else 500
        return False, http_code

# --- NEW ENDPOINT FOR CPI PAYMENT FILE UPLOAD (SAP -> CPI -> Bank) ---

@app.post("/bank/process_payment_file")
def process_payment_file():
    """
    Receives the pain.001 batch file XML from SAP CPI.
    This simulates the acceptance and processing of the batch file.
    
    The request body is expected to be raw XML with Content-Type: application/xml.
    """
    
    # Check if the content type is XML
    if request.content_type != 'application/xml' and request.content_type != 'text/xml':
        print(f"Received incorrect Content-Type: {request.content_type}. Expected application/xml.")
        return jsonify({"status": "ERROR", "message": "Content-Type must be application/xml"}), 415

    # Get the raw XML data
    pain_xml = request.data.decode('utf-8')
    
    if not pain_xml:
        return jsonify({"status": "FAILED", "message": "Received empty payment file"}), 400

    # --- BEGIN SIMULATION OF BATCH PROCESSING ---
    
    # In a real system, you would parse the pain.001 XML here to extract
    # individual transactions, debit the bank's master account, and notify
    # the target bank/network (e.g., SWIFT, SEPA).
    
    # Since we cannot parse complex XML here, we simply acknowledge and log.
    
    print(f"--- Successfully Received PAIN.001 Batch File from CPI ---")
    print(f"File Size: {len(pain_xml.encode('utf-8'))} bytes")
    # print(f"File Content (Truncated): {pain_xml[:500]}...") # Optional: log file start
    
    # --- END SIMULATION ---
    
    # If processing succeeded, the bank returns a success acknowledgement (HTTP 202 or 200).
    return jsonify({
        "status": "ACCEPTED",
        "message": "Payment file accepted for batch processing.",
        "file_receipt_id": str(uuid.uuid4())
    }), 202

# --- CORE TRANSACTION ENDPOINT (PHASE 2 - Kept for Confirmation Push Logic) ---

@app.post("/bank/receive_payment")
def receive_payment():
    """
    PHASE 2.1: Receives JSON payload from SAP.
    Executes atomic database transaction (Debit/Credit).
    Generates CAMT.054 (PHASE 3) and pushes to SAP (PHASE 4).
    
    NOTE: This endpoint is likely superseded by the pain.001 file upload flow, 
    but we keep it to reuse the core transaction and confirmation push logic.
    """
    data = request.json
    
    required_fields = ["customerAccount", "vendorAccount", "paymentAmount", "currency", "paymentId", "endToEndId", "xmlContent"]
    if not all(field in data for field in required_fields):
        return jsonify({"status": "ERROR", "statusCode": 99, "message": "Missing required payment fields"}), 400

    customer_acc = data.get("customerAccount")
    vendor_acc = data.get("vendorAccount")
    payment_amount = float(data.get("paymentAmount"))
    currency = data.get("currency")
    payment_id = data.get("paymentId")
    end_to_end_id = data.get("endToEndId")
    xml_content = data.get("xmlContent")
    received_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn = get_db_connection()
    if not conn:
        return jsonify({"status": "ERROR", "statusCode": 99, "message": "Bank system offline"}), 503
    
    conn.autocommit = False # Start transaction
    cur = None
    status_code = 99
        
    try:
        cur = conn.cursor(dictionary=True) 
        
        # 1. Log initial request status: RECEIVED
        cur.execute(
            "INSERT INTO PAYMENT_REQUEST (REQUEST_ID, END_TO_END_ID, CUSTOMER_ACC, VENDOR_ACC, AMOUNT, CURRENCY, STATUS, RECEIVED_AT, XML_DATA) VALUES (%s, %s, %s, %s, %s, %s, 'RECEIVED', %s, %s)",
            (payment_id, end_to_end_id, customer_acc, vendor_acc, payment_amount, currency, received_at, xml_content)
        )

        # 2. Check Customer Balance (PHASE 2.2 - Lock Payer Account)
        cur.execute("SELECT BALANCE FROM CUSTOMER_ACCOUNT WHERE ACC_NO = %s FOR UPDATE", (customer_acc,))
        customer_record = cur.fetchone()

        if customer_record is None:
            status_code = 3 
            raise Exception(f"Payer account {customer_acc} not found")

        customer_balance = float(customer_record["BALANCE"]) 

        if customer_balance < payment_amount:
            status_code = 1 
            raise Exception("Insufficient balance")

        # 3. Check Vendor Account Existence
        cur.execute("SELECT ACC_NO FROM VENDOR_ACCOUNT WHERE ACC_NO = %s", (vendor_acc,))
        if cur.fetchone() is None:
            status_code = 4 
            raise Exception(f"Payee account {vendor_acc} not found")

        # 4. Core Transaction: Deduct and Credit (PHASE 2.3)
        cur.execute("UPDATE CUSTOMER_ACCOUNT SET BALANCE = BALANCE - %s WHERE ACC_NO = %s", (payment_amount, customer_acc))
        cur.execute("UPDATE VENDOR_ACCOUNT SET BALANCE = BALANCE + %s WHERE ACC_NO = %s", (payment_amount, vendor_acc))
        
        # 5. Update Request Status: PAID (PHASE 2.4)
        updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("UPDATE PAYMENT_REQUEST SET STATUS = 'PAID', UPDATED_AT = %s WHERE REQUEST_ID = %s", (updated_at, payment_id))

        conn.commit()
        status_code = 0 # Success

        # 6. PUSH CAMT.054 TO SAP (PHASE 3 & 4)
        # Note: We now assume the SAP OData service expects JSON containing the XML string.
        payment_data = {
            "paymentAmount": payment_amount, 
            "customerAccount": customer_acc, 
            "currency": currency, 
            "endToEndId": end_to_end_id
        }
        camt_xml = generate_camt_054_xml(payment_data)
        push_status, http_code = push_camt_to_sap_odata(camt_xml)

        return jsonify({
            "status": "SUCCESS",
            "statusCode": status_code,
            "paymentId": payment_id,
            "message": "Payment processed successfully. CAMT statement push triggered.",
            "sap_odata_status": "SENT" if push_status else f"PUSH_FAILED (HTTP {http_code})",
            "amount": payment_amount
        }), 200

    except Exception as e:
        print(f"Transaction Logic Error: {e}")
        conn.rollback()
        
        # Log FAILED status
        if payment_id and cur:
            try:
                updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cur.execute("UPDATE PAYMENT_REQUEST SET STATUS = 'FAILED', UPDATED_AT = %s WHERE REQUEST_ID = %s", (updated_at, payment_id))
                conn.commit()
            except Error as rollback_err:
                print(f"Error updating FAILED status: {rollback_err}")
                
        error_message = str(e)
        http_status = 500
        if status_code == 1: http_status = 200 # Insufficient funds is a functional success of the API
        elif status_code in [3, 4]: http_status = 404
        
        return jsonify({
            "status": "FAILED", 
            "statusCode": status_code, 
            "paymentId": payment_id, 
            "message": error_message
        }), http_status
    finally:
        if cur: cur.close()
        if conn: conn.close()


# --- MANAGEMENT/CRUD ENDPOINTS (For Testing/Monitoring) ---

@app.get("/health")
def health_check():
    """PHASE 7 - Simple health check endpoint."""
    conn = get_db_connection()
    if conn:
        conn.close()
        db_status = "Online"
        db_code = 0
    else:
        db_status = "Offline"
        db_code = 1

    return jsonify({
        "status": "OK",
        "service": "Bank API",
        "db_status": db_status,
        "db_code": db_code
    }), 200

@app.get("/accounts/<string:acc_type>/<string:acc_no>")
def get_account_details(acc_type, acc_no):
    """Retrieves account details for CUSTOMER or VENDOR accounts."""
    if acc_type not in ['customer', 'vendor']:
        return jsonify({"message": "Invalid account type"}), 400
    
    table_name = "CUSTOMER_ACCOUNT" if acc_type == 'customer' else "VENDOR_ACCOUNT"
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB connection failed"}), 503
    
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {table_name} WHERE ACC_NO = %s", (acc_no,))
        account = cur.fetchone()
        
        if account:
            return jsonify(account), 200
        else:
            return jsonify({"message": f"{acc_type.capitalize()} account not found"}), 404
    except Error as e:
        return jsonify({"message": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.get("/transactions/<string:request_id>")
def get_transaction_details(request_id):
    """Retrieves a single transaction log from PAYMENT_REQUEST."""
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB connection failed"}), 503
    
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT REQUEST_ID, END_TO_END_ID, CUSTOMER_ACC, VENDOR_ACC, AMOUNT, CURRENCY, STATUS, RECEIVED_AT, UPDATED_AT FROM PAYMENT_REQUEST WHERE REQUEST_ID = %s", (request_id,))
        transaction = cur.fetchone()
        
        if transaction:
            return jsonify(transaction), 200
        else:
            return jsonify({"message": "Transaction request not found"}), 404
    except Error as e:
        return jsonify({"message": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()


if __name__ == "__main__":
    # Use environment variables for port configuration in Railway
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000), debug=True)