import json
import urllib.parse
import boto3

# Initialize the AWS SDK clients outside the handler for better performance
textract = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')

# Connect to the exact table we configured earlier
table = dynamodb.Table('ScannedReceipts')

def lambda_handler(event, context):
    # 1. THE TRIGGER: Get the bucket and file name from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print(f"File {key} detected in bucket {bucket}. Starting extraction...")
    
    try:
        # 2. THE AI ENGINE: Send the image to Textract's Expense API
        response = textract.analyze_expense(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
        
        # Default variables to hold our extracted data
        vendor_name = "UNKNOWN"
        total_amount = "0.00"
        purchase_date = "UNKNOWN"
        line_items = []
        
        # 3. THE PARSING: Dig through the Textract JSON response
        for expense_doc in response.get('ExpenseDocuments', []):
            
            # --- Summary fields (vendor, total, date) ---
            for field in expense_doc.get('SummaryFields', []):
                field_type = field.get('Type', {}).get('Text')
                field_val = field.get('ValueDetection', {}).get('Text')
                
                if field_type == 'VENDOR_NAME':
                    vendor_name = field_val
                elif field_type == 'TOTAL':
                    total_amount = field_val
                elif field_type == 'INVOICE_RECEIPT_DATE':
                    purchase_date = field_val

            # --- Line items (individual products/services on the receipt) ---
            # Textract groups each row under LineItemGroups > LineItems > LineItemExpenseFields
            for group in expense_doc.get('LineItemGroups', []):
                for line_item in group.get('LineItems', []):
                    item = {
                        'Description': 'UNKNOWN',
                        'Quantity':    'UNKNOWN',
                        'UnitPrice':   'UNKNOWN',
                        'Price':       'UNKNOWN',
                    }
                    
                    for field in line_item.get('LineItemExpenseFields', []):
                        field_type = field.get('Type', {}).get('Text')
                        field_val  = field.get('ValueDetection', {}).get('Text', 'UNKNOWN')
                        
                        if field_type == 'ITEM':
                            item['Description'] = field_val
                        elif field_type == 'QUANTITY':
                            item['Quantity'] = field_val
                        elif field_type == 'UNIT_PRICE':
                            item['UnitPrice'] = field_val
                        elif field_type == 'PRICE':
                            item['Price'] = field_val
                    
                    # Apply fallbacks after all fields are parsed
                    if item['Quantity'] == 'UNKNOWN':
                        item['Quantity'] = '1'
                    if item['UnitPrice'] == 'UNKNOWN':
                        item['UnitPrice'] = item['Price']  # Price may still be UNKNOWN, but that's fine
                    
                    line_items.append(item)

        # 4. THE DATABASE: Write the structured data to DynamoDB
        # Note: DynamoDB cannot store floats natively — keep monetary values as strings.
        table.put_item(
            Item={
                'ReceiptId':    key,           # Primary Key
                'VendorName':   vendor_name,
                'TotalAmount':  total_amount,
                'PurchaseDate': purchase_date,
                'LineItems':    line_items,    # List of dicts, stored as a DynamoDB List
            }
        )
        
        print(f"Success! Saved -> Vendor: {vendor_name} | Total: ${total_amount} | Date: {purchase_date} | Items: {len(line_items)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {key} with {len(line_items)} line item(s)')
        }
        
    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e