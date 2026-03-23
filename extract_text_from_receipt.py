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
        
        # 3. THE PARSING: Dig through the Textract JSON response
        # Textract returns a deeply nested JSON. We loop through to find specific labels.
        for expense_doc in response.get('ExpenseDocuments', []):
            for field in expense_doc.get('SummaryFields', []):
                field_type = field.get('Type', {}).get('Text')
                field_val = field.get('ValueDetection', {}).get('Text')
                
                if field_type == 'VENDOR_NAME':
                    vendor_name = field_val
                elif field_type == 'TOTAL':
                    total_amount = field_val
                elif field_type == 'INVOICE_RECEIPT_DATE':
                    purchase_date = field_val

        # 4. THE DATABASE: Write the structured data to DynamoDB
        table.put_item(
            Item={
                'ReceiptId': key,  # This is our Primary Key
                'VendorName': vendor_name,
                'TotalAmount': total_amount,
                'PurchaseDate': purchase_date
            }
        )
        
        # This print statement will show up in CloudWatch Logs
        print(f"Success! Saved -> Vendor: {vendor_name} | Total: ${total_amount} | Date: {purchase_date}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {key}')
        }
        
    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e
