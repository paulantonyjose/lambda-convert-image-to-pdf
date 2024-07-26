import json
import boto3
import img2pdf
import io
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

s3 = boto3.client('s3')

def create_watermark(text):
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)
    can.setFont("Helvetica", 50)
    can.setFillColorRGB(0.5, 0.5, 0.5)  # Gray color
    can.saveState()
    can.translate(300, 400)
    can.rotate(45)
    can.drawCentredString(0, 0, text)
    can.restoreState()
    can.save()
    packet.seek(0)
    return PdfReader(packet)

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the image from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    image_data = response['Body'].read()
    
    # Convert image to PDF
    pdf_bytes = img2pdf.convert(image_data)
    
    # Create a PDF reader object
    pdf_reader = PdfReader(io.BytesIO(pdf_bytes))
    
    # Create a PDF writer object
    pdf_writer = PdfWriter()
    
    # Create watermark
    watermark = create_watermark("Confidential")
    
    # Add watermark to the PDF
    page = pdf_reader.pages[0]
    page.merge_page(watermark.pages[0])
    pdf_writer.add_page(page)
    
    # Save the watermarked PDF to a bytes buffer
    output_pdf = io.BytesIO()
    pdf_writer.write(output_pdf)
    output_pdf.seek(0)
    
    # Upload the watermarked PDF back to S3
    output_key = key.rsplit('.', 1)[0] + '_watermarked.pdf'
    s3.put_object(Bucket=bucket, Key=output_key, Body=output_pdf.getvalue())
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Watermarked PDF created and saved as {output_key}')
    }
