from azure.storage.blob import BlobServiceClient, BlobType
from datetime import datetime
from dataformatting import convert_markdown_to_clean_text
import os
from dotenv import load_dotenv
import re

load_dotenv()

def remove_inline_download_links(text: str) -> str:
    return re.sub(
        r'📄.*?\(data:application\/vnd\.openxmlformats-officedocument\.wordprocessingml\.document;base64,[^)]+\)',
        '', 
        text
    )

def log_query_to_blob(container_name, resource_id, benchmark_code, benchmark_id, query,processing_time, lesson_plan, ai_output):
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("❌ AZURE_STORAGE_CONNECTION_STRING not found.")

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")
    blob_name = f"lesson_logs_{date}.txt"  # 👈 daily file
    ai_output=remove_inline_download_links(ai_output)
    formatted_lesson = convert_markdown_to_clean_text(lesson_plan)
    formatted_ai = convert_markdown_to_clean_text(ai_output)

    log_entry = f"""Time: {timestamp}
Resource ID: {resource_id}
Benchmark Code: {benchmark_code}
Benchmark ID: {benchmark_id}
Query: {query}
Processing Time: {processing_time:.2f} seconds
📘 Lesson Plan:
{formatted_lesson}

✨ AI Customization:
{formatted_ai}

------------------------------------------------------------------------------------------------------------
"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if not blob_client.exists():
            blob_client.create_append_blob()

        blob_client.append_block(log_entry.encode('utf-8'))

        print("✅ Log entry appended successfully.")

    except Exception as e:
        print(f"❌ Error appending to blob: {e}")
