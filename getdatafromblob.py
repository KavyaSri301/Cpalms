import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
load_dotenv()

def clean_html(html_text):
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text(separator="\n", strip=True)

def get_blob_data(benchmark: str, resource_id: str):
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connect_str:
        raise ValueError("❌ AZURE_STORAGE_CONNECTION_STRING not found in environment.")

    container_name = "cpalmsnewdata"
    blob_path = f"lessonplans/{benchmark}/{resource_id}.json"

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    try:
        blob_data = blob_client.download_blob().readall()
        json_data = json.loads(blob_data)
        return json_data
    except Exception as e:
        print(f"❌ Failed to retrieve blob: {e}")
        return None

def format_lesson_output(data: dict,attachments_hyperlinks: list) -> str:
    def section(label, value):
        return f"**{label}:** {clean_html(str(value))}" if value else ""

    output = []

   
    resource_id = data.get("ResourceId")
    if resource_id:
        resource_link = f'<a href="https://www.cpalms.org/Public/PreviewResourceLesson/Preview/{resource_id}" target="_blank">{resource_id}</a>'
        output.append(f"**ResourceId:** {resource_link}")
    output.append(section("Title", data.get("Title")))
    output.append(section("Grade Level", data.get("GradeLevelNames")))
    output.append(section("Subject Areas", data.get("SubjectAreaNames")))
    output.append(section("Audience", data.get("IntendedAudienceNames")))
    output.append(section("Benchmarks", data.get("BenchmarkCodes")))
    output.append(section("Description", data.get("Description")))
    if attachments_hyperlinks.strip():
        output.append("**Attachments:**")
        output.append(attachments_hyperlinks)
    questions = data.get("LessonPlanQuestions", [])
    for q in questions:
        title = clean_html(q.get("Title", ""))
        answer = clean_html(q.get("ResLessPlanQuestionAnswer", ""))
        if title and answer:
            output.append(f"### {title}")
            output.append(answer)

    return "\n\n".join(output)        

def fetch_and_get_lesson(benchmark: str, resource_id: str):
    blob_data = get_blob_data(benchmark, resource_id)
    if blob_data is None:
        return f"⚠️ No lesson plan found for Resource ID '{resource_id}' under Benchmark '{benchmark}'. Please check if the ID is correct and try again."
    return blob_data