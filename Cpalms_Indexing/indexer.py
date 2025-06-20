import json
import re
import asyncio
import logging
import time
from typing import List, Dict, Optional
from azure.search.documents import SearchClient
from azure.search.documents.aio import SearchClient as AsyncSearchClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import AzureError, ServiceRequestError
from openai import AzureOpenAI, OpenAI
from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ResourceIndexer:
    def __init__(self, endpoint: str, key: str, index_name: str):
        self.endpoint = endpoint
        self.credential = AzureKeyCredential(key)
        self.index_name = index_name
        self.search_client = AsyncSearchClient(
            endpoint=endpoint,
            index_name=index_name,
            credential=self.credential
        )
        self.index_client = SearchIndexClient(
            endpoint=endpoint,
            credential=self.credential
        )
        
        self._setup_openai_client()
        
    def _setup_openai_client(self):
        """Initialize OpenAI client based on configuration"""
        try:
            if settings.is_azure_openai_configured():
                logger.info("Using Azure OpenAI configuration")
                self.openai_client = AzureOpenAI(
                    api_key=settings.AZURE_OPENAI_API_KEY,
                    api_version=settings.AZURE_OPENAI_API_VERSION,
                    azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
                )
                self.embedding_model = settings.AZURE_OPENAI_EMBEDDING_DEPLOYMENT
                self.is_azure_openai = True
            elif settings.OPENAI_API_KEY:
                logger.info("Using regular OpenAI configuration")
                self.openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
                self.embedding_model = settings.OPENAI_EMBEDDING_MODEL
                self.is_azure_openai = False
            else:
                raise ValueError("No OpenAI configuration found")
                
        except Exception as e:
            logger.error(f"Error setting up OpenAI client: {str(e)}")
            raise
        
    async def create_index_if_not_exists(self):
        """Create the search index if it doesn't exist"""
        try:
            try:
                self.index_client.get_index(self.index_name)
                logger.info(f"Index '{self.index_name}' already exists")
                return
            except:
                logger.info(f"Index '{self.index_name}' does not exist, creating...")
            
            fields = [
                SimpleField(name="id", type=SearchFieldDataType.String, key=True),
                SearchableField(name="benchmarkId", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="title", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
                SearchableField(name="description", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
                SearchableField(name="type", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="objectives", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
                SearchableField(name="materials", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
                SearchableField(name="files", type=SearchFieldDataType.Collection(SearchFieldDataType.String), facetable=True),
                SearchableField(name="text", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
                SearchField(name="embedding", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), 
                           vector_search_dimensions=3072, vector_search_profile_name="default-vector-profile"),
                SearchableField(name="grade_levels", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="subject_areas", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="audience", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SimpleField(name="resource_url", type=SearchFieldDataType.String),
                SimpleField(name="published_date", type=SearchFieldDataType.String, filterable=True, sortable=True)
            ]
            
            vector_search = VectorSearch(
                profiles=[
                    VectorSearchProfile(
                        name="default-vector-profile",
                        algorithm_configuration_name="default-algorithm"
                    )
                ],
                algorithms=[
                    HnswAlgorithmConfiguration(name="default-algorithm")
                ]
            )
            
            index = SearchIndex(
                name=self.index_name,
                fields=fields,
                vector_search=vector_search
            )
            
            self.index_client.create_index(index)
            logger.info(f"Successfully created index '{self.index_name}'")
            
        except Exception as e:
            logger.error(f"Error creating index: {str(e)}")
            raise
            
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using OpenAI (Azure or regular)"""
        try:
            if not text or not text.strip():
                return [0.0] * 1536  
                
            text = text[:8000]
            
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model
            )
            
            return response.data[0].embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            if "rate limit" in str(e).lower():
                logger.info("Rate limit hit, waiting 5 seconds...")
                time.sleep(5)
                try:
                    response = self.openai_client.embeddings.create(
                        input=text,
                        model=self.embedding_model
                    )
                    return response.data[0].embedding
                except:
                    pass
            return [0.0] * 1536

    def prepare_document(self, resource_json: Dict, benchmark_id: str, file_path: str = "") -> Dict:
        """Prepare document for indexing"""
        try:
            title = resource_json.get('Title', '')
            description = self._clean_html(resource_json.get('Description', ''))
            resource_id = str(resource_json.get("ResourceId", ""))
            lesson_plan_questions = resource_json.get("LessonPlanQuestions", [])
            objectives = []
            for question in lesson_plan_questions:
                title = question.get("Title", "")
                answer = self._clean_html(question.get("ResLessPlanQuestionAnswer", ""))
                if title and answer:
                    objectives.append(f"{title}: {answer}")
            objectives_text = " ".join(objectives)

            accommodation = self._clean_html(resource_json.get("Accomodation", ""))
            extensions = self._clean_html(resource_json.get("Extensions", ""))
            further = self._clean_html(resource_json.get("FurtherRecommendations", ""))

            extra_info = []
            if accommodation:
                extra_info.append(f"Accommodations: {accommodation}")
            if extensions:
                extra_info.append(f"Extensions: {extensions}")
            if further:
                extra_info.append(f"Further Recommendations: {further}")
            benchmark_ids = resource_json.get("BenchmarkIds", "")
            if benchmark_ids:
                extra_info.append(f"Benchmark IDs: {benchmark_ids}")

            primary_ict_id = resource_json.get("PrimaryResourceICTId", "")
            if primary_ict_id:
                extra_info.append(f"Primary Resource ICT ID: {primary_ict_id}")

            resource_type_id = resource_json.get("ResourceTypeId", "")
            if resource_type_id:
                extra_info.append(f"Resource Type ID: {resource_type_id}")

            extra_info_text = " ".join(extra_info)

            files = resource_json.get("Files", [])
            files_str = []
            for f in files:
                title = f.get("FileTitle", "").strip()
                desc = f.get("FileDescription", "")
                if title:
                    files_str.append(f"{title}: {self._clean_html(desc)}")
            files_text = ", ".join(files_str)


            benchmark_codes = [b.strip() for b in resource_json.get("BenchmarkCodes", "").split(",") if b.strip()]

            benchmark_id_cleaned = benchmark_id.replace(".", "_")
            document_id = f"{benchmark_id_cleaned}_{resource_id}"
            text_for_embedding = (
                f"{title} {description} "
                f"Grade Levels: {resource_json.get('GradeLevelNames', '')} "
                f"Subject Areas: {resource_json.get('SubjectAreaNames', '')} "
                f"Type: {resource_json.get('PrimaryICT', '')}"
            ).strip()
            print("######################################################################")
            print(text_for_embedding)
            print("*############################################################################")
            embedding = self.generate_embedding(text_for_embedding)
            
            if isinstance(embedding, (list, tuple)):
                embedding = [float(x) if x is not None else 0.0 for x in embedding]
            else:
                embedding = [0.0] * 1536




            document = {
                    "id": f"{benchmark_id_cleaned}_{resource_id}",
                    "benchmarkId": ", ".join(benchmark_codes),
                    "title": resource_json.get("Title", ""),
                    "description": self._clean_html(resource_json.get("Description", "")),
                    "type": resource_json.get("PrimaryICT", "Lesson Plan"),
                    "objectives": objectives_text,
                    "materials": self._clean_html(resource_json.get("SpecialMaterialsNeeded", "")),
                    "files": files_text,
                    "text": f"{self._clean_html(resource_json.get('Description', ''))} {extra_info_text}",
                    "embedding": embedding,
                    "grade_levels": str(resource_json.get("GradeLevelNames", "")).strip(),
                    "subject_areas": str(resource_json.get("SubjectAreaNames", "")).strip(),
                    "audience": str(resource_json.get("IntendedAudienceNames", "")).strip(),
                    "resource_url": str(resource_json.get("ResourceUrl", "")).strip(),
                    "published_date": str(resource_json.get("PublishedDate", "")).strip()
                    
                }
            

            document = {k: v for k, v in document.items() 
                       if v is not None and v != "" and v != []}
            if "embedding" not in document or not isinstance(document["embedding"], list):
                raise ValueError("Invalid embedding format")
                
            if "files" in document and not isinstance(document["files"], str):
                document["files"] = ""

            return document

        except Exception as e:
            logger.error(f"Error preparing document: {str(e)}")
            return {
                "id": f"{benchmark_id}_{resource_id if resource_id else 'fallback'}",
                "benchmarkId": benchmark_id,
                "title": title if 'title' in locals() else f"Resource {benchmark_id}",
                "type": "problem_solving",
                "text": str(resource_json)[:500],
                "embedding": [0.0] * 1536,
                "files": []  
            }

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags from text"""
        if not text:
            return ""
        clean = re.compile('<.*?>')
        cleaned_text = re.sub(clean, '', text)
        return cleaned_text.strip()

    def _extract_objectives(self, resource_json) -> str:
        """Extract objectives from lesson plan questions"""
        if not isinstance(resource_json, dict) or "LessonPlanQuestions" not in resource_json:
            return ""
        
        for question in resource_json["LessonPlanQuestions"]:
            title = question.get("Title", "")
            if "Learning Objectives" in title or "Objective" in title:
                return self._clean_html(question.get("ResLessPlanQuestionAnswer", ""))
        return ""

    def _extract_procedures(self, resource_json) -> str:
        """Extract teaching procedures from lesson plan questions"""
        if not isinstance(resource_json, dict) or "LessonPlanQuestions" not in resource_json:
            return ""
            
        procedures = []
        for question in resource_json["LessonPlanQuestions"]:
            title = question.get("Title", "")
            if any(keyword in title for keyword in ["Teaching Phase", "Procedure", "Activity", "Step"]):
                answer = self._clean_html(question.get("ResLessPlanQuestionAnswer", ""))
                if answer:
                    procedures.append(answer)
        return " ".join(procedures)

    async def index_documents(self, documents: List[Dict]) -> int:
        """Index multiple documents in batch"""
        if not documents:
            return 0
            
        try:
            valid_documents = []
            for doc in documents:
                if not self._validate_document(doc):
                    logger.error(f"Invalid document structure for ID: {doc.get('id', 'unknown')}")
                    continue
                valid_documents.append(doc)
            if not valid_documents:
                logger.error("No valid documents to index")
                return 0
            results = await self.search_client.upload_documents(valid_documents)
            
            successful_uploads = sum(1 for result in results if result.succeeded)
            
            if successful_uploads < len(valid_documents):
                failed_uploads = len(valid_documents) - successful_uploads
                logger.warning(f"Successfully indexed {successful_uploads} documents, failed: {failed_uploads}")
            else:
                logger.info(f"Successfully indexed {successful_uploads} documents")
                
            return successful_uploads
        except Exception as e:
            logger.error(f"Error indexing documents batch: {str(e)}")
            return 0

    async def close(self):
        """Close the search client"""
        if self.search_client:
            await self.search_client.close()

    def _validate_document(self, doc: Dict) -> bool:
        """Validate document structure before indexing"""
        try:
            if not all(key in doc for key in ["id", "benchmarkId", "title"]):
                return False

            if "embedding" in doc:
                if not isinstance(doc["embedding"], list):
                    return False
                if not all(isinstance(x, (int, float)) for x in doc["embedding"]):
                    return False
            if "files" in doc and not isinstance(doc["files"], str):
                return False

            string_fields = ["id", "benchmarkId", "title", "description", "type", 
                            "objectives", "materials", "text", "grade_levels", 
                            "subject_areas", "audience", "resource_url", "published_date"]
            
            for field in string_fields:
                if field in doc and not isinstance(doc[field], str):
                    return False

            return True
        except Exception:
            return False


class BlobDataProcessor:
    def __init__(self, connection_string: str, containers: List[str]):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string,
            connection_timeout=300,  
            read_timeout=300        
        )
        self.containers = containers if isinstance(containers, list) else [containers]
        
    def list_all_lessonplan_files(self) -> List[tuple]:
        """List all resource files from lessonplan folders across all containers with improved error handling"""
        all_files = []
        
        for container_name in self.containers:
            max_retries = 3
            retry_delay = 10
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Scanning container '{container_name}' for lesson plan files (attempt {attempt + 1}/{max_retries})...")
                    
                    container_client = self.blob_service_client.get_container_client(container_name)
                    
                    logger.info("Testing container connectivity...")
                    try:
                        next(container_client.list_blobs())
                        logger.info("Container connectivity test successful")
                    except StopIteration:
                        logger.info("Container is empty but accessible")
                    except Exception as e:
                        raise Exception(f"Container connectivity test failed: {str(e)}")
                    
                    logger.info("Listing blobs with 'lessonplans/' prefix...")
                    lessonplan_files = []
                    benchmark_folders = set()
                    
                    continuation_token = None
                    total_blobs_checked = 0
                    
                    while True:
                        try:
                            blob_list = container_client.list_blobs(
                                name_starts_with='lessonplans/',
                            )
                            
                            for blob in blob_list:
                                blob_name = blob.name
                                total_blobs_checked += 1
                                
                                if total_blobs_checked % 100 == 0:
                                    logger.info(f"Processed {total_blobs_checked} blobs...")
                                
                                path_parts = blob_name.split('/')
                                
                                if len(path_parts) >= 3:
                                    benchmark_id = path_parts[1]
                                    filename = path_parts[-1]
                                    
                                    if not filename:
                                        continue
                                        
                                    benchmark_folders.add(benchmark_id)
                                    
                                    if any(filename.lower().endswith(ext) for ext in ['.json', '.txt', '.html', '.htm', '.md']):
                                        resource_id = filename.rsplit('.', 1)[0]
                                        lessonplan_files.append((container_name, blob_name, benchmark_id, resource_id))
                            
                            break  
                            
                        except Exception as e:
                            logger.error(f"Error processing page: {str(e)}")
                            break
                    
                    logger.info(f"Completed scanning. Total blobs checked: {total_blobs_checked}")
                    logger.info(f"Found {len(benchmark_folders)} benchmark folders: {sorted(list(benchmark_folders)[:10])}{'...' if len(benchmark_folders) > 10 else ''}")
                    logger.info(f"Found {len(lessonplan_files)} lesson plan files in container '{container_name}'")
                    
                    if lessonplan_files:
                        all_files.extend(lessonplan_files)
                        logger.info(f"Successfully processed container '{container_name}' on attempt {attempt + 1}")
                        break
                    else:
                        logger.warning(f"No lesson plan files found in container '{container_name}'")
                        break
                        
                except (ServiceRequestError, AzureError) as e:
                    logger.error(f"Azure service error on attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2 
                    else:
                        logger.error(f"Failed to access container '{container_name}' after {max_retries} attempts")
                        
                except Exception as e:
                    logger.error(f"Unexpected error accessing container '{container_name}' on attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(f"Failed to access container '{container_name}' after {max_retries} attempts")
        
        logger.info(f"Total lesson plan files found across all containers: {len(all_files)}")
        return all_files
    
    def download_resource_file(self, container_name: str, blob_name: str) -> Optional[Dict]:
        """Download and parse a resource file from blob storage with retry logic"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name, 
                    blob=blob_name
                )
                
                logger.debug(f"Downloading {blob_name} (attempt {attempt + 1}/{max_retries})")
                
                download_stream = blob_client.download_blob(timeout=120)  
                content = download_stream.readall().decode('utf-8')
                
                file_extension = blob_name.lower().split('.')[-1]
                
                if file_extension == 'json':
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in file '{blob_name}': {str(e)}")
                        resource_id = blob_name.split('/')[-1].replace('.json', '')
                        return {
                            "ResourceId": resource_id,
                            "Title": f"Resource {resource_id}",
                            "Description": content[:500] + "..." if len(content) > 500 else content,
                            "PrimaryICT": "lesson_plan",
                            "BenchmarkCodes": "",
                            "Files": [],
                            "LessonPlanQuestions": []
                        }
                else:
                    resource_id = blob_name.split('/')[-1].rsplit('.', 1)[0]
                    return {
                        "ResourceId": resource_id,
                        "Title": f"Lesson Plan: {resource_id}",
                        "Description": content,
                        "PrimaryICT": "lesson_plan",
                        "type": file_extension,
                        "content": content
                    }
                    
            except (ServiceRequestError, AzureError) as e:
                logger.error(f"Azure service error downloading '{blob_name}' (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying download in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to download '{blob_name}' after {max_retries} attempts")
                    
            except Exception as e:
                logger.error(f"Unexpected error downloading file '{blob_name}' (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying download in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to download '{blob_name}' after {max_retries} attempts")
        
        return None
    