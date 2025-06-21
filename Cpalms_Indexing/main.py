import json
import asyncio
import logging
import time
from typing import List, Dict, Set
from indexer import ResourceIndexer, BlobDataProcessor
from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IndexingPipeline:
    def __init__(self):
        self.indexer = ResourceIndexer(
            endpoint=settings.AZURE_SEARCH_ENDPOINT,
            key=settings.AZURE_SEARCH_API_KEY,
            index_name=settings.AZURE_SEARCH_INDEX_NAME
        )
        container_names = [name.strip() for name in settings.AZURE_STORAGE_CONTAINERS.split(',')]
        self.blob_processor = BlobDataProcessor(
            connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
            containers=container_names
        )
        
        self.stats = {
            'total_containers': 0,
            'total_files': 0,
            'successful_indexes': 0,
            'failed_indexes': 0,
            'start_time': None,
            'end_time': None,
            'benchmark_folders_found': 0
        }
        
        self.processed_files: Set[str] = set()  
        self.max_files_per_benchmark = getattr(settings, 'MAX_FILES_PER_BENCHMARK', 20)  
        self.max_total_files = getattr(settings, 'MAX_TOTAL_FILES', 500)  
        self.processing_timeout = getattr(settings, 'PROCESSING_TIMEOUT_MINUTES', 30)  
    
    async def process_all_resources(self) -> int:
        """Process all lesson plan resource files from all containers"""
        logger.info("Starting to process all lesson plan resource files...")
        
        process_start_time = time.time()
        
        all_files = self.blob_processor.list_all_lessonplan_files()
        
        if not all_files:
            logger.warning("No lesson plan resource files found in any container")
            return 0
        
        if len(all_files) > self.max_total_files:
            logger.warning(f"Found {len(all_files)} files, limiting to {self.max_total_files} for safety")
            all_files = all_files[:self.max_total_files]
        
        self.stats['total_files'] = len(all_files)
        benchmark_counts = {}
        filtered_files = []
        benchmark_file_counts = {}
        
        for container_name, blob_name, benchmark_id, resource_id in all_files:
            if benchmark_id not in benchmark_file_counts:
                benchmark_file_counts[benchmark_id] = 0
            
            if benchmark_file_counts[benchmark_id] >= self.max_files_per_benchmark:
                logger.warning(f"Skipping file {blob_name} - benchmark {benchmark_id} has reached limit of {self.max_files_per_benchmark} files")
                continue
            
            benchmark_file_counts[benchmark_id] += 1
            benchmark_counts[benchmark_id] = benchmark_counts.get(benchmark_id, 0) + 1
            
            file_key = f"{container_name}::{blob_name}"
            if file_key in self.processed_files:
                logger.warning(f"Skipping duplicate file: {blob_name}")
                continue
            
            filtered_files.append((container_name, blob_name, benchmark_id, resource_id))
            self.processed_files.add(file_key)
        
        self.stats['total_files'] = len(filtered_files)
        self.stats['benchmark_folders_found'] = len(benchmark_counts)
        
        logger.info(f"Files distributed across {len(benchmark_counts)} benchmark folders:")
        for benchmark, count in sorted(benchmark_counts.items()):
            logger.info(f"  {benchmark}: {count} files")
        
        documents_batch = []
        successful_count = 0
        batch_size = getattr(settings, 'BATCH_SIZE', 6) 
        
        logger.info(f"Processing {len(filtered_files)} files with batch size {batch_size}")
        
        for i, (container_name, blob_name, benchmark_id, resource_id) in enumerate(filtered_files, 1):
            try:
                elapsed_time = time.time() - process_start_time
                if elapsed_time > (self.processing_timeout * 60):
                    logger.error(f"Processing timeout reached ({self.processing_timeout} minutes). Stopping.")
                    break
                
                logger.info(f"Processing file {i}/{len(filtered_files)}: {blob_name}")
                logger.info(f"  Benchmark: {benchmark_id}, Resource: {resource_id}")
                
                resource_data = self.blob_processor.download_resource_file(container_name, blob_name)
                
                if resource_data is None:
                    logger.error(f"Failed to download or parse file: {blob_name}")
                    error_message = f"Failed to download or parse file: {blob_name}"
                    logger.error(error_message)

                    with open("failed_files_log.txt", "a") as f:
                        f.write(f"[DownloadError] File: {blob_name}, BenchmarkID: {benchmark_id}, ResourceID: {resource_id} — Reason: Failed to download or parse\n")
    
                    self.stats['failed_indexes'] += 1
                    continue
                
                if isinstance(resource_data, dict):
                    resource_benchmark_codes = resource_data.get("BenchmarkCodes", "")
                    if resource_benchmark_codes:
                        primary_benchmark = resource_benchmark_codes.split(",")[0].strip() or benchmark_id
                    else:
                        primary_benchmark = benchmark_id
                else:
                    primary_benchmark = benchmark_id
                
                document = self.indexer.prepare_document(resource_data, primary_benchmark, blob_name)                
                document_id = document.get('id')
                if not document_id:
                    logger.error(f"Document preparation failed for {blob_name} - no ID generated")
                    with open("failed_files_log.txt", "a") as f:
                        f.write(f"[PrepError] File: {blob_name}, BenchmarkID: {benchmark_id}, ResourceID: {resource_id} — Reason: No document ID generated\n")
                    self.stats['failed_indexes'] += 1
                    continue
                
                documents_batch.append(document)
                logger.debug(f"Prepared document with ID: {document_id}")
                
                if len(documents_batch) >= batch_size:
                    batch_success = await self.process_batch(documents_batch)
                    successful_count += batch_success
                    documents_batch = []
                    
                    await asyncio.sleep(0.5)
                    
                    if i % (batch_size * 5) == 0:
                        logger.info(f"Progress: {i}/{len(filtered_files)} files processed, {successful_count} documents indexed")
                
            except Exception as e:
                logger.error(f"Error processing file {blob_name}: {str(e)}")
                self.stats['failed_indexes'] += 1
                
                continue
        
        if documents_batch:
            batch_success = await self.process_batch(documents_batch)
            successful_count += batch_success
        
        logger.info(f"Completed processing all resources: {successful_count} documents indexed")
        return successful_count
    
    async def process_batch(self, documents: List[Dict]) -> int:
        """Process a batch of documents with retry logic"""
        max_retries = getattr(settings, 'MAX_RETRIES', 3)
        retry_delay = getattr(settings, 'RETRY_DELAY', 2)
        
        if not documents:
            logger.warning("Empty batch provided, skipping")
            return 0
        
        document_ids = [doc.get('id') for doc in documents]
        unique_ids = set(document_ids)
        if len(unique_ids) != len(document_ids):
            logger.warning(f"Found duplicate IDs in batch: {len(document_ids)} docs, {len(unique_ids)} unique IDs")
            seen_ids = set()
            deduplicated_docs = []
            for doc in documents:
                doc_id = doc.get('id')
                if doc_id not in seen_ids:
                    seen_ids.add(doc_id)
                    deduplicated_docs.append(doc)
            documents = deduplicated_docs
            logger.info(f"Deduplicated batch: {len(documents)} documents remaining")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Indexing batch of {len(documents)} documents (attempt {attempt + 1}/{max_retries})")
                
                successful_uploads = await self.indexer.index_documents(documents)
                self.stats['successful_indexes'] += successful_uploads
                
                if successful_uploads < len(documents):
                    failed_count = len(documents) - successful_uploads
                    self.stats['failed_indexes'] += failed_count
                    logger.warning(f"Batch partially successful: {successful_uploads}/{len(documents)} documents indexed")
                else:
                    logger.info(f"Batch fully successful: {successful_uploads} documents indexed")
                
                return successful_uploads
                
            except Exception as e:
                logger.error(f"Batch indexing attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"All retry attempts failed for batch of {len(documents)} documents")
                    self.stats['failed_indexes'] += len(documents)
        
        return 0
    
    def validate_configuration(self):
        """Validate that all required configuration is present"""
        required_settings = [
            'AZURE_SEARCH_ENDPOINT',
            'AZURE_SEARCH_API_KEY',
            'AZURE_SEARCH_INDEX_NAME',
            'AZURE_STORAGE_CONNECTION_STRING',
            'AZURE_STORAGE_CONTAINERS'
        ]      
        if hasattr(settings, 'OPENAI_API_KEY') and settings.OPENAI_API_KEY:
            required_settings.append('OPENAI_API_KEY')
        
        missing_settings = []
        for setting in required_settings:
            if not hasattr(settings, setting) or not getattr(settings, setting):
                missing_settings.append(setting)
        
        if missing_settings:
            raise ValueError(f"Missing required configuration: {', '.join(missing_settings)}")
        
        logger.info("Configuration validation passed")
        logger.info(f"Safety limits: {self.max_files_per_benchmark} files per benchmark, {self.max_total_files} total files max")
        logger.info(f"Processing timeout: {self.processing_timeout} minutes")
    
    def print_stats(self):
        """Print processing statistics"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("INDEXING PIPELINE COMPLETED")
        logger.info("="*60)
        logger.info(f"Total containers processed: {self.stats['total_containers']}")
        logger.info(f"Total benchmark folders found: {self.stats['benchmark_folders_found']}")
        logger.info(f"Total lesson plan files found: {self.stats['total_files']}")
        logger.info(f"Successfully indexed documents: {self.stats['successful_indexes']}")
        logger.info(f"Failed to index documents: {self.stats['failed_indexes']}")
        
        if self.stats['total_files'] > 0:
            success_rate = (self.stats['successful_indexes'] / self.stats['total_files'] * 100)
            logger.info(f"Success rate: {success_rate:.2f}%")
        else:
            logger.info("Success rate: 0.00%")
            
        logger.info(f"Total processing time: {duration:.2f} seconds")
        
        if duration > 0:
            processing_rate = self.stats['total_files'] / duration
            logger.info(f"Average processing rate: {processing_rate:.2f} files/second")
        else:
            logger.info("Average processing rate: 0.00 files/second")
            
        logger.info("="*60)
    
    async def run(self):
        """Run the complete indexing pipeline"""
        try:
            logger.info("Starting lesson plan indexing pipeline...")
            self.stats['start_time'] = time.time()
            
            self.validate_configuration()
            
            await self.indexer.create_index_if_not_exists()
            
            container_names = [name.strip() for name in settings.AZURE_STORAGE_CONTAINERS.split(',')]
            self.stats['total_containers'] = len(container_names)
            
            logger.info(f"Processing containers: {container_names}")
            
            await self.process_all_resources()
            
            self.stats['end_time'] = time.time()
            self.print_stats()
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}")
            raise
        finally:
            await self.indexer.close()

async def main():
    """Main entry point"""
    pipeline = IndexingPipeline()
    await pipeline.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        exit(1)
