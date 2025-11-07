"""
Module for S3 bucket cleaning and copying operations.
"""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Callable, Optional, List, Dict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

logger = logging.getLogger(__name__)


class S3Cleaner:
    """Class to handle cleaning and copying operations on S3 buckets."""
    
    def __init__(self, access_key_id: str, secret_access_key: str, region: Optional[str] = None):
        """
        Initialize the S3 client with credentials.
        
        Args:
            access_key_id: AWS Access Key ID
            secret_access_key: AWS Secret Access Key
            region: AWS region (optional)
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.s3_client = None
        self._connect()
    
    def _connect(self):
        """Establish connection with S3."""
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region
            )
            self.s3_client = session.client('s3')
        except Exception as e:
            logger.error(f"Error connecting to S3: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test the connection by listing buckets.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.s3_client.list_buckets()
            return True
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def list_buckets(self) -> List[str]:
        """
        List all accessible buckets.
        
        Returns:
            List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            return sorted(buckets)
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error listing buckets: {e}")
            return []
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists and is accessible.
        
        Args:
            bucket_name: Bucket name
            
        Returns:
            True if the bucket exists, False otherwise
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False
    
    def is_versioned(self, bucket_name: str) -> bool:
        """
        Check if a bucket has versioning enabled.
        
        Args:
            bucket_name: Bucket name
            
        Returns:
            True if the bucket has versioning, False otherwise
        """
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            return response.get('Status') == 'Enabled'
        except ClientError as e:
            logger.error(f"Error checking versioning: {e}")
            return False
    
    def count_objects(self, bucket_name: str, prefix: Optional[str] = None) -> Dict[str, int]:
        """
        Count objects in a bucket (with and without versions).
        
        Args:
            bucket_name: Bucket name
            prefix: Prefix to filter objects (optional)
            
        Returns:
            Dictionary with counts: {'objects': int, 'versions': int, 'delete_markers': int}
        """
        counts = {'objects': 0, 'versions': 0, 'delete_markers': 0}
        
        try:
            # Count objects without versioning
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix or ''):
                if 'Contents' in page:
                    counts['objects'] += len(page['Contents'])
            
            # Count versions if bucket has versioning
            if self.is_versioned(bucket_name):
                paginator = self.s3_client.get_paginator('list_object_versions')
                for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix or ''):
                    if 'Versions' in page:
                        counts['versions'] += len(page['Versions'])
                    if 'DeleteMarkers' in page:
                        counts['delete_markers'] += len(page['DeleteMarkers'])
        except ClientError as e:
            logger.error(f"Error counting objects: {e}")
            raise
        
        return counts
    
    def clean_bucket(self, bucket_name: str, progress_callback: Optional[Callable[[str], None]] = None, 
                    max_workers: int = 10, cancel_flag: Optional[Callable[[], bool]] = None,
                    pause_flag: Optional[Callable[[], bool]] = None) -> bool:
        """
        Delete all objects from a bucket, including versions if versioning is enabled.
        
        Args:
            bucket_name: Bucket name
            progress_callback: Function to report progress (optional)
            
        Returns:
            True if the operation was successful, False otherwise
        """
        try:
            if progress_callback:
                progress_callback(f"Verifying bucket '{bucket_name}'...")
            
            if not self.bucket_exists(bucket_name):
                error_msg = f"The bucket '{bucket_name}' does not exist or is not accessible"
                if progress_callback:
                    progress_callback(f"ERROR: {error_msg}")
                return False
            
            is_versioned = self.is_versioned(bucket_name)
            
            if progress_callback:
                progress_callback(f"Bucket {'has' if is_versioned else 'does not have'} versioning enabled")
            
            if is_versioned:
                return self._clean_versioned_bucket(bucket_name, progress_callback, max_workers, cancel_flag, pause_flag)
            else:
                return self._clean_non_versioned_bucket(bucket_name, progress_callback, max_workers, cancel_flag, pause_flag)
        
        except Exception as e:
            error_msg = f"Error cleaning bucket: {str(e)}"
            logger.error(error_msg)
            if progress_callback:
                progress_callback(f"ERROR: {error_msg}")
            return False
    
    def _delete_batch(self, bucket_name: str, objects_batch: List[Dict], progress_callback: Optional[Callable[[str], None]] = None) -> int:
        """Delete a batch of objects (used by threads)."""
        try:
            response = self.s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_batch}
            )
            deleted = len(response.get('Deleted', []))
            
            if response.get('Errors'):
                for error in response['Errors']:
                    logger.warning(f"Error deleting {error.get('Key')}: {error.get('Message')}")
            
            return deleted
        except Exception as e:
            logger.error(f"Error deleting batch: {e}")
            return 0
    
    def _clean_non_versioned_bucket(self, bucket_name: str, progress_callback: Optional[Callable[[str], None]] = None, 
                                   max_workers: int = 10, cancel_flag: Optional[Callable[[], bool]] = None,
                                   pause_flag: Optional[Callable[[], bool]] = None) -> bool:
        """Delete objects from a non-versioned bucket using concurrency, processing on the fly."""
        deleted_count = 0
        deleted_lock = threading.Lock()
        pages_processed = 0
        batches_created = 0
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            if progress_callback:
                progress_callback(f"Deleting objects (concurrency: {max_workers})...")
            
            # Process and delete on the fly using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                for page in paginator.paginate(Bucket=bucket_name):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        # Cancel pending futures
                        for future in futures:
                            future.cancel()
                        return False
                    
                    # Wait if paused
                    while pause_flag and pause_flag():
                        time.sleep(0.5)  # Wait 0.5 seconds
                        if cancel_flag and cancel_flag():
                            return False
                    
                    pages_processed += 1
                    if 'Contents' not in page or len(page['Contents']) == 0:
                        if progress_callback and pages_processed % 10 == 0:
                            progress_callback(f"Processing page {pages_processed}... (empty)")
                        continue
                    
                    # Create batches of up to 1000 objects from this page
                    objects_in_page = []
                    for obj in page['Contents']:
                        objects_in_page.append({'Key': obj['Key']})
                    
                    # Split into batches of 1000 (S3 limit)
                    for i in range(0, len(objects_in_page), 1000):
                        batch = objects_in_page[i:i+1000]
                        batches_created += 1
                        # Submit immediately for deletion
                        future = executor.submit(self._delete_batch, bucket_name, batch, progress_callback)
                        futures.append(future)
                        
                        if progress_callback and batches_created % 5 == 0:
                            progress_callback(f"Processing page {pages_processed}... Creating batch {batches_created} ({len(batch)} objects)")
                
                if progress_callback:
                    progress_callback(f"Total: {pages_processed} pages processed, {batches_created} batches created. Deleting...")
                
                # Process results as they complete
                completed_batches = 0
                for future in as_completed(futures):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        return False
                    
                    try:
                        deleted = future.result()
                        completed_batches += 1
                        with deleted_lock:
                            deleted_count += deleted
                            # Update more frequently
                            if progress_callback:
                                progress_callback(f"Deleted {deleted_count} objects... ({completed_batches}/{batches_created} batches completed)")
                    except Exception as e:
                        logger.error(f"Error in deletion thread: {e}")
                        if progress_callback:
                            progress_callback(f"ERROR in batch: {str(e)}")
            
            if progress_callback:
                progress_callback(f"✓ Cleanup completed. Total deleted: {deleted_count} objects")
            
            return True
        
        except ClientError as e:
            error_msg = f"Error deleting objects: {str(e)}"
            logger.error(error_msg)
            if progress_callback:
                progress_callback(f"ERROR: {error_msg}")
            return False
    
    def _clean_versioned_bucket(self, bucket_name: str, progress_callback: Optional[Callable[[str], None]] = None, 
                               max_workers: int = 10, cancel_flag: Optional[Callable[[], bool]] = None,
                               pause_flag: Optional[Callable[[], bool]] = None) -> bool:
        """Delete objects and versions from a versioned bucket using concurrency, processing on the fly."""
        deleted_count = 0
        deleted_lock = threading.Lock()
        pages_processed = 0
        batches_created = 0
        
        try:
            paginator = self.s3_client.get_paginator('list_object_versions')
            
            if progress_callback:
                progress_callback(f"Deleting versions and delete markers (concurrency: {max_workers})...")
            
            # Process and delete on the fly using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                for page in paginator.paginate(Bucket=bucket_name):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        # Cancel pending futures
                        for future in futures:
                            future.cancel()
                        return False
                    
                    # Wait if paused
                    while pause_flag and pause_flag():
                        time.sleep(0.5)  # Wait 0.5 seconds
                        if cancel_flag and cancel_flag():
                            return False
                    
                    pages_processed += 1
                    objects_to_delete = []
                    
                    # Add versions from this page
                    if 'Versions' in page:
                        for version in page['Versions']:
                            objects_to_delete.append({
                                'Key': version['Key'],
                                'VersionId': version['VersionId']
                            })
                    
                    # Add delete markers from this page
                    if 'DeleteMarkers' in page:
                        for marker in page['DeleteMarkers']:
                            objects_to_delete.append({
                                'Key': marker['Key'],
                                'VersionId': marker['VersionId']
                            })
                    
                    if not objects_to_delete:
                        if progress_callback and pages_processed % 10 == 0:
                            progress_callback(f"Processing page {pages_processed}... (0 objects in this page)")
                        continue
                    
                    # Split into batches of 1000 (S3 limit) and submit immediately for deletion
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i:i+1000]
                        future = executor.submit(self._delete_batch, bucket_name, batch, progress_callback)
                        futures.append(future)
                        batches_created += 1
                        
                        if progress_callback and batches_created % 5 == 0:
                            progress_callback(f"Processing page {pages_processed}... Creating batch {batches_created} ({len(batch)} objects)")
                
                if progress_callback:
                    progress_callback(f"Total: {pages_processed} pages processed, {batches_created} batches created. Deleting...")
                
                # Process results as they complete
                completed_batches = 0
                for future in as_completed(futures):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        return False
                    
                    try:
                        deleted = future.result()
                        completed_batches += 1
                        with deleted_lock:
                            deleted_count += deleted
                            # Update more frequently
                            if progress_callback:
                                progress_callback(f"Deleted {deleted_count} objects/versions... ({completed_batches}/{batches_created} batches completed)")
                    except Exception as e:
                        logger.error(f"Error in deletion thread: {e}")
                        if progress_callback:
                            progress_callback(f"ERROR in batch: {str(e)}")
            
            if progress_callback:
                progress_callback(f"✓ Cleanup completed. Total deleted: {deleted_count} objects/versions")
            
            return True
        
        except ClientError as e:
            error_msg = f"Error deleting versions: {str(e)}"
            logger.error(error_msg)
            if progress_callback:
                progress_callback(f"ERROR: {error_msg}")
            return False
    
    def _copy_single_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str, 
                          progress_callback: Optional[Callable[[str], None]] = None) -> bool:
        """Copy a single object (used by threads)."""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            return True
        except Exception as e:
            logger.error(f"Error copying {source_key}: {e}")
            return False
    
    def copy_objects(self, source_bucket: str, source_prefix: str,
                    dest_bucket: str, dest_prefix: str,
                    progress_callback: Optional[Callable[[str], None]] = None, max_workers: int = 20,
                    cancel_flag: Optional[Callable[[], bool]] = None,
                    pause_flag: Optional[Callable[[], bool]] = None) -> bool:
        """
        Copy objects from a source bucket to a destination bucket using concurrency.
        
        Args:
            source_bucket: Source bucket name
            source_prefix: Prefix/path in the source bucket
            dest_bucket: Destination bucket name
            dest_prefix: Prefix/path in the destination bucket
            progress_callback: Function to report progress (optional)
            
        Returns:
            True if the operation was successful, False otherwise
        """
        try:
            if progress_callback:
                progress_callback(f"Verifying buckets...")
            
            if not self.bucket_exists(source_bucket):
                error_msg = f"The source bucket '{source_bucket}' does not exist or is not accessible"
                if progress_callback:
                    progress_callback(f"ERROR: {error_msg}")
                return False
            
            if not self.bucket_exists(dest_bucket):
                error_msg = f"The destination bucket '{dest_bucket}' does not exist or is not accessible"
                if progress_callback:
                    progress_callback(f"ERROR: {error_msg}")
                return False
            
            # Normalize prefixes
            source_prefix = source_prefix.strip('/') if source_prefix else ''
            dest_prefix = dest_prefix.strip('/') if dest_prefix else ''
            
            copied_count = 0
            copied_lock = threading.Lock()
            pages_processed = 0
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            if progress_callback:
                progress_callback(f"Copying objects (concurrency: {max_workers})...")
            
            # Copy objects on the fly using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                total_objects_submitted = 0
                
                # First, iterate through all pages and create all futures
                for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        for future, _ in futures:
                            future.cancel()
                        return False
                    
                    # Wait if paused
                    while pause_flag and pause_flag():
                        time.sleep(0.5)  # Wait 0.5 seconds
                        if cancel_flag and cancel_flag():
                            return False
                    
                    if 'Contents' not in page:
                        continue
                    
                    pages_processed += 1
                    objects_in_page = 0
                    
                    for obj in page['Contents']:
                        source_key = obj['Key']
                        
                        # Calculate destination key
                        if source_prefix:
                            relative_key = source_key[len(source_prefix):].lstrip('/')
                        else:
                            relative_key = source_key
                        
                        # Build destination key
                        if dest_prefix:
                            dest_key = f"{dest_prefix}/{relative_key}" if relative_key else dest_prefix
                        else:
                            dest_key = relative_key
                        
                        # Submit immediately for copying
                        future = executor.submit(
                            self._copy_single_object,
                            source_bucket, source_key, dest_bucket, dest_key, progress_callback
                        )
                        futures.append((future, source_key))
                        objects_in_page += 1
                        total_objects_submitted += 1
                    
                    # Show progress each page
                    if progress_callback:
                        progress_callback(f"Processing page {pages_processed}... ({objects_in_page} objects in this page, {total_objects_submitted} total in queue)")
                
                # Show summary before processing
                if progress_callback:
                    progress_callback(f"Total: {pages_processed} pages processed, {total_objects_submitted} objects in queue. Copying...")
                
                # Process results as they complete using as_completed
                completed_count = 0
                for future in as_completed([f for f, _ in futures]):
                    # Check cancellation
                    if cancel_flag and cancel_flag():
                        if progress_callback:
                            progress_callback("Operation cancelled")
                        return False
                    
                    # Wait if paused
                    while pause_flag and pause_flag():
                        time.sleep(0.5)
                        if cancel_flag and cancel_flag():
                            return False
                    
                    try:
                        success = future.result()
                        if success:
                            with copied_lock:
                                copied_count += 1
                                completed_count += 1
                                # Update every 50 copied objects
                                if progress_callback and copied_count % 50 == 0:
                                    progress_callback(f"Copied {copied_count} objects... ({completed_count}/{total_objects_submitted} completed)")
                        else:
                            completed_count += 1
                            logger.error(f"Error copying object")
                    except Exception as e:
                        completed_count += 1
                        logger.error(f"Error in copy thread: {e}")
                        if progress_callback:
                            progress_callback(f"ERROR copying object: {str(e)}")
            
            if progress_callback:
                progress_callback(f"✓ Copy completed. Total copied: {copied_count} objects")
            
            return True
        
        except Exception as e:
            error_msg = f"Error copying objects: {str(e)}"
            logger.error(error_msg)
            if progress_callback:
                progress_callback(f"ERROR: {error_msg}")
            return False

