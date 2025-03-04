import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from datetime import datetime, timezone
import os

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Blob cleanup function triggered')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    try:
        # Get configuration and log it
        account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
        retention_days = int(os.environ.get("RETENTION_DAYS", "90"))
        
        # Validate configuration
        if not account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")
            
        logging.info(f"Configuration: Storage Account: {account_name}, Retention Days: {retention_days}")
        
        # Create the credential and log token acquisition
        logging.info("Acquiring managed identity token...")
        credential = DefaultAzureCredential()
        
        # Create the BlobServiceClient
        account_url = f"https://{account_name}.blob.core.windows.net"
        logging.info(f"Connecting to storage account: {account_url}")
        blob_service_client = BlobServiceClient(account_url, credential=credential)

        # Verify storage account access
        try:
            account_info = blob_service_client.get_account_information()
            logging.info(f"Successfully connected to storage account. SKU: {account_info['sku_name']}")
        except Exception as account_error:
            logging.error(f"Failed to access storage account: {str(account_error)}")
            raise

        # Get all containers with error handling
        try:
            logging.info("Listing containers...")
            containers = list(blob_service_client.list_containers())
            logging.info(f"Found {len(containers)} containers")
        except Exception as container_error:
            logging.error(f"Error listing containers: {str(container_error)}")
            raise
        
        deleted_count = 0
        processed_count = 0
        error_count = 0

        # Process each container
        for container in containers:
            logging.info(f"Processing container: {container.name}")
            container_client = blob_service_client.get_container_client(container.name)
            
            try:
                # List all blobs in the container
                blobs = list(container_client.list_blobs())
                logging.info(f"Found {len(blobs)} blobs in container {container.name}")
                
                for blob in blobs:
                    processed_count += 1
                    
                    # Log blob details for debugging
                    logging.info(f"Processing blob: {blob.name}")
                    logging.info(f"Blob tier: {blob.blob_tier}")
                    logging.info(f"Last modified: {blob.last_modified}")
                    
                    # Calculate age in days
                    age_days = (datetime.now(timezone.utc) - blob.last_modified).days
                    logging.info(f"Blob age: {age_days} days")
                    
                    # Check if blob should be deleted
                    if (blob.blob_tier == 'Archive' and age_days > retention_days):
                        try:
                            container_client.delete_blob(blob.name)
                            deleted_count += 1
                            logging.info(f"Successfully deleted blob: {container.name}/{blob.name}")
                        except ResourceNotFoundError:
                            logging.warning(f"Blob not found (already deleted?): {container.name}/{blob.name}")
                            error_count += 1
                        except Exception as delete_error:
                            logging.error(f"Error deleting blob {container.name}/{blob.name}: {str(delete_error)}")
                            error_count += 1
                    else:
                        logging.info(f"Skipping blob {blob.name} (tier: {blob.blob_tier}, age: {age_days} days)")
                
            except Exception as container_error:
                logging.error(f"Error processing container {container.name}: {str(container_error)}")
                error_count += 1
                continue

        # Log summary
        logging.info(f"Cleanup Summary:")
        logging.info(f"- Processed containers: {len(containers)}")
        logging.info(f"- Total blobs processed: {processed_count}")
        logging.info(f"- Blobs deleted: {deleted_count}")
        logging.info(f"- Errors encountered: {error_count}")

    except ValueError as ve:
        logging.error(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        logging.error(f"Error in blob cleanup function: {str(e)}")
        logging.error(f"Error type: {type(e)}")
        raise