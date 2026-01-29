from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta

import os
import time
import logging

#root logger
logger = logging.getLogger()

to_clean: dict[str, str] = {
            "audit": "_created",
            "dbusage": "timestamp",
            "oauthtokens":"expiresAt",
            "openrpa_instances": "_created",
            "workitems": "_created",
            "entities_hist": "_created",
            "agents_hist": "_created",
            "config_hist": "_created",
            "mq_hist": "_created",
            "users_hist": "_created",
            "openrpa_hist": "_created",
            "nodered_hist": "_created",
            "fs.files": "_created"
        }

def delete_outdated(dry_run: bool = True, uri: str = "mongodb://mongodb:27017/?directConnection=true",  cutoff: int = 205) -> int:
    logger.info("Connecting to database")
    total_deleted = 0
    try:
        with MongoClient(uri) as client:

            client.admin.command("ping")
            if not "openflow" in client.list_database_names():
                logger.error("No openflow database found. Are multiple mongodb instances running or is the port diffrent?")
                return
    
            db = client["openflow"]
        
            for collection_name in to_clean.keys():
                if not collection_name in db.list_collection_names():
                    logger.warning(f"Collection {collection_name} not found. Skipping...")
                    continue

                collection_ = db[collection_name]
                query = {to_clean[collection_name]: {"$lt": datetime.now() - timedelta(days=cutoff)}}

                candidates = collection_.count_documents(query)
                if dry_run:
                    logger.warning(f"{collection_}: {candidates} documents would be deleted")
                    continue

                if collection_name == "fs.files":
                    files_meta_data = collection_.find(query)
                    for file_meta_data in files_meta_data:
                        file_chunks_query = {"files_id": file_meta_data["_id"]}
                        
                        file_chunks = db["fs.chunks"].find(file_chunks_query)
                        if file_chunks is None:
                            logger.warning(f"Nothing found for file id {file_meta_data['_id']}. Skipping...")
                            continue

                        db["fs.chunks"].delete_one(file_chunks_query)

                    
                res = db[collection_.name].delete_many(query)
                total_deleted += res.deleted_count
                logger.info(f"Deleted {res.deleted_count} documents")
        logger.info(f"Delted {total_deleted} docs in total")
        return 0

    except PyMongoError as e:
        logger.error("MongoDB Error:", repr(e))
        return 2
    except Exception as e:
        logger.error("Error:", repr(e))
        return 1


if __name__ == "__main__":
    dry_run = os.environ.get("DRY_RUN", "1").lower() in ("1", "true", "yes", "y")
    days = int(os.environ.get("RETENTION_DAYS", "30"))
    uri = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/?directConnection=true")
    raise SystemExit(delete_outdated(dry_run, uri, days))
   # with MongoClient(uri) as client:
   #     db = client["openflow"]
   #     file_data = db["fs.files"].find_one()
   #     id_ = file_data["_id"]
   #     print(db["fs.chunks"].find({"files_id": id_})[0])
