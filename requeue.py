import os
from dotenv import load_dotenv
import redis
from rq import Queue
from rq.registry import FailedJobRegistry

load_dotenv()
redis_conn = redis.Redis.from_url(os.getenv('REDIS_URL'))
queue = Queue('default', connection=redis_conn)
registry = FailedJobRegistry(queue=queue)

job_ids = registry.get_job_ids()
print(f"Found {len(job_ids)} failed jobs")
for job_id in job_ids:
    registry.requeue(job_id)
    print(f"Requeued {job_id}")