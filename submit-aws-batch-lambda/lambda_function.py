import json
import boto3
import os
from typing import List, Dict, Any
import logging
import time
from urllib.parse import unquote_plus
import urllib.request

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
s3_client = boto3.client("s3")
batch_client = boto3.client("batch")

# 환경변수
JOB_QUEUE = os.environ.get("BATCH_JOB_QUEUE", "default-queue")
JOB_DEFINITION = os.environ.get("BATCH_JOB_DEFINITION", "default-job-def")
API_URL = os.environ.get(
    "API_URL", "http://localhost:8080/api/restaurant"
)  # 추가: API URL 환경변수


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    S3 이벤트를 처리하고 JSON 파일에서 placeId를 추출하여 DB에 저장 후 Batch 작업 실행
    """
    total_submitted_jobs = 0
    total_saved_restaurants = 0
    try:
        for record in event["Records"]:
            s3_event = record["s3"]
            bucket_name = s3_event["bucket"]["name"]
            object_key = s3_event["object"]["key"]
            object_key = unquote_plus(object_key)

            logger.info(f"Processing file: s3://{bucket_name}/{object_key}")

            data = load_json_from_s3(bucket_name, object_key)
            logger.info(f"Successfully loaded JSON from {object_key}")

            saved_restaurants = save_restaurants_to_db(data)
            total_saved_restaurants += len(saved_restaurants)
            logger.info(
                f"Saved {len(saved_restaurants)} restaurants to DB from {object_key}"
            )

            job_responses = submit_batch_jobs_for_restaurants(
                saved_restaurants, bucket_name, object_key
            )
            total_submitted_jobs += len(job_responses)
            logger.info(f"Submitted {len(job_responses)} batch jobs for {object_key}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Successfully processed S3 event",
                    "restaurants_saved": total_saved_restaurants,
                    "jobs_submitted": total_submitted_jobs,
                }
            ),
        }
    except s3_client.exceptions.NoSuchKey as e:
        logger.error(f"No such key: {object_key}")
        return {
            "statusCode": 404,
            "body": json.dumps({"error": f"File not found: {object_key}"}),
        }
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def load_json_from_s3(bucket_name: str, object_key: str) -> Any:
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response["Body"].read().decode("utf-8")
    return json.loads(file_content)


def save_restaurants_to_db(data: Any) -> List[Dict[str, Any]]:
    saved_restaurants = []
    restaurants = []
    if isinstance(data, list):
        restaurants = data
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                restaurants.extend(value)

    if not restaurants:
        logger.warning("No restaurant data found in JSON")
        return saved_restaurants

    if not API_URL:
        logger.error("API_URL 환경변수가 설정되어 있지 않습니다.")
        return saved_restaurants

    for restaurant in restaurants:
        if not isinstance(restaurant, dict):
            continue
        if not is_valid_restaurant(restaurant):
            logger.warning(f"Missing required fields in restaurant data: {restaurant}")
            continue
        print(restaurant)
        saved_data = post_restaurant_to_api(restaurant)
        if saved_data:
            saved_restaurants.append(saved_data)
    return saved_restaurants


def is_valid_restaurant(restaurant: Dict[str, Any]) -> bool:
    required_fields = ["placeId", "name", "address", "latitude", "longitude"]
    return all(field in restaurant for field in required_fields)


def post_restaurant_to_api(restaurant: Dict[str, Any]) -> Dict[str, Any]:
    try:
        req = urllib.request.Request(
            API_URL + "/api/restaurant",
            data=json.dumps(restaurant).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200 or response.status == 201:
                logger.info(
                    f"Saved restaurant via API: {restaurant['name']} (placeId: {restaurant['placeId']})"
                )
                return json.loads(response.read().decode("utf-8"))
            else:
                logger.error(
                    f"API 저장 실패: {restaurant.get('placeId', 'unknown')} - status {response.status}, response: {response.read().decode('utf-8')}"
                )
                return {}
    except Exception as e:
        logger.error(
            f"Error saving restaurant {restaurant.get('placeId', 'unknown')} via API: {str(e)}"
        )
        return {}


def submit_batch_jobs_for_restaurants(
    restaurants: List[Dict[str, Any]], bucket_name: str, object_key: str
) -> List[Dict[str, Any]]:
    job_responses = []
    for restaurant in restaurants:
        try:
            job_response = submit_batch_job(
                placeId=restaurant["placeId"],
                source_bucket=bucket_name,
                source_key=object_key,
            )
            if job_response:
                job_responses.append(job_response)
        except Exception as e:
            logger.error(
                f"Error submitting batch job for placeId {restaurant.get('placeId', 'unknown')}: {str(e)}"
            )
    return job_responses


def submit_batch_job(
    placeId: str, source_bucket: str, source_key: str
) -> Dict[str, Any]:
    job_name = f"process-place-{placeId}-{int(time.time())}"
    response = batch_client.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        parameters={},
        containerOverrides={
            "environment": [
                {"name": "PLACE_ID", "value": placeId},
                {"name": "SOURCE_BUCKET", "value": source_bucket},
                {"name": "SOURCE_KEY", "value": source_key},
            ]
        },
    )
    logger.info(f"Submitted batch job {job_name} for placeId: {placeId}")
    return response


def extract_placeIds(data: Any) -> List[str]:
    """
    JSON 데이터에서 placeId 값들을 추출

    Args:
        data: JSON 데이터 (리스트 또는 딕셔너리)

    Returns:
        placeId 리스트
    """
    placeIds = []

    # 데이터가 리스트인 경우
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "placeId" in item:
                placeId = item.get("placeId")
                if placeId:
                    placeIds.append(str(placeId))

    # 중복 제거
    return list(set(placeIds))


def process_large_file(
    bucket: str, key: str, chunk_size: int = 1024 * 1024
) -> List[str]:
    """
    대용량 JSON 파일을 스트리밍으로 처리
    """
    placeIds = []

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # 스트리밍 파싱 대신 일반 파싱 사용 (ijson 없이)
        file_content = response["Body"].read().decode("utf-8")
        data = json.loads(file_content)

        # 데이터가 리스트인 경우
        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict) and "placeId" in obj:
                    placeIds.append(str(obj["placeId"]))
        # 데이터가 딕셔너리인 경우
        elif isinstance(data, dict):
            # extract_placeIds 함수 재사용
            placeIds = extract_placeIds(data)

    except Exception as e:
        logger.error(f"Error processing large file: {str(e)}")

    return placeIds


# 테스트용 로컬 실행
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "naver-map-restaurant"},
                    "object": {"key": "공덕역 식당.json"},
                }
            }
        ]
    }

    # 핸들러 실행
    result = handler(test_event, None)
    print(json.dumps(result, indent=2))
