import json
import boto3
import os
from typing import List, Dict, Any
import logging
import time
from urllib.parse import unquote_plus
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
s3_client = boto3.client("s3")
batch_client = boto3.client("batch")

# 환경변수
JOB_QUEUE = os.environ.get("BATCH_JOB_QUEUE", "default-queue")
JOB_DEFINITION = os.environ.get("BATCH_JOB_DEFINITION", "default-job-def")

# DB 연결 정보
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")


@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=RealDictCursor,
        )
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    S3 이벤트를 처리하고 JSON 파일에서 place_id를 추출하여 DB에 저장 후 Batch 작업 실행

    Args:
        event: S3 이벤트 정보
        context: Lambda 실행 컨텍스트

    Returns:
        처리 결과
    """
    try:
        total_submitted_jobs = 0
        total_saved_restaurants = 0

        # S3 이벤트에서 버킷과 키 정보 추출
        for record in event["Records"]:
            # S3 이벤트 정보 파싱
            s3_event = record["s3"]
            bucket_name = s3_event["bucket"]["name"]
            object_key = s3_event["object"]["key"]
            object_key = unquote_plus(object_key)

            logger.info(f"Processing file: s3://{bucket_name}/{object_key}")

            # S3에서 파일 읽기
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response["Body"].read().decode("utf-8")
            data = json.loads(file_content)

            logger.info(f"Successfully loaded JSON from {object_key}")

            # 식당 정보를 DB에 저장
            saved_restaurants = save_restaurants_to_db(data)
            total_saved_restaurants += len(saved_restaurants)
            logger.info(
                f"Saved {len(saved_restaurants)} restaurants to DB from {object_key}"
            )

            # 저장된 식당들에 대해서만 Batch 작업 실행
            job_responses = []
            for restaurant in saved_restaurants:
                job_response = submit_batch_job(
                    place_id=restaurant["place_id"],
                    source_bucket=bucket_name,
                    source_key=object_key,
                )
                if job_response:
                    job_responses.append(job_response)

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


def save_restaurants_to_db(data: Any) -> List[Dict[str, Any]]:
    """
    JSON 데이터에서 식당 정보를 추출하여 DB에 저장

    Args:
        data: JSON 데이터 (리스트 또는 딕셔너리)

    Returns:
        저장된 식당 정보 리스트
    """
    saved_restaurants = []

    # 식당 리스트 추출
    restaurants = []
    if isinstance(data, list):
        restaurants = data
    elif isinstance(data, dict):
        # 딕셔너리인 경우 리스트 형태의 값들 찾기
        for key, value in data.items():
            if isinstance(value, list):
                restaurants.extend(value)

    if not restaurants:
        logger.warning("No restaurant data found in JSON")
        return saved_restaurants

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for restaurant in restaurants:
                if not isinstance(restaurant, dict):
                    continue

                # 필수 필드 확인
                required_fields = [
                    "place_id",
                    "name",
                    "address",
                    "latitude",
                    "longitude",
                ]
                if not all(field in restaurant for field in required_fields):
                    logger.warning(
                        f"Missing required fields in restaurant data: {restaurant}"
                    )
                    continue

                try:
                    # INSERT ... ON CONFLICT DO UPDATE 사용하여 중복 처리
                    insert_query = """
                        INSERT INTO restaurant (
                            place_id, name, address, latitude, longitude, thumbnail
                        ) VALUES (
                            %(place_id)s, %(name)s, %(address)s, 
                            %(latitude)s, %(longitude)s, %(thumbnail)s
                        ) ON CONFLICT (place_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            address = EXCLUDED.address,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            thumbnail = EXCLUDED.thumbnail
                        RETURNING id, place_id, name, address, latitude, longitude, thumbnail
                    """

                    restaurant_data = {
                        "place_id": str(restaurant["place_id"]),
                        "name": restaurant["name"],
                        "address": restaurant["address"],
                        "latitude": float(restaurant["latitude"]),
                        "longitude": float(restaurant["longitude"]),
                        "thumbnail": restaurant.get("thumbnail"),  # optional field
                    }

                    cursor.execute(insert_query, restaurant_data)
                    saved_restaurant = cursor.fetchone()
                    print(saved_restaurant)

                    if saved_restaurant:
                        saved_restaurants.append(dict(saved_restaurant))
                        logger.info(
                            f"Saved restaurant: {saved_restaurant['name']} (place_id: {saved_restaurant['place_id']})"
                        )
                    conn.commit()
                except Exception as e:
                    logger.error(
                        f"Error saving restaurant {restaurant.get('place_id', 'unknown')}: {str(e)}"
                    )
                    conn.rollback()
                    # 개별 레코드 실패 시 다음 레코드 처리 계속
                    continue

    return saved_restaurants


def extract_place_ids(data: Any) -> List[str]:
    """
    JSON 데이터에서 place_id 값들을 추출

    Args:
        data: JSON 데이터 (리스트 또는 딕셔너리)

    Returns:
        place_id 리스트
    """
    place_ids = []

    # 데이터가 리스트인 경우
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "place_id" in item:
                place_id = item.get("place_id")
                if place_id:
                    place_ids.append(str(place_id))

    # 데이터가 딕셔너리인 경우 (예: {"items": [...], "places": [...]})
    elif isinstance(data, dict):
        # 모든 값을 순회하며 place_id 찾기
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and "place_id" in item:
                        place_id = item.get("place_id")
                        if place_id:
                            place_ids.append(str(place_id))
            elif isinstance(value, dict) and "place_id" in value:
                place_id = value.get("place_id")
                if place_id:
                    place_ids.append(str(place_id))

    # 중복 제거
    return list(set(place_ids))


def submit_batch_job(
    place_id: str, source_bucket: str, source_key: str
) -> Dict[str, Any]:
    """
    AWS Batch 작업 제출

    Args:
        place_id: 처리할 place_id
        source_bucket: 소스 S3 버킷
        source_key: 소스 S3 키

    Returns:
        Batch 작업 응답
    """
    try:
        job_name = f"process-place-{place_id}-{int(time.time())}"

        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            parameters={},
            containerOverrides={
                "environment": [
                    {"name": "PLACE_ID", "value": place_id},
                    {"name": "SOURCE_BUCKET", "value": source_bucket},
                    {"name": "SOURCE_KEY", "value": source_key},
                ]
            },
        )

        logger.info(f"Submitted batch job {job_name} for place_id: {place_id}")
        return response

    except Exception as e:
        logger.error(f"Error submitting batch job for place_id {place_id}: {str(e)}")
        return {}


def validate_json_structure(data: Any) -> bool:
    """
    JSON 데이터 구조 검증
    """
    if isinstance(data, list):
        return all(isinstance(item, dict) for item in data)
    elif isinstance(data, dict):
        return True
    return False


def process_large_file(
    bucket: str, key: str, chunk_size: int = 1024 * 1024
) -> List[str]:
    """
    대용량 JSON 파일을 스트리밍으로 처리
    """
    place_ids = []

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # 스트리밍 파싱 대신 일반 파싱 사용 (ijson 없이)
        file_content = response["Body"].read().decode("utf-8")
        data = json.loads(file_content)

        # 데이터가 리스트인 경우
        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict) and "place_id" in obj:
                    place_ids.append(str(obj["place_id"]))
        # 데이터가 딕셔너리인 경우
        elif isinstance(data, dict):
            # extract_place_ids 함수 재사용
            place_ids = extract_place_ids(data)

    except Exception as e:
        logger.error(f"Error processing large file: {str(e)}")

    return place_ids


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

    # 환경변수 설정
    # os.environ["BATCH_JOB_QUEUE"] = "test-queue"
    # os.environ["BATCH_JOB_DEFINITION"] = "test-job-def"
    # os.environ["DB_HOST"] = "localhost"
    # os.environ["DB_NAME"] = "restaurant_db"
    # os.environ["DB_USER"] = "postgres"
    # os.environ["DB_PASSWORD"] = "password"

    # 핸들러 실행
    result = handler(test_event, None)
    print(json.dumps(result, indent=2))
