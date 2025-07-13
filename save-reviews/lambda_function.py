import json
import boto3
import os
from typing import List, Dict, Any
import logging
from urllib.parse import unquote_plus
import urllib.request
import urllib.error

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 클라이언트 초기화
s3 = boto3.client("s3")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    S3 업로드 이벤트를 처리하는 Lambda 핸들러

    Args:
        event: S3 이벤트 정보
        context: Lambda 실행 컨텍스트

    Returns:
        처리 결과를 담은 딕셔너리
    """

    # API URL 환경 변수에서 가져오기
    api_url = os.environ.get("API_URL")
    if not api_url:
        logger.error("API_URL environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps("API_URL environment variable is not set"),
        }

    try:
        # S3 이벤트에서 버킷과 키 정보 추출
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"Processing file: {bucket}/{key}")

        # S3에서 파일 다운로드
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # JSON 파싱
        reviews = json.loads(file_content)

        if not isinstance(reviews, list):
            raise ValueError("File content is not a list of reviews")

        logger.info(f"Found {len(reviews)} reviews to process")

        # 처리 결과 저장
        success_count = 0
        failed_count = 0
        errors = []

        # 각 리뷰에 대해 API 호출
        for review in reviews[:10]:
            try:
                print(review)
                # 요청 데이터 준비
                request_data = {
                    "restaurantId": int(review.get("place_id", 0)),
                    "content": review.get("content", ""),
                    "hash": review.get("id", ""),
                }

                # API 호출
                result = send_review_to_api(api_url, request_data)

                if result["success"]:
                    success_count += 1
                    logger.info(f"Successfully sent review {request_data['hash']}")
                else:
                    failed_count += 1
                    error_msg = f"Failed to send review {request_data['hash']}: {result['error']}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            except Exception as e:
                failed_count += 1
                error_msg = (
                    f"Error processing review {review.get('id', 'unknown')}: {str(e)}"
                )
                logger.error(error_msg)
                errors.append(error_msg)

        # 처리 결과 로깅
        logger.info(
            f"Processing complete. Success: {success_count}, Failed: {failed_count}"
        )

        # 결과 반환
        result_body = {
            "message": "Processing complete",
            "file": f"{bucket}/{key}",
            "total_reviews": len(reviews),
            "success_count": success_count,
            "failed_count": failed_count,
        }

        if errors:
            result_body["errors"] = errors[:10]  # 최대 10개의 에러만 반환

        return {"statusCode": 200, "body": json.dumps(result_body)}

    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e), "message": "Failed to process S3 event"}
            ),
        }


def send_review_to_api(api_url: str, review_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    리뷰 데이터를 API로 전송

    Args:
        api_url: API 엔드포인트 URL
        review_data: 전송할 리뷰 데이터

    Returns:
        성공 여부와 에러 메시지를 담은 딕셔너리
    """
    try:
        # JSON 데이터 준비
        json_data = json.dumps(review_data).encode("utf-8")

        # HTTP 요청 생성
        req = urllib.request.Request(
            api_url,
            data=json_data,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(json_data)),
            },
            method="POST",
        )

        # API 호출 (타임아웃 10초)
        with urllib.request.urlopen(req, timeout=10) as response:
            response_data = response.read().decode("utf-8")

            # 성공적인 응답 (2xx)
            if 200 <= response.getcode() < 300:
                return {
                    "success": True,
                    "response": response_data,
                    "status_code": response.getcode(),
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.getcode()}: {response_data}",
                }

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else "No error body"
        return {"success": False, "error": f"HTTP {e.code}: {error_body}"}
    except urllib.error.URLError as e:
        return {"success": False, "error": f"URL Error: {str(e.reason)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "naver-map-review"},
                    "object": {"key": "31238198.json"},
                }
            }
        ]
    }

    # 환경 변수 설정 (테스트용)
    os.environ["API_URL"] = "http://localhost:8080/api/crawling-reviews"

    # 핸들러 실행
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
