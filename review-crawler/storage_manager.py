import boto3
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class ReviewStorageManager:
    def __init__(self, bucket_name, bucket_directory):
        self.bucket_name = bucket_name
        self.bucket_directory = bucket_directory
        self.s3 = boto3.client("s3")

    def upload_reviews_json(self, placeId, reviews):
        """
        리뷰 리스트(reviews)를 S3의 bucket에 placeId.json 파일로 저장
        기존 파일이 있으면 합치기
        """
        key = f"{self.bucket_directory}/{placeId}.json"

        # 기존 파일 확인 및 읽기
        existing_reviews = []
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            existing_data = response["Body"].read().decode("utf-8")
            existing_reviews = json.loads(existing_data)
            logger.info(f"기존 리뷰 {len(existing_reviews)}개 발견")
        except self.s3.exceptions.NoSuchKey:
            logger.info(f"{key} 파일이 없어서 새로 생성")
        except Exception as e:
            logger.error(f"파일 읽기 오류: {e}")

        # 기존 리뷰와 새 리뷰 합치기
        all_reviews = existing_reviews + reviews

        # 중복 제거가 필요한 경우 (예: review_id로 중복 체크)
        # seen = set()
        # unique_reviews = []
        # for review in all_reviews:
        #     if review.get('review_id') not in seen:
        #         seen.add(review.get('review_id'))
        #         unique_reviews.append(review)
        # all_reviews = unique_reviews

        # S3에 저장
        data = json.dumps(all_reviews, ensure_ascii=False, indent=2)
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data.encode("utf-8"))
        logger.info(
            f"Uploaded {key} to S3 bucket {self.bucket_name} (총 {len(all_reviews)}개 리뷰)"
        )

    def get_review_ids_with_s3_select(self, placeId):
        """
        S3 Select를 사용해 placeId.json 파일에서 리뷰 id만 리스트로 반환
        """
        key = f"{self.bucket_directory}/{placeId}.json"
        logger.info(key)
        try:
            response = self.s3.select_object_content(
                Bucket=self.bucket_name,
                Key=key,
                ExpressionType="SQL",
                Expression="SELECT * FROM S3Object[*] s",
                InputSerialization={"JSON": {"Type": "DOCUMENT"}},
                OutputSerialization={"JSON": {}},
            )

            # 모든 데이터를 먼저 수집
            all_data = b""
            for event in response["Payload"]:
                if "Records" in event:
                    all_data += event["Records"]["Payload"]

            # 한 번에 디코딩하고 처리
            ids = []
            if all_data:
                records = all_data.decode("utf-8")
                for line in records.strip().split("\n"):
                    if line:
                        obj = json.loads(line)["_1"]
                        for object in obj:
                            logger.info(object["id"])
                            ids.append(object["id"])

            logger.info(ids)
            return ids
        except Exception as e:
            logger.error(f"S3 Select error: {e}")
            return []
