import boto3
import json


class RestaurantStorageManager:
    def __init__(
        self,
        bucket_name,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name=None,
    ):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def upload_restaurants_json(self, query, restaurants):
        """
        식당 리스트(restaurants)를 S3의 bucket에 place_id.json 파일로 저장
        기존 파일이 있으면 합치기
        """
        key = f"{query}.json"

        # 기존 파일 확인 및 읽기
        existing_reviews = []
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            existing_data = response["Body"].read().decode("utf-8")
            existing_reviews = json.loads(existing_data)
            print(f"기존 리뷰 {len(existing_reviews)}개 발견")
        except self.s3.exceptions.NoSuchKey:
            print(f"{key} 파일이 없어서 새로 생성")
        except Exception as e:
            print(f"파일 읽기 오류: {e}")

        # 기존 식당과 새 식당 합치기
        all_reviews = existing_reviews + restaurants

        # S3에 저장
        data = json.dumps(all_reviews, ensure_ascii=False, indent=2)
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data.encode("utf-8"))
        print(
            f"Uploaded {key} to S3 bucket {self.bucket_name} (총 {len(all_reviews)}개 리뷰)"
        )

    def get_restaurant_ids_with_s3_select(self, query):
        """
        S3 Select를 사용해 query.json 파일에서 식당 id만 리스트로 반환
        """
        key = f"{query}.json"
        print(key)
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
                            print(object["place_id"])
                            ids.append(object["place_id"])

            print(ids)
            return ids
        except Exception as e:
            print(f"S3 Select error: {e}")
            return []
