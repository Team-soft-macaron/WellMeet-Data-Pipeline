import os
from storage_manager import ReviewStorageManager
from naver_crawler import NaverMapReviewCrawler


# 사용 예시
def handler(event, context):
    # 환경변수에서 S3 정보 읽기
    bucket_name = os.environ.get("REVIEW_S3_BUCKET_NAME")
    region_name = os.environ.get("AWS_REGION")

    # 환경변수에서 placeId 읽기
    placeId = os.environ.get("placeId")
    if not placeId:
        print("환경변수 placeId가 설정되어 있지 않습니다.")
        return
    if not bucket_name:
        print("환경변수 S3_BUCKET_NAME이 설정되어 있지 않습니다.")
        return

    # S3 매니저 생성
    storage_manager = ReviewStorageManager(
        bucket_name=bucket_name,
        region_name=region_name,
    )

    # 기존 리뷰 id set 가져오기
    existing_ids = set(storage_manager.get_review_ids_with_s3_select(placeId))
    print(f"기존 리뷰 {len(existing_ids)}개를 S3에서 불러옴")

    # 크롤러 생성 및 크롤링
    crawler = NaverMapReviewCrawler(headless=True)
    reviews = crawler.crawl_all_reviews(placeId, existing_ids)

    print(f"\n{'='*60}")
    print(f"총 {len(reviews)}개 신규 리뷰 수집 완료")
    print(f"{'='*60}")

    for i, review in enumerate(reviews, 1):
        print(f"\n[{i}] {review['author']} ({review['visit_date']})")
        print(f"id: {review['id']}")
        print(f"content: {review['content']}")

    # S3에 업로드
    if reviews:
        storage_manager.upload_reviews_json(placeId, reviews)
        print(f"{len(reviews)}개 리뷰를 S3에 업로드 완료")
    else:
        print("업로드할 신규 리뷰가 없습니다.")
