import os
import logging
from storage_manager import ReviewStorageManager
from naver_crawler import NaverMapReviewCrawler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# 사용 예시
def main():
    # 환경변수에서 S3 정보 읽기
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    region_name = os.environ.get("AWS_REGION")

    # 환경변수에서 place_id 읽기
    place_id = os.environ.get("PLACE_ID")
    if not place_id:
        logger.error("환경변수 PLACE_ID가 설정되어 있지 않습니다.")
        return
    if not bucket_name:
        logger.error("환경변수 S3_BUCKET_NAME이 설정되어 있지 않습니다.")
        return

    # S3 매니저 생성
    storage_manager = ReviewStorageManager(
        bucket_name=bucket_name,
        region_name=region_name,
    )

    # 기존 리뷰 id set 가져오기
    existing_ids = set(storage_manager.get_review_ids_with_s3_select(place_id))
    logger.info(f"기존 리뷰 {len(existing_ids)}개를 S3에서 불러옴")
    existing_ids = set()

    # 크롤러 생성 및 크롤링
    crawler = NaverMapReviewCrawler(headless=False)
    reviews = crawler.crawl_all_reviews(place_id, existing_ids)

    logger.info(f"\n{'='*60}")
    logger.info(f"총 {len(reviews)}개 신규 리뷰 수집 완료")
    logger.info(f"{'='*60}")

    for i, review in enumerate(reviews, 1):
        logger.info(f"\n[{i}] {review['author']} ({review['visit_date']})")
        logger.info(f"id: {review['id']}")
        logger.info(f"content: {review['content']}")

    # S3에 업로드
    if reviews:
        storage_manager.upload_reviews_json(place_id, reviews)
        logger.info(f"{len(reviews)}개 리뷰를 S3에 업로드 완료")
    else:
        logger.info("업로드할 신규 리뷰가 없습니다.")


if __name__ == "__main__":
    main()
