from playwright.sync_api import sync_playwright
from typing import List, Dict
import time
import os
import hashlib


class NaverMapReviewCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless

    def crawl_all_reviews(self, place_id: str) -> List[Dict]:
        """네이버 지도의 모든 리뷰 크롤링"""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()

            reviews = []

            try:
                # 리뷰 페이지로 이동
                url = f"https://pcmap.place.naver.com/restaurant/{place_id}/review/visitor"
                page.goto(url)
                page.wait_for_selector("ul#_review_list", timeout=10000)
                sort_buttons = page.query_selector_all("a.ScBz5")
                for btn in sort_buttons:
                    btn_text = btn.inner_text()
                    if "최신순" in btn_text:
                        btn.click()
                        time.sleep(2)  # 정렬 완료 대기
                        print("최신순으로 정렬됨")
                        break
                no_new_reviews_count = 0

                while True:
                    # 현재 페이지의 리뷰 수집
                    review_elements = page.query_selector_all(
                        "ul#_review_list > li.EjjAW"
                    )
                    current_count = len(reviews)

                    for elem in review_elements:
                        try:
                            # 작성자
                            author = elem.query_selector("span.pui__NMi-Dp")
                            author_name = author.inner_text() if author else "익명"

                            # 리뷰 내용 더보기 클릭
                            more_btn = elem.query_selector(
                                "a.pui__wFzIYl[data-pui-click-code='rvshowmore']"
                            )
                            if more_btn and more_btn.is_visible():
                                more_btn.click()
                                time.sleep(0.5)

                            # 리뷰 내용
                            content = elem.query_selector("div.pui__vn15t2 > a")
                            review_text = content.inner_text() if content else ""

                            # 방문날짜
                            date = elem.query_selector("time")
                            visit_date = date.inner_text() if date else ""

                            # Use SHA-256 hash for unique review ID
                            hash_input = f"{author_name}|{review_text}|{visit_date}"
                            review_id = hashlib.sha256(
                                hash_input.encode("utf-8")
                            ).hexdigest()

                            # 중복 체크
                            if not any(r.get("id") == review_id for r in reviews):
                                reviews.append(
                                    {
                                        "id": review_id,
                                        "place_id": place_id,
                                        "author": author_name,
                                        "content": review_text,
                                        "visit_date": visit_date,
                                    }
                                )

                        except Exception as e:
                            continue

                    print(f"현재까지 {len(reviews)}개 리뷰 수집")

                    # 새로운 리뷰가 없으면 카운트
                    if len(reviews) == current_count:
                        no_new_reviews_count += 1
                        if no_new_reviews_count >= 3:
                            print("더 이상 새로운 리뷰가 없습니다.")
                            break
                    else:
                        no_new_reviews_count = 0

                    # 더보기 버튼 찾기 및 클릭
                    more_button = page.query_selector("div.NSTUp a.fvwqf")
                    if more_button and more_button.is_visible():
                        more_button.scroll_into_view_if_needed()
                        more_button.click()
                        time.sleep(2)  # 새 리뷰 로딩 대기
                    else:
                        # 더보기 버튼이 없으면 스크롤
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(2)

            except Exception as e:
                print(f"크롤링 오류: {str(e)}")
            finally:
                browser.close()

            return reviews


# 사용 예시
def main():
    crawler = NaverMapReviewCrawler(headless=True)

    # 환경변수에서 place_id 읽기
    place_id = os.environ.get("PLACE_ID")
    if not place_id:
        print("환경변수 PLACE_ID가 설정되어 있지 않습니다.")
        return

    reviews = crawler.crawl_all_reviews(place_id)

    print(f"\n{'='*60}")
    print(f"총 {len(reviews)}개 리뷰 수집 완료")
    print(f"{'='*60}")

    for i, review in enumerate(reviews, 1):
        print(f"\n[{i}] {review['author']} ({review['visit_date']})")
        print(f"ID: {review['id']}")
        print(f"Content: {review['content']}")


if __name__ == "__main__":
    main()
