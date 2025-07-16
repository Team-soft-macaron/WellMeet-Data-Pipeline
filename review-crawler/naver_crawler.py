from playwright.sync_api import sync_playwright
from typing import List, Dict, Set
import time
import hashlib
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class NaverMapReviewCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless

    def _get_fetch_bypass_script(self) -> str:
        """fetch API 가로채기 스크립트 반환"""
        return """
            // fetch 함수 오버라이드
            const originalFetch = window.fetch;
            window.fetch = function(...args) {
                const [url, options] = args;
                
                // visitorReview/views API 차단
                if (typeof url === 'string' && url.includes('/rest/visitorReview/views')) {
                    console.log('[Bypass] Blocked review view tracking:', url);
                    // 즉시 204 응답 반환
                    return Promise.resolve(new Response(null, {
                        status: 204,
                        statusText: 'No Content',
                        headers: new Headers()
                    }));
                }
                
                // 다른 요청은 정상 처리
                return originalFetch.apply(this, args);
            };
            
            // XMLHttpRequest도 차단 (혹시 모를 경우 대비)
            const originalXHR = window.XMLHttpRequest;
            window.XMLHttpRequest = function() {
                const xhr = new originalXHR();
                const originalOpen = xhr.open;
                
                xhr.open = function(method, url, ...args) {
                    if (url && url.includes('/rest/visitorReview/views')) {
                        console.log('[Bypass] Blocked XHR review tracking:', url);
                        // 가짜 응답 설정
                        xhr.send = function() {
                            Object.defineProperty(xhr, 'status', { value: 204 });
                            Object.defineProperty(xhr, 'readyState', { value: 4 });
                            xhr.onreadystatechange && xhr.onreadystatechange();
                            xhr.onload && xhr.onload();
                        };
                        return;
                    }
                    return originalOpen.apply(this, [method, url, ...args]);
                };
                
                return xhr;
            };
            
            console.log('[Bypass] Review tracking API disabled successfully!');
        """

    def _get_security_bypass_script(self) -> str:
        """보안 우회 스크립트 반환"""
        return """
            // navigator.webdriver 숨기기
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
            
            // Chrome 객체 추가
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // plugins 추가
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // permissions 숨기기
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """

    def setup_api_bypass(self, page):
        """네이버 리뷰 조회수 추적 API 무력화"""
        page.add_init_script(self._get_fetch_bypass_script())
        page.add_init_script(self._get_security_bypass_script())

    def _get_launch_options(self) -> dict:
        """브라우저 실행 옵션 반환"""
        return {
            "headless": self.headless,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-web-security",
                "--disable-site-isolation-trials",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-default-apps",
                "--disable-sync",
                "--disable-translate",
                "--hide-scrollbars",
                "--metrics-recording-only",
                "--mute-audio",
                "--safebrowsing-disable-auto-update",
                "--ignore-certificate-errors",
                "--ignore-ssl-errors",
                "--ignore-certificate-errors-spki-list",
                "--disable-setuid-sandbox",
                "--window-size=1920,1080",
                "--start-maximized",
            ],
        }

    def _get_context_options(self) -> dict:
        """브라우저 컨텍스트 옵션 반환"""
        return {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "locale": "ko-KR",
            "timezone_id": "Asia/Seoul",
            "permissions": ["geolocation"],
            "geolocation": {"latitude": 37.5665, "longitude": 126.9780},  # 서울
            "color_scheme": "light",
            "device_scale_factor": 1,
            "is_mobile": False,
            "has_touch": False,
            "extra_http_headers": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "max-age=0",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Sec-Fetch-Dest": "document",
                "Upgrade-Insecure-Requests": "1",
            },
        }

    def _sort_by_latest(self, page):
        """리뷰를 최신순으로 정렬"""
        sort_buttons = page.query_selector_all("a.place_btn_option")
        for btn in sort_buttons:
            btn_text = btn.inner_text()
            if "최신순" in btn_text:
                btn.click()
                time.sleep(2)  # 정렬 완료 대기
                logger.info("최신순으로 정렬됨")
                break

    def _generate_review_id(
        self, author_name: str, review_text: str, visit_date: str
    ) -> str:
        """리뷰 고유 ID 생성"""
        hash_input = f"{author_name}|{review_text}|{visit_date}"
        return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

    def _extract_review_data(self, elem, place_id: str) -> dict:
        """단일 리뷰 요소에서 데이터 추출"""
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

        # 고유 ID 생성
        review_id = self._generate_review_id(author_name, review_text, visit_date)

        return {
            "id": review_id,
            "place_id": place_id,
            "author": author_name,
            "content": review_text,
            "visit_date": visit_date,
        }

    def _load_more_reviews(self, page):
        """더 많은 리뷰 로드"""
        more_button = page.query_selector("div.NSTUp a.fvwqf")
        if more_button and more_button.is_visible():
            more_button.scroll_into_view_if_needed()
            more_button.click()
            time.sleep(2)  # 새 리뷰 로딩 대기
            return True
        else:
            # 더보기 버튼이 없으면 스크롤
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            return False

    def _process_reviews_on_page(
        self,
        page,
        place_id: str,
        existing_ids: Set[str],
        already_appended_ids: Set[str],
    ) -> tuple:
        """현재 페이지의 리뷰 처리"""
        reviews = []
        stop_crawling = False

        review_elements = page.query_selector_all("ul#_review_list > li.EjjAW")

        for elem in review_elements:
            review_data = self._extract_review_data(elem, place_id)
            review_id = review_data["id"]

            # 이미 존재하는 id라면 즉시 중단
            if review_id in existing_ids:
                logger.info(f"이미 존재하는 리뷰(id={review_id}) 발견, 크롤링 중단")
                stop_crawling = True
                break

            # 중복 체크
            if review_id not in already_appended_ids:
                reviews.append(review_data)
                already_appended_ids.add(review_id)
                page.evaluate("(element) => element.remove()", elem)

        return reviews, stop_crawling

    def crawl_all_reviews(self, place_id: str, existing_ids: Set[str]) -> List[Dict]:
        """네이버 지도의 모든 리뷰 크롤링
        existing_ids: 이미 존재하는 리뷰 id의 set. 발견 시 즉시 중단 (필수)."""
        with sync_playwright() as p:
            browser = p.chromium.launch(**self._get_launch_options())
            context = browser.new_context(**self._get_context_options())
            page = context.new_page()
            self.setup_api_bypass(page)

            reviews = []
            already_appended_ids = set()

            # 리뷰 페이지로 이동
            page.goto("https://httpbin.org/headers")
            page.wait_for_timeout(2000)
            url = f"https://pcmap.place.naver.com/restaurant/{place_id}/review/visitor"
            page.goto(url)
            page.wait_for_selector("ul#_review_list", timeout=10000)

            # 최신순 정렬
            self._sort_by_latest(page)

            no_new_reviews_count = 0
            stop_crawling = False

            while not stop_crawling:
                current_count = len(reviews)

                # 현재 페이지의 리뷰 수집
                page_reviews, stop_crawling = self._process_reviews_on_page(
                    page, place_id, existing_ids, already_appended_ids
                )
                reviews.extend(page_reviews)

                logger.info(f"현재까지 {len(reviews)}개 리뷰 수집")

                if stop_crawling:
                    break

                # 새로운 리뷰가 없으면 카운트
                if len(reviews) == current_count:
                    no_new_reviews_count += 1
                    if no_new_reviews_count >= 3:
                        logger.info("더 이상 새로운 리뷰가 없습니다.")
                        break
                else:
                    no_new_reviews_count = 0

                # 더 많은 리뷰 로드
                self._load_more_reviews(page)

            browser.close()
            return reviews


# 사용 예시
def main():
    try:
        crawler = NaverMapReviewCrawler(headless=False)

        # 예시: 특정 장소의 리뷰 크롤링
        place_id = "1234567890"  # 실제 place_id로 변경
        existing_review_ids = set()  # 기존 리뷰 ID들

        reviews = crawler.crawl_all_reviews(place_id, existing_review_ids)

        print(f"\n총 {len(reviews)}개의 리뷰를 수집했습니다.")
        for i, review in enumerate(reviews[:5], 1):  # 처음 5개만 출력
            print(f"\n{i}. 작성자: {review['author']}")
            print(f"   내용: {review['content'][:50]}...")
            print(f"   방문일: {review['visit_date']}")

    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {str(e)}")
        raise


if __name__ == "__main__":
    main()
