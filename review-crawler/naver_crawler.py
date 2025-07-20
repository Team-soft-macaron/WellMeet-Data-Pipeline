from playwright.sync_api import sync_playwright
from typing import List, Dict, Set
import time
import hashlib
import psutil
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class NaverMapReviewCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless

    def setup_api_bypass(self, page):
        """네이버 리뷰 조회수 추적 API 무력화"""

        # 1. fetch API 가로채기 및 무력화
        page.add_init_script(
            """
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
        )

        # 2. 추가 보안 우회 스크립트
        page.add_init_script(
            """
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
        )

    def crawl_all_reviews(self, placeId: str, existing_ids: Set[str]) -> List[Dict]:
        """네이버 지도의 모든 리뷰 크롤링
        existing_ids: 이미 존재하는 리뷰 id의 set. 발견 시 즉시 중단 (필수)."""
        with sync_playwright() as p:
            launch_options = {
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
            browser = p.chromium.launch(**launch_options)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                locale="ko-KR",
                timezone_id="Asia/Seoul",
                permissions=["geolocation"],
                geolocation={"latitude": 37.5665, "longitude": 126.9780},  # 서울
                color_scheme="light",
                device_scale_factor=1,
                is_mobile=False,
                has_touch=False,
                extra_http_headers={
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
            )
            page = context.new_page()
            self.setup_api_bypass(page)

            reviews = []
            already_appended_ids = set()

            try:
                # 리뷰 페이지로 이동
                page.goto("https://httpbin.org/headers")
                page.wait_for_timeout(2000)
                url = f"https://pcmap.place.naver.com/restaurant/{placeId}/review/visitor"
                page.goto(url)
                page.wait_for_selector("ul#_review_list", timeout=10000)
                sort_buttons = page.query_selector_all("a.ScBz5")
                for btn in sort_buttons:
                    btn_text = btn.inner_text()
                    if "최신순" in btn_text:
                        btn.click()
                        time.sleep(2)  # 정렬 완료 대기
                        logger.info("최신순으로 정렬됨")
                        break
                no_new_reviews_count = 0

                stop_crawling = False
                process = psutil.Process(os.getpid())
                while not stop_crawling:
                    # 현재 페이지의 리뷰 수집
                    review_elements = page.query_selector_all(
                        "ul#_review_list > li.EjjAW"
                    )
                    # 메모리 및 CPU 사용량 체크
                    mem_info = process.memory_info()
                    cpu_percent = psutil.cpu_percent(interval=0.1)
                    logger.info(
                        f"[Resource] RSS: {mem_info.rss / 1024 / 1024:.2f} MB, CPU: {cpu_percent:.1f}%"
                    )
                    current_count = len(reviews)
                    # 1000개 이상 리뷰가 있으면 중단
                    if current_count > 1000:
                        break

                    for elem in review_elements:
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

                        # 이미 존재하는 id라면 즉시 중단
                        if review_id in existing_ids:
                            logger.info(
                                f"이미 존재하는 리뷰(id={review_id}) 발견, 크롤링 중단"
                            )
                            stop_crawling = True
                            break

                        # 중복 체크
                        if review_id not in already_appended_ids:
                            reviews.append(
                                {
                                    "id": review_id,
                                    "placeId": placeId,
                                    "author": author_name,
                                    "content": review_text,
                                    "visit_date": visit_date,
                                }
                            )
                            already_appended_ids.add(review_id)
                            page.evaluate(
                                "(element) => element.remove()",
                                elem,
                            )

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
                logger.error(f"크롤링 오류: {str(e)}")
            finally:
                browser.close()

            return reviews
