import asyncio
from playwright.async_api import async_playwright
from typing import List, Dict, Optional, Tuple
from geopy.geocoders import Nominatim
from geopy.location import Location
import re
import json
from storage_manager import RestaurantStorageManager
import os
import logging
import traceback
from functools import wraps

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 타임아웃 상수 (ms)
TIMEOUT = 10000


class CriticalError(Exception):
    """심각한 에러 - 프로그램 중단 필요"""

    pass


def handle_errors(critical=False):
    """전역 에러 처리 데코레이터"""

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"{func.__name__} 함수에서 에러 발생: {str(e)}")
                logger.error(f"상세 에러: {traceback.format_exc()}")

                if critical:
                    raise CriticalError(f"심각한 에러 발생: {str(e)}")
                return None if not func.__name__.startswith("get_") else []

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"{func.__name__} 함수에서 에러 발생: {str(e)}")
                logger.error(f"상세 에러: {traceback.format_exc()}")

                if critical:
                    raise CriticalError(f"심각한 에러 발생: {str(e)}")
                return None

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


class NaverMapRestaurantCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.geolocator = Nominatim(user_agent="myGeocoder")

    @handle_errors()
    def clean_address(self, address: str) -> str:
        """도로명 주소에서 상세 주소 제거"""
        if not address:
            return ""

        # 주소 정제를 위한 정규표현식
        regex = (
            r"(\w+[원,산,남,울,북,천,주,기,시,도]\s*)?"
            r"(\w+[구,시,군]\s*)?(\w+[구,시]\s*)?"
            r"(\w+[면,읍]\s*)"
            r"?(\w+\d*\w*[동,리,로,길]\s*)"
            r"?(\w*\d+-?\d*)?"
        )

        match = re.search(regex, address)
        if match:
            return match.group().strip()
        return address

    @handle_errors()
    def get_coordinates(self, address: str) -> Optional[Tuple[float, float]]:
        """주소로부터 위도, 경도 추출"""
        if not address:
            return None

        # 주소 정제
        cleaned_address = self.clean_address(address)

        # 지오코딩 (geopy의 geocode는 동기 함수임)
        location = self.geolocator.geocode(cleaned_address)
        # location이 None이 아니고, geopy.location.Location 타입이어야 함
        if isinstance(location, Location):
            return (location.latitude, location.longitude)
        else:
            return None

    @handle_errors()
    async def _scroll_until_all_loaded(self, frame):
        """모든 데이터 로드될 때까지 스크롤"""
        previous_count = 0
        no_change_count = 0
        max_no_change = 3  # 3번 연속으로 변화가 없으면 종료

        while True:
            # 현재 로드된 식당 수 확인
            current_restaurants = await frame.query_selector_all("li.UEzoS")
            current_count = len(current_restaurants)

            # 변화가 없으면 카운트 증가
            if current_count == previous_count:
                no_change_count += 1

                if no_change_count >= max_no_change:
                    logger.info("더 이상 로드할 데이터가 없습니다.")
                    break
            else:
                no_change_count = 0  # 변화가 있으면 카운트 리셋

            previous_count = current_count

            # 스크롤 실행
            await frame.evaluate(
                """
                () => {
                    const scrollContainer = document.querySelector('.Ryr1F') || 
                                           document.querySelector('[role="main"]') || 
                                           document.body;
                    
                    if (scrollContainer) {
                        scrollContainer.scrollTop = scrollContainer.scrollHeight;
                    } else {
                        window.scrollTo(0, document.body.scrollHeight);
                    }
                }
            """
            )

            # 새로운 데이터 로딩 대기
            await asyncio.sleep(2)

    @handle_errors()
    async def _navigate_to_page(self, frame, page_num: int):
        """특정 페이지로 이동"""
        if page_num > 1:
            page_link = await frame.query_selector(f"a.mBN2s:has-text('{page_num}')")
            if not page_link:
                raise Exception("해당 페이지 없음")
            await page_link.click()
            await asyncio.sleep(3)
            await frame.wait_for_selector("li.UEzoS", state="visible", timeout=TIMEOUT)

    @handle_errors()
    async def _extract_placeId(self, restaurant, page):
        """placeId 추출"""
        placeId = None
        link_elem = await restaurant.query_selector("a.place_bluelink")

        if link_elem:
            # 클릭
            await link_elem.click()

            # URL 변경 대기 (최대 3초)
            await page.wait_for_url(lambda url: "/place/" in url, timeout=TIMEOUT)

            # 변경된 URL에서 place ID 추출
            new_url = page.url
            match = re.search(r"/place/(\d+)", new_url)
            if match:
                placeId = match.group(1)

        return placeId

    @handle_errors()
    async def _extract_restaurant_details(self, context, placeId):
        """식당 상세 정보 추출"""
        address = None
        cleaned_address = None
        latitude = None
        longitude = None

        place_detail_url = f"https://pcmap.place.naver.com/place/{placeId}"
        detail_page = await context.new_page()

        try:
            await detail_page.goto(place_detail_url)
            await detail_page.wait_for_selector("span.LDgIH", timeout=TIMEOUT)
            address_elem = await detail_page.query_selector("span.LDgIH")
            address = await address_elem.inner_text()

            # 주소 정제 및 지오코딩
            if address:
                cleaned_address = self.clean_address(address)
                coordinates = self.get_coordinates(cleaned_address)
                if coordinates:
                    latitude, longitude = coordinates

        finally:
            await detail_page.close()

        return address, cleaned_address, latitude, longitude

    @handle_errors()
    async def _extract_single_restaurant(self, restaurant, page, context, page_num):
        """단일 식당 정보 추출"""
        # 식당 이름 정보
        name_elem = await restaurant.query_selector("span.TYaxT")
        name = await name_elem.inner_text() if name_elem else "이름 없음"

        # 식당 카테고리 정보
        category_elem = await restaurant.query_selector("span.KCMnt")
        category = await category_elem.inner_text() if category_elem else ""

        # 식당 placeId 정보
        placeId = await self._extract_placeId(restaurant, page)

        # 주소 찾기
        address, cleaned_address, latitude, longitude = (
            await self._extract_restaurant_details(context, placeId)
        )

        return {
            "placeId": placeId,
            "name": name,
            "category": category,
            "page": page_num,
            "origin_address": address,
            "address": cleaned_address,
            "latitude": latitude,
            "longitude": longitude,
        }

    @handle_errors(critical=True)
    async def crawl_single_page(self, search_query: str, page_num: int) -> List[Dict]:
        """특정 페이지 하나만 크롤링"""
        async with async_playwright() as p:
            # 프록시 설정 추가
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

            browser = await p.chromium.launch(**launch_options)

            context = await browser.new_context(
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
            page = await context.new_page()

            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,webp}", lambda route: route.abort()
            )

            results = []

            try:
                await page.goto("https://httpbin.org/ip")
                await page.goto("https://map.naver.com/", wait_until="domcontentloaded")

                search_input = await page.wait_for_selector(
                    "input.input_search", state="visible", timeout=TIMEOUT
                )
                await search_input.click()
                await search_input.fill(search_query)
                await search_input.press("Enter")

                await page.wait_for_selector(
                    "iframe#searchIframe", state="visible", timeout=TIMEOUT
                )
                iframe_element = await page.query_selector("iframe#searchIframe")

                frame = await iframe_element.content_frame()

                if frame:
                    await self._scroll_until_all_loaded(frame)

                if not frame:
                    return results

                await frame.wait_for_selector(
                    "li.UEzoS", state="visible", timeout=TIMEOUT
                )

                # 페이지 이동
                await self._navigate_to_page(frame, page_num)

                # 데이터 추출
                restaurants = await frame.query_selector_all("li.UEzoS")

                for restaurant in restaurants:
                    try:
                        restaurant_data = await self._extract_single_restaurant(
                            restaurant, page, context, page_num
                        )
                        if restaurant_data:
                            results.append(restaurant_data)
                        await page.go_back()
                    except Exception as e:
                        logger.warning(f"식당 데이터 추출 실패: {str(e)}")
                        continue

                logger.info(f"페이지 {page_num}: {len(restaurants)}개 수집")

            finally:
                await browser.close()

            return results


# 사용 예시
@handle_errors(critical=True)
async def main():
    # S3 설정 (환경변수 사용)
    BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    REGION_NAME = os.environ.get("AWS_REGION", "ap-northeast-2")

    search_query = "공덕역 식당"
    logger.info(f"search_query: {search_query}")

    # S3 매니저 생성
    s3_manager = RestaurantStorageManager(
        bucket_name=BUCKET_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )

    # 1. S3에서 기존 placeId 리스트 가져오기
    existing_placeIds = set()
    try:
        existing_placeIds = set(
            s3_manager.get_restaurant_ids_with_s3_select(search_query)
        )
    except Exception as e:
        logger.warning(f"S3에서 기존 데이터 조회 실패, 새로운 데이터로 간주: {str(e)}")

    crawler = NaverMapRestaurantCrawler(headless=True)

    # 2. 여러 페이지 동시 실행
    tasks = [
        crawler.crawl_single_page(search_query, 1),
        # crawler.crawl_single_page(search_query, 2),
        # crawler.crawl_single_page(search_query, 3),
        # crawler.crawl_single_page(search_query, 4),
    ]

    # 3. 모든 결과 대기
    all_results = await asyncio.gather(*tasks, return_exceptions=True)

    # 4. 결과 병합
    merged_results = []
    for i, page_results in enumerate(all_results):
        if isinstance(page_results, Exception):
            logger.error(f"페이지 {i+1} 크롤링 실패: {str(page_results)}")
            continue
        if page_results:
            merged_results.extend(page_results)

    # 5. 기존 placeId와 중복 제거
    deduped_results = [
        item for item in merged_results if item["placeId"] not in existing_placeIds
    ]

    logger.info(f"\n총 {len(deduped_results)}개 신규 식당 수집")
    for i, restaurant in enumerate(deduped_results, 1):
        logger.info(
            f"{i}. {restaurant['placeId']} [{restaurant['name']}] "
            f"[{restaurant['category']}] [{restaurant['page']}] "
            f"[origin_address: {restaurant['origin_address']}] "
            f"[address: {restaurant['address']}] "
            f"[latitude: {restaurant['latitude']}, longitude: {restaurant['longitude']}]"
        )

    # 6. S3에 업로드 (신규만)
    # if deduped_results:
    #     try:
    #         s3_manager.upload_restaurants_json(search_query, deduped_results)
    #         logger.info("S3 업로드 완료")
    #     except Exception as e:
    #         logger.error(f"S3 업로드 실패: {str(e)}")
    #         raise CriticalError("데이터 저장 실패")
    # else:
    #     logger.info("신규 식당 없음, 업로드 생략")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except CriticalError as e:
        logger.critical(f"프로그램 종료: {str(e)}")
        exit(1)
    except Exception as e:
        logger.critical(f"예상치 못한 에러로 프로그램 종료: {str(e)}")
        exit(1)
