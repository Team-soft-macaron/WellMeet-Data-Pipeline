# WellMeet-Review-Crawling
네이버 map에서 식당마다 리뷰 크롤링하는 repository

# 사용법
```bash
docker build -t review .
docker run -e PLACE_ID=<네이버 place_id> review
```
