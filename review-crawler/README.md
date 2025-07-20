# WellMeet-Review-Crawling
네이버 map에서 식당마다 리뷰 크롤링하는 repository

# 사용법
이 레포지토리는 AWS Lambda에서 실행할 docker image 기준으로 작성되었습니다.

로컬에서 테스트하기 위해 다음과 같은 과정을 거쳐야 합니다. 

### 환경 변수 세팅
root에 .env 파일을 생성하여 다음 환경 변수를 입력합니다.

```
S3_BUCKET_NAME=<리뷰 데이터 저장할 AWS S3 버킷 이름>
AWS_ACCESS_KEY_ID=<aws access key>
AWS_SECRET_ACCESS_KEY=<aws secret key>
AWS_REGION=<aws region>
placeId=<네이버 지도 식당 place id>
```

### arm64 런타임 인터페이스 에뮬레이터 설치 (macOS 기준)

macOS가 아닌 운영체제에서는 공식 문서를 참조하시기 바랍니다.
https://docs.aws.amazon.com/ko_kr/lambda/latest/dg/python-image.html#python-image-instructions

```
mkdir -p ~/.aws-lambda-rie && \
    curl -Lo ~/.aws-lambda-rie/aws-lambda-rie https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie-arm64 && \
    chmod +x ~/.aws-lambda-rie/aws-lambda-rie
```

### docker image 빌드

```bash
docker buildx build --env-file .env --platform linux/amd64 --provenance=false -t docker-image:test .
```

linux/amd64 플랫폼에서 docker image를 빌드해야 합니다.

### docker container 실행

```bash
docker run --platform linux/amd64 -p 9000:8080 docker-image:test
```

contaier를 실행합니다.

### 이벤트 게시

터미널을 하나 더 열고 container가 실행 중인 상태로 아래 명령어를 입력합니다.

```bash
curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

