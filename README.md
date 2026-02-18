\# 파일 설명

main.py : VM 에 redis 로 중복 체크 => 크롤링 => BigQuery 적재 => vertex AI => JSON 을 GCS 저장 => PDF 를 GCS 저장

test\_pdf\_gen.py : pdf 생성



\# 크롤링 동작

1\.리뷰텍스트가 5자 이하면 스킵

2\.사진만 올린 리뷰 페스 

