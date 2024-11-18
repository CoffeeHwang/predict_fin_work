# 1. 개요 

## 코드의 목적
 - 랜덤포레스트 모델을 활용한 특정 LOT의 예측 종료시간을 반환 

## 워크트리 
 - main.py : get_predict_endtime 함수코드 포함
 - predict_module.py
 - yhs_common.py : 공통함수
 - yhs_mysql.py : mysql 연결 및 쿼리 기본실행
 - requirements.txt : 필요 패키지 목록

## 작동방식
   1. Google Cloud Functions에 배포된 get_predict_endtime 함수를 HTTP 트리거를 이용한 호출을 한다.
   2. 예측 종료시간을 반환한다. 
--- 

# 2. 개발가이드

## python 개발환경 구축 (for Mac) 참고
> https://yhsbearing.atlassian.net/wiki/spaces/RNHM/pages/964689921/google+cloud+-+python+for+Mac

## 테스트 방법
### Google Cloud Functions 콘솔에서 테스트 
테스트하는 방법은 여러가지가 있으나, 콘솔을 통해 테스트하는 방법을 소개한다.
1. 콘솔에서 Cloud Functions 메뉴로 이동한다.
2. 배포하고자 하는 함수 이름을 클릭한다. (get_predict_endtime)
3. "테스트" 혹은 "테스트 중" 탭을 클릭한다.
4. 좌측 editable area에 테스트할 json 형식 데이터를 입력후 "함수테스트" 클릭   
   ex) {"lot":12345}
5. 출력에 년월일시분초 형식의 예측 종료시간이 출력되는 것을 확인한다.

## 배포 방법
### Google Cloud Functions 배포 
배포하는 방법은 여러가지가 있으나, 콘솔을 통해 배포하는 방법을 소개한다. (추후 gcloud cli 를 이용하는 배포 방법을 통해 git-hook 연동 등 자동화를 고려해 볼 수 있다.  )
1. 콘솔에서 Cloud Functions 메뉴로 이동한다.
2. 배포하고자 하는 함수 이름을 클릭한다. (get_predict_endtime)
3. 상단의 수정을 클릭한다.
4. 하단의 다음을 클릭한다.
5. 변경할 코드를 붙여넣는다.
6. 하단 배포를 클릭한다.

## 기타 사항 
 - 환경변수 (.env) 파일은 배포된 Google Cloud Functions 소스탭에서 확인 가능
