# LuxuryBrands_Airflow
Airflow로 스케줄링하는 ETL/ELT 파이프라인 모듈  [[프로젝트 전체보기]](https://github.com/LuxuryBrands)


## 1. ETL
### 1 - 1. Airflow 스케줄링
![img1](https://github.com/LuxuryBrands/.github/blob/main/profile/files/ETL_airflow.png)
- 매 시 정각 마다 Raw Data를 Stage Data로 처리하는 DAG 
- 동적 DAG 를 구현하여, 수집 데이터 모델 별로 DAG 생성
- Slack 알림 모듈을 구현 및 각 task 의 결과를 Slack 으로 전송
- EMR을 이용하여 데이터 처리 및 품질 체크 후 Stage 데이터 생성 
- 모델별 저장 전략에 따라 Stage 데이터를 Redshift Raw Schema의 테이블로 적재


### 1 - 2. EMR 프로세스
![img2](https://github.com/LuxuryBrands/.github/blob/main/profile/files/ETL_emr_process.png)
- Raw Data와 EMR 클러스터가 수행할 스크립트 정보를 S3에 적재
- pydeequ 라이브러리를 이용해 데이터 품질체크 수행
- 처리 결과 S3 /Stage 버킷에 Avro 타입으로 저장 (Avro : 자동매핑 편의성 제공)
- Sensor를 통해 EMR 작업의 결과 확인

<br/>

## 2. ELT

### 2 - 1. Airflow 스케줄링
![img3](https://github.com/LuxuryBrands/.github/blob/main/profile/files/ELT_airflow.png)
- 매 시 30분 마다 Raw Data를 Mart Data로 처리하는 DAG 
- EMR을 통해 데이터 가공, 품질 검증, 데이터 적재 작업 수행
- Slack 모듈을 활용하여 각 task 의 결과를 Slack 으로 전송


### 2 - 2. EMR 프로세스
![img4](https://github.com/LuxuryBrands/.github/blob/main/profile/files/ELT_emr_process.png)
- 품질체크 쿼리를 통해 Raw 데이터를 검증 후 로깅 및 실패시 Dag 종료
- 마트 테이블 모두 Overwrite 적재 수행
- Cross Filtering 차트를 위한 Fact 테이블에 데이터 적재
- 데이터 집계 연산 후 집계(Agg) 테이블에 적재
- 분석 지표에 맞춰 데이터 가공 후 Fact 테이블에 각각 적재

<br/>

## 3. Airflow Deploy
![img5](https://github.com/LuxuryBrands/.github/blob/main/profile/files/Airflow_deploy.png)
- Github Action을 이용하여 Docker 이미지를 생성해 ECR에 저장
- Airflow 빌드 정보가 담긴 task-definition의 이미지 버전정보 업데이트
- ECS를 통해 Airflow 서비스 배포 (롤링 업데이트)
- 컨테이너별 Health Check 후 정상이 아닐 경우 재배포
- 배포 결과 Slack으로 알람


<br/>

## 4. 폴더구조
```
.
├── config
├── dags
├── logs
├── plugins
├── tests
├── Dockerfile
├── docker-compose.local.yaml
├── entrypoint.sh
├── requirements.txt
└── README.md
```

| Name | Explanation |
|:---:|:---|
| `config` | Airflow 및 ECS task 설정 |
| `dags` | Airflow Dag 정의 폴더 |
| `logs` | Airflow 로그 폴더 |
| `plugins` | Spark 패키지, 플러그인 |
| `tests` | 테스트 스크립트 |
