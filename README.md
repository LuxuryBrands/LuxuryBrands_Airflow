# LuxuryBrands_Airflow

## ❗ 로컬 실행

1. 프로젝트 checkout
2. 루트 경로에서 env.local 파일 생성
3. 루트 경로에서 shell 실행후 가상환경 생성
    ```
    python -m venv venv
    source venv/bin/activate
    ```
4. 패키지 다운로드
    ```
    pip install -r requirements.txt
    ```
5. docker image build
    ```
    docker build --tag de-2-1-airflow:latest --platform linux/amd64 . 
    ```
6. docker compose 실행
   ```
    docker compose -f docker-compose.local.yaml up -d 
   ```