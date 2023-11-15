# pyupbit

### ✅ 프로젝트 기간
 2023-08-02 ~ 2023-08-28

### ✅ 한 줄 소개
Pyupbit 실시간 데이터를 Kafka를 통해 수집한 후, MySQL DB에 데이터를 적재하는 Airflow 파이프라인 구축 진행

### ✅ 사용 기술

#### ETL 파이프라인
  - 배치도구 : <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white">
  - 데이터 스토리지 : <img src="https://img.shields.io/badge/Amazon S3-569A31?style=flat-square&logo=amazons3&logoColor=white">
  - 실시간 데이터 스트리밍 : <img src="https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=Apache Kafka&logoColor=white">

#### WEB
  - 컴퓨팅 서비스 : <img src="https://img.shields.io/badge/AWS EC2-FF9900?style=flat-square&logo=amazonec2&logoColor=white">
  - 데이터 베이스 : <img src="https://img.shields.io/badge/MySQL-4479A1?style=flat&logo=MySQL&logoColor=white">
  
<br>

## 데이터 파이프라인

### 워크 플로우
<img src="https://github.com/jenny5587/airflow/assets/103649749/7309e94a-b431-4626-820e-633e0a61206b" width="600" height="300">
<img src="https://github.com/jenny5587/airflow/assets/103649749/4ccaafd1-5086-4501-8bbe-518beccf2296" width="600" height="350"> 

### 수집 데이터
- Python Wrapper for Upbit API
