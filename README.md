# 네이버 뉴스 검색 API - 분기별 분할 수집

네이버 뉴스 검색 API를 사용하여 특정 키워드의 뉴스를 2022년부터 분기별로 수집하는 프로젝트입니다.

## 🎯 주요 기능

- **분기별 수집**: 2022년부터 현재까지 분기(Q1~Q4)별로 뉴스 수집
- **분할 작업**: 각 키워드×분기 조합을 독립적으로 실행 가능
- **병렬 처리**: 여러 워커가 동시에 작업할 수 있도록 스크립트 제공
- **자동 병합**: 수집된 모든 데이터를 하나의 CSV로 자동 병합
- **정확한 필터링**: 키워드가 정확히 포함된 뉴스만 수집
- **YAML 설정**: 모든 설정을 config.yaml에서 관리
- **.env 지원**: API 키를 환경 변수 파일로 안전하게 관리

## 📁 프로젝트 구조

```
naver_NsBg/
├── README.md                    # 프로젝트 설명
├── requirements.txt             # 필요한 패키지
├── .gitignore                  # Git 제외 파일
├── .env.example                # 환경 변수 템플릿
├── .env                        # 환경 변수 (gitignore, 직접 생성)
├── config.yaml                 # 설정 파일
├── collect_news.py             # 메인 수집 스크립트
├── generate_tasks.py           # 작업 목록 생성 스크립트
├── output_parts/               # 분할 수집 결과 (gitignore)
├── output/                     # 최종 병합 결과 (gitignore)
└── logs/                       # 로그 파일 (gitignore)
```

## 🚀 시작하기

### 1. 설치

```bash
# 저장소 클론
git clone https://github.com/Sgorilla3v3/naver_NsBg.git
cd naver_NsBg

# 필요한 패키지 설치
pip install -r requirements.txt
```

### 2. 환경 변수 설정

네이버 개발자 센터에서 발급받은 API 키를 설정합니다:

#### 방법 1: .env 파일 생성 (권장)

```bash
# .env.example을 복사하여 .env 파일 생성
cp .env.example .env

# .env 파일을 편집하여 실제 API 키 입력
# NAVER_CLIENT_ID=your_actual_client_id
# NAVER_CLIENT_SECRET=your_actual_client_secret
```

#### 방법 2: 환경 변수 직접 설정

```bash
# Linux/Mac
export NAVER_CLIENT_ID="your_client_id"
export NAVER_CLIENT_SECRET="your_client_secret"

# Windows (PowerShell)
$env:NAVER_CLIENT_ID="your_client_id"
$env:NAVER_CLIENT_SECRET="your_client_secret"
```

### 3. 설정 파일 수정

`config.yaml` 파일을 수정하여 검색 키워드 및 기타 옵션을 설정합니다:

```yaml
# 검색 키워드
keywords:
  - 청도혁신센터
  - 경북시민재단
  - 로컬임팩트랩

# 수집 설정
collection:
  start_year: 2022
  display_per_page: 100
  max_items_per_query: 1000
  
# 출력 설정
output:
  parts_dir: output_parts
  merged_dir: output
  merged_filename: news_merged.csv
```

## 📖 사용 방법

### 방법 1: 자동 실행 (가장 간단)

```bash
python collect_news.py
```

- 모든 키워드, 모든 분기 자동 수집
- 수집 완료 후 자동 병합
- 결과: `output/news_merged.csv`

### 방법 2: 단계별 실행

#### 2-1. 작업 목록 생성

```bash
python generate_tasks.py
```

생성되는 파일:
- `run_all_tasks.sh`: 순차 실행 스크립트 (Bash)
- `run_batch.py`: 순차 실행 스크립트 (Python)
- `tasks.json`: 작업 목록 (JSON)
- `run_worker_*.sh`: 병렬 실행 워커 스크립트
- `run_all_workers.sh`: 모든 워커 실행 스크립트

#### 2-2. 수집 실행

**순차 실행:**
```bash
# Bash
bash run_all_tasks.sh

# Python
python run_batch.py
```

**병렬 실행 (4개 워커):**
```bash
bash run_all_workers.sh
```

#### 2-3. 결과 병합

```bash
python collect_news.py --mode merge
```

### 방법 3: 단일 작업 실행 (개별 제어)

특정 키워드와 분기만 수집:

```bash
python collect_news.py --mode single \
  --keyword "청도혁신센터" \
  --quarter "2022_Q1" \
  --start-date "2022-01-01" \
  --end-date "2022-03-31"
```

## 🔧 고급 사용법

### 커스텀 설정 파일 사용

```bash
python collect_news.py --config my_config.yaml
```

### 출력 디렉토리 변경

```bash
python collect_news.py --mode all \
  --output-dir my_output_parts

python collect_news.py --mode merge \
  --output-dir my_output_parts
```

### 특정 연도부터 수집

```bash
python collect_news.py --mode all --start-year 2023
```

## ⚙️ 설정 파일 (config.yaml)

### 주요 설정 항목

| 섹션 | 설정 | 설명 | 기본값 |
|------|------|------|--------|
| **keywords** | - | 검색 키워드 리스트 | - |
| **collection** | start_year | 수집 시작 연도 | 2022 |
| | display_per_page | 페이지당 뉴스 개수 | 100 |
| | max_items_per_query | 쿼리당 최대 수집 개수 | 1000 |
| | api_call_delay | API 호출 간격 (초) | 0.1 |
| **output** | parts_dir | 분할 파일 저장 디렉토리 | output_parts |
| | merged_dir | 병합 파일 저장 디렉토리 | output |
| | merged_filename | 병합 파일명 | news_merged.csv |
| | encoding | 파일 인코딩 | utf-8-sig |
| **logging** | dir | 로그 디렉토리 | logs |
| | level | 로그 레벨 | INFO |
| **filtering** | exact_phrase_match | 정확한 문구 매칭 | true |
| | remove_duplicates | 중복 제거 | true |
| **api** | sort | 정렬 방식 (date/sim) | date |
| | retry_count | 실패 시 재시도 횟수 | 3 |

## 📊 출력 형식

### 분할 파일 (output_parts/)

각 파일명: `{키워드}_{분기}.csv`

예시:
- `청도혁신센터_2022_Q1.csv`
- `경북시민재단_2023_Q2.csv`

### 병합 파일 (output/)

파일명: `news_merged.csv` (config.yaml에서 변경 가능)

컬럼:
- `title`: 뉴스 제목
- `url`: 네이버 뉴스 링크
- `source_url`: 원문 링크
- `description`: 뉴스 설명
- `date`: 발행일
- `quarter`: 분기 정보 (예: 2022_Q1)
- `keyword`: 검색 키워드

## 📝 로그

수집 진행 상황은 `logs/` 디렉토리에 자동 저장됩니다:

```
logs/
└── collection_20251024_153045.log
```

로그 레벨은 config.yaml에서 설정 가능:
- DEBUG: 디버깅 정보 포함
- INFO: 일반 정보 (기본값)
- WARNING: 경고
- ERROR: 오류만

## ⚠️ 주의사항

1. **API 제한**: 네이버 검색 API는 하루 25,000회, 초당 10회 제한이 있습니다
2. **1000건 제한**: 각 검색 쿼리당 최대 1000건만 수집 가능 (API 제한)
3. **분기별 분할**: 1000건 이상의 결과를 수집하기 위해 분기별로 나눠서 수집합니다
4. **.env 파일 보안**: `.env` 파일에 API 키가 포함되므로 절대 커밋하지 마세요 (`.gitignore`에 포함됨)

## 🔍 트러블슈팅

### API 키 오류

```
❌ 오류: 네이버 API 키가 설정되지 않았습니다.
```

→ `.env` 파일이 제대로 생성되었는지, API 키가 올바른지 확인하세요.

### 설정 파일 오류

```
⚠️ 설정 파일을 찾을 수 없습니다: config.yaml
```

→ `config.yaml` 파일이 스크립트와 같은 디렉토리에 있는지 확인하세요.

### 빈 결과

```
❌ 수집된 데이터 없음
```

→ 해당 분기에 검색 결과가 없거나, 정확한 문구 매칭 조건에 맞지 않습니다.

### 중복 파일

같은 작업을 여러 번 실행하면 `output_parts/`에 동일한 파일이 덮어쓰여집니다.

## 📈 성능

- **키워드 5개 × 분기 12개** (2022~2024) = 총 60개 작업
- **순차 실행**: 약 30~60분
- **병렬 실행 (4워커)**: 약 10~20분

## 🛠️ 개발

### 의존성

- Python 3.7+
- requests: HTTP 요청
- pandas: 데이터 처리
- python-dotenv: 환경 변수 로드
- pyyaml: YAML 설정 파일 파싱

### 테스트

```bash
# 단일 분기 테스트
python collect_news.py --mode single \
  --keyword "청도혁신센터" \
  --quarter "2024_Q3" \
  --start-date "2024-07-01" \
  --end-date "2024-09-30"
```

## 🤝 기여

이슈나 개선 사항은 GitHub Issues에 등록해주세요.

## 📄 라이센스

MIT License

## 👤 작성자

- GitHub: [@Sgorilla3v3](https://github.com/Sgorilla3v3)
- 프로젝트: [naver_NsBg](https://github.com/Sgorilla3v3/naver_NsBg)

## 📚 참고 자료

- [네이버 개발자 센터](https://developers.naver.com/)
- [네이버 검색 API 가이드](https://developers.naver.com/docs/serviceapi/search/news/news.md)
- [Python dotenv 문서](https://pypi.org/project/python-dotenv/)
- [PyYAML 문서](https://pyyaml.org/)
