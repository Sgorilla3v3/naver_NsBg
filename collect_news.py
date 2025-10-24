"""
네이버 뉴스 검색 API - 분기별 분할 수집 스크립트
작성일: 2025-10-24
"""

import os
import sys
import time
import requests
import pandas as pd
from math import ceil
from datetime import datetime, timedelta
import argparse
import yaml
from pathlib import Path
from dotenv import load_dotenv
import logging

# ─────────────────────────────────────────────────────────
# 1) 설정 로드
def load_config(config_path: str = "config.yaml") -> dict:
    """YAML 설정 파일 로드"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"⚠️ 설정 파일을 찾을 수 없습니다: {config_path}")
        print("기본 설정을 사용합니다.")
        return get_default_config()
    except yaml.YAMLError as e:
        print(f"⚠️ YAML 파싱 오류: {e}")
        print("기본 설정을 사용합니다.")
        return get_default_config()

def get_default_config() -> dict:
    """기본 설정 반환"""
    return {
        'keywords': ["청도혁신센터", "경북시민재단", "로컬임팩트랩", "경북지속가능캠프", "청도군"],
        'collection': {
            'start_year': 2022,
            'display_per_page': 100,
            'max_items_per_query': 1000,
            'api_call_delay': 0.1,
            'request_timeout': 10
        },
        'output': {
            'parts_dir': 'output_parts',
            'merged_dir': 'output',
            'merged_filename': 'news_merged.csv',
            'encoding': 'utf-8-sig'
        },
        'logging': {
            'dir': 'logs',
            'level': 'INFO',
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        },
        'filtering': {
            'exact_phrase_match': True,
            'remove_duplicates': True,
            'duplicate_check_column': 'url'
        },
        'api': {
            'search_endpoint': 'https://openapi.naver.com/v1/search',
            'sort': 'date',
            'retry_count': 3,
            'retry_delay': 1.0
        }
    }

# 환경 변수 로드
load_dotenv()

# 설정 로드
CONFIG = load_config()

# 인증 정보
CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ 오류: 네이버 API 키가 설정되지 않았습니다.")
    print("다음 중 하나를 수행하세요:")
    print("  1. .env 파일을 생성하고 NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET을 설정")
    print("  2. 환경 변수로 설정: export NAVER_CLIENT_ID=your_id")
    sys.exit(1)

# ─────────────────────────────────────────────────────────
# 2) 로깅 설정
def setup_logging():
    """로깅 설정"""
    log_config = CONFIG.get('logging', {})
    log_dir = log_config.get('dir', 'logs')
    log_level = log_config.get('level', 'INFO')
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    
    # 로그 디렉토리 생성
    Path(log_dir).mkdir(exist_ok=True)
    
    # 로그 레벨 설정
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # 로거 설정
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                os.path.join(log_dir, f"collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                encoding='utf-8'
            )
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ─────────────────────────────────────────────────────────
# 3) Search API 공통 호출 함수
def naver_search(endpoint: str, query: str, display: int = None, start: int = 1, sort: str = None) -> dict:
    """네이버 검색 API 호출"""
    api_config = CONFIG.get('api', {})
    collection_config = CONFIG.get('collection', {})
    
    if display is None:
        display = collection_config.get('display_per_page', 100)
    if sort is None:
        sort = api_config.get('sort', 'date')
    
    base_url = api_config.get('search_endpoint', 'https://openapi.naver.com/v1/search')
    url = f"{base_url}/{endpoint}.json"
    
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
    }
    params = {
        "query": query, 
        "display": display, 
        "start": start,
        "sort": sort
    }
    
    timeout = collection_config.get('request_timeout', 10)
    retry_count = api_config.get('retry_count', 3)
    retry_delay = api_config.get('retry_delay', 1.0)
    
    for attempt in range(retry_count):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=timeout)
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                logger.warning(f"API 호출 실패 (재시도 {attempt + 1}/{retry_count}): {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"API 호출 최종 실패: {e}")
                return {"total": 0, "items": []}

# ─────────────────────────────────────────────────────────
# 4) 분기별 날짜 범위 생성
def generate_quarterly_ranges(start_year: int, end_year: int = None) -> list:
    """분기별 날짜 범위 생성"""
    if end_year is None:
        end_year = datetime.now().year
    
    quarters = []
    quarter_months = [
        ("Q1", 1, 3),
        ("Q2", 4, 6),
        ("Q3", 7, 9),
        ("Q4", 10, 12)
    ]
    
    for year in range(start_year, end_year + 1):
        for q_name, start_month, end_month in quarter_months:
            start_date = datetime(year, start_month, 1)
            
            if end_month == 12:
                end_date = datetime(year, 12, 31)
            else:
                end_date = datetime(year, end_month + 1, 1) - timedelta(days=1)
            
            if start_date > datetime.now():
                break
            
            if end_date > datetime.now():
                end_date = datetime.now()
            
            quarter_label = f"{year}_{q_name}"
            quarters.append((
                quarter_label,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            ))
    
    return quarters

# ─────────────────────────────────────────────────────────
# 5) 페이징 수집
def fetch_news_in_quarter(keyword: str, quarter_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """특정 분기의 뉴스 데이터 수집"""
    collection_config = CONFIG.get('collection', {})
    display = collection_config.get('display_per_page', 100)
    max_items = collection_config.get('max_items_per_query', 1000)
    api_delay = collection_config.get('api_call_delay', 0.1)
    
    logger.info(f"[{quarter_name}] {start_date} ~ {end_date}")
    
    first = naver_search("news", keyword, display=display, start=1)
    total_available = first.get("total", 0)
    total_to_fetch = min(total_available, max_items)

    items = first.get("items", [])
    
    if total_to_fetch == 0:
        logger.info(f"검색 결과 없음")
        return pd.DataFrame()
    
    pages = ceil(total_to_fetch / display)

    for page in range(2, pages + 1):
        start = (page - 1) * display + 1
        time.sleep(api_delay)
        
        js = naver_search("news", keyword, display=display, start=start)
        batch = js.get("items", [])
        
        if not batch:
            break
        
        items.extend(batch)
        
        if len(items) >= max_items:
            items = items[:max_items]
            break

    df = pd.DataFrame(items)
    if not df.empty:
        df = df[["title", "link", "originallink", "description", "pubDate"]].rename(columns={
            "link": "url", 
            "originallink": "source_url", 
            "pubDate": "date"
        })
        
        df["title"] = df["title"].str.replace(r"</?b>", "", regex=True)
        df["description"] = df["description"].str.replace(r"</?b>", "", regex=True)
        
        df = filter_by_date_range(df, start_date, end_date)
        logger.info(f"수집 완료: {len(df)}건")
    else:
        logger.info(f"검색 결과 없음")
    
    return df

# ─────────────────────────────────────────────────────────
# 6) 날짜 범위 필터링
def filter_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """날짜 범위로 필터링"""
    if df.empty:
        return df
    
    df["parsed_date"] = pd.to_datetime(df["date"], format="%a, %d %b %Y %H:%M:%S %z", errors="coerce")
    
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date) + timedelta(days=1)
    
    mask = (df["parsed_date"] >= start) & (df["parsed_date"] < end)
    filtered_df = df[mask].copy()
    
    if "parsed_date" in filtered_df.columns:
        filtered_df = filtered_df.drop(columns=["parsed_date"])
    
    return filtered_df

# ─────────────────────────────────────────────────────────
# 7) 정확 연속어구 필터링
def filter_exact_phrase(df: pd.DataFrame, phrase: str) -> pd.DataFrame:
    """제목 또는 설명에 정확한 문구가 포함된 항목만 필터링"""
    if df.empty:
        return df
    
    filtering_config = CONFIG.get('filtering', {})
    if not filtering_config.get('exact_phrase_match', True):
        return df
    
    mask_title = df["title"].str.contains(phrase, regex=False, na=False)
    mask_desc = df["description"].str.contains(phrase, regex=False, na=False)
    return df[mask_title | mask_desc]

# ─────────────────────────────────────────────────────────
# 8) 단일 키워드, 단일 분기 수집
def collect_single_keyword_quarter(keyword: str, quarter_name: str, start_date: str, 
                                   end_date: str, output_dir: str = None) -> str:
    """단일 키워드와 단일 분기 데이터 수집 및 저장"""
    if output_dir is None:
        output_dir = CONFIG.get('output', {}).get('parts_dir', 'output_parts')
    
    logger.info(f"=" * 60)
    logger.info(f"키워드: '{keyword}' | 분기: {quarter_name}")
    logger.info(f"=" * 60)
    
    # 데이터 수집
    df = fetch_news_in_quarter(keyword, quarter_name, start_date, end_date)
    
    if df.empty:
        logger.warning(f"수집된 데이터 없음")
        return None
    
    # 정확 문구 필터링
    df_exact = filter_exact_phrase(df, keyword)
    logger.info(f"정확 문구 매칭: {len(df_exact)}건")
    
    if df_exact.empty:
        logger.warning(f"필터링 후 데이터 없음")
        return None
    
    # 분기 정보 추가
    df_exact["quarter"] = quarter_name
    df_exact["keyword"] = keyword
    
    # 출력 디렉토리 생성
    Path(output_dir).mkdir(exist_ok=True)
    
    # 파일명
    filename = f"{keyword}_{quarter_name}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # CSV 저장
    encoding = CONFIG.get('output', {}).get('encoding', 'utf-8-sig')
    df_exact.to_csv(filepath, index=False, encoding=encoding)
    logger.info(f"저장 완료: {filepath} ({len(df_exact)}건)")
    
    return filepath

# ─────────────────────────────────────────────────────────
# 9) 전체 키워드, 전체 분기 수집
def collect_all_keywords_all_quarters(keywords: list = None, start_year: int = None, 
                                      output_dir: str = None):
    """모든 키워드, 모든 분기 수집"""
    if keywords is None:
        keywords = CONFIG.get('keywords', [])
    if start_year is None:
        start_year = CONFIG.get('collection', {}).get('start_year', 2022)
    if output_dir is None:
        output_dir = CONFIG.get('output', {}).get('parts_dir', 'output_parts')
    
    logger.info("=" * 60)
    logger.info("네이버 뉴스 검색 API - 전체 분할 수집")
    logger.info("=" * 60)
    
    # 분기 목록 생성
    quarters = generate_quarterly_ranges(start_year)
    
    logger.info(f"분기 개수: {len(quarters)}")
    logger.info(f"키워드 개수: {len(keywords)}")
    logger.info(f"총 작업 개수: {len(keywords) * len(quarters)}")
    
    # 각 키워드, 각 분기별로 수집
    saved_files = []
    total_tasks = len(keywords) * len(quarters)
    current_task = 0
    
    api_delay = CONFIG.get('collection', {}).get('api_call_delay', 0.1)
    
    for keyword in keywords:
        for quarter_name, start_date, end_date in quarters:
            current_task += 1
            logger.info(f"\n[{current_task}/{total_tasks}] 작업 진행 중...")
            
            try:
                filepath = collect_single_keyword_quarter(
                    keyword, quarter_name, start_date, end_date, output_dir
                )
                
                if filepath:
                    saved_files.append(filepath)
            
            except Exception as e:
                logger.error(f"{keyword}_{quarter_name} - 오류: {e}")
            
            # API 호출 제한 고려
            time.sleep(api_delay * 5)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"전체 작업 완료!")
    logger.info(f"저장된 파일 개수: {len(saved_files)}")
    logger.info(f"저장 위치: {output_dir}/")
    logger.info(f"{'='*60}")
    
    return saved_files

# ─────────────────────────────────────────────────────────
# 10) 분할된 파일 병합
def merge_all_parts(input_dir: str = None, output_file: str = None):
    """분할된 CSV 파일들을 하나로 병합"""
    if input_dir is None:
        input_dir = CONFIG.get('output', {}).get('parts_dir', 'output_parts')
    if output_file is None:
        output_config = CONFIG.get('output', {})
        output_dir = output_config.get('merged_dir', 'output')
        filename = output_config.get('merged_filename', 'news_merged.csv')
        output_file = os.path.join(output_dir, filename)
    
    logger.info("\n" + "=" * 60)
    logger.info("분할 파일 병합 시작")
    logger.info("=" * 60)
    
    # 출력 디렉토리 생성
    Path(os.path.dirname(output_file)).mkdir(exist_ok=True)
    
    # CSV 파일 목록 가져오기
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        logger.error(f"'{input_dir}' 디렉토리에 CSV 파일이 없습니다.")
        return
    
    logger.info(f"발견된 파일 개수: {len(csv_files)}")
    
    # 모든 파일 읽어서 병합
    encoding = CONFIG.get('output', {}).get('encoding', 'utf-8-sig')
    all_dfs = []
    
    for i, filename in enumerate(csv_files, 1):
        filepath = os.path.join(input_dir, filename)
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            all_dfs.append(df)
            if i % 10 == 0:
                logger.info(f"{i}/{len(csv_files)} 파일 읽기 완료...")
        except Exception as e:
            logger.warning(f"{filename} 읽기 실패: {e}")
    
    if not all_dfs:
        logger.error("읽을 수 있는 파일이 없습니다.")
        return
    
    # 병합
    df_merged = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"\n병합 전 총 레코드: {len(df_merged):,}건")
    
    # 중복 제거
    filtering_config = CONFIG.get('filtering', {})
    if filtering_config.get('remove_duplicates', True):
        initial_count = len(df_merged)
        dup_column = filtering_config.get('duplicate_check_column', 'url')
        df_merged = df_merged.drop_duplicates(subset=[dup_column]).reset_index(drop=True)
        duplicates_removed = initial_count - len(df_merged)
        
        logger.info(f"중복 제거: {duplicates_removed:,}건")
    
    logger.info(f"최종 레코드: {len(df_merged):,}건")
    
    # 저장
    df_merged.to_csv(output_file, index=False, encoding=encoding)
    logger.info(f"\n병합 완료: {output_file}")
    
    # 통계 출력
    if "keyword" in df_merged.columns:
        logger.info(f"\n키워드별 통계:")
        keyword_counts = df_merged["keyword"].value_counts()
        for keyword, count in keyword_counts.items():
            logger.info(f"  - {keyword}: {count:,}건")
    
    if "quarter" in df_merged.columns:
        logger.info(f"\n분기별 통계 (상위 10개):")
        quarter_counts = df_merged["quarter"].value_counts().sort_index().tail(10)
        for quarter, count in quarter_counts.items():
            logger.info(f"  - {quarter}: {count:,}건")
    
    logger.info("=" * 60)
    
    return output_file

# ─────────────────────────────────────────────────────────
# 11) 메인 함수
def main():
    parser = argparse.ArgumentParser(description="네이버 뉴스 검색 API - 분할 작업")
    parser.add_argument("--mode", choices=["single", "all", "merge"], default="all",
                       help="실행 모드: single(단일 작업), all(전체 실행), merge(병합)")
    parser.add_argument("--keyword", type=str, help="검색 키워드 (single 모드)")
    parser.add_argument("--quarter", type=str, help="분기 (예: 2022_Q1) (single 모드)")
    parser.add_argument("--start-date", type=str, help="시작일 (YYYY-MM-DD) (single 모드)")
    parser.add_argument("--end-date", type=str, help="종료일 (YYYY-MM-DD) (single 모드)")
    parser.add_argument("--output-dir", type=str, help="출력 디렉토리")
    parser.add_argument("--start-year", type=int, help="시작 연도 (all 모드)")
    parser.add_argument("--config", type=str, default="config.yaml", help="설정 파일 경로")
    
    args = parser.parse_args()
    
    # 설정 파일 재로드 (커맨드라인에서 지정한 경우)
    if args.config != "config.yaml":
        global CONFIG
        CONFIG = load_config(args.config)
    
    keywords = CONFIG.get('keywords', [])
    
    if args.mode == "single":
        # 단일 작업 실행
        if not all([args.keyword, args.quarter, args.start_date, args.end_date]):
            logger.error("single 모드는 --keyword, --quarter, --start-date, --end-date가 필요합니다.")
            return
        
        collect_single_keyword_quarter(
            args.keyword, 
            args.quarter, 
            args.start_date, 
            args.end_date,
            args.output_dir
        )
    
    elif args.mode == "all":
        # 전체 실행
        start_year = args.start_year or CONFIG.get('collection', {}).get('start_year', 2022)
        collect_all_keywords_all_quarters(
            keywords,
            start_year,
            args.output_dir
        )
    
    elif args.mode == "merge":
        # 병합
        merge_all_parts(args.output_dir)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        # 명령줄 인자 없이 실행하면 전체 자동 실행
        logger.info("🚀 자동 실행 모드: 전체 수집 + 병합")
        collect_all_keywords_all_quarters()
        merge_all_parts()
    else:
        main()
