"""
분할 작업 목록 생성 스크립트
여러 워커가 병렬로 작업할 수 있도록 작업 목록을 생성합니다.
"""

from datetime import datetime, timedelta
import json

def generate_quarterly_ranges(start_year=2022):
    """분기별 날짜 범위 생성"""
    quarters = []
    quarter_months = [
        ("Q1", 1, 3), ("Q2", 4, 6), ("Q3", 7, 9), ("Q4", 10, 12)
    ]
    
    for year in range(start_year, datetime.now().year + 1):
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
            
            quarters.append((
                f"{year}_{q_name}",
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            ))
    
    return quarters

def generate_task_list(keywords, start_year=2022, output_format="bash"):
    """작업 목록 생성
    
    Args:
        keywords: 키워드 리스트
        start_year: 시작 연도
        output_format: 출력 형식 (bash, json, python)
    """
    quarters = generate_quarterly_ranges(start_year)
    
    tasks = []
    task_num = 1
    
    for keyword in keywords:
        for quarter_name, start_date, end_date in quarters:
            tasks.append({
                "task_id": task_num,
                "keyword": keyword,
                "quarter": quarter_name,
                "start_date": start_date,
                "end_date": end_date
            })
            task_num += 1
    
    # Bash 스크립트 생성
    if output_format == "bash":
        with open("run_all_tasks.sh", "w", encoding="utf-8") as f:
            f.write("#!/bin/bash\n\n")
            f.write("# 네이버 뉴스 수집 - 전체 작업 실행 스크립트\n")
            f.write(f"# 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 총 작업 수: {len(tasks)}\n\n")
            
            for task in tasks:
                cmd = (f'python collect_news.py --mode single '
                      f'--keyword "{task["keyword"]}" '
                      f'--quarter "{task["quarter"]}" '
                      f'--start-date "{task["start_date"]}" '
                      f'--end-date "{task["end_date"]}"\n')
                f.write(f"# Task {task['task_id']}\n")
                f.write(cmd)
                f.write("\n")
            
            f.write("# 병합\n")
            f.write("python collect_news.py --mode merge\n")
        
        print(f"✅ run_all_tasks.sh 생성 완료! (총 {len(tasks)}개 작업)")
    
    # JSON 형식 생성
    if output_format == "json":
        with open("tasks.json", "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now().isoformat(),
                "total_tasks": len(tasks),
                "tasks": tasks
            }, f, ensure_ascii=False, indent=2)
        
        print(f"✅ tasks.json 생성 완료! (총 {len(tasks)}개 작업)")
    
    # Python 배치 실행 스크립트 생성
    if output_format == "python":
        with open("run_batch.py", "w", encoding="utf-8") as f:
            f.write('"""\n배치 실행 스크립트\n"""\n\n')
            f.write("import subprocess\nimport sys\nfrom datetime import datetime\n\n")
            f.write("tasks = [\n")
            for task in tasks:
                f.write(f"    {task},\n")
            f.write("]\n\n")
            f.write("""
def run_task(task):
    cmd = [
        "python", "collect_news.py",
        "--mode", "single",
        "--keyword", task["keyword"],
        "--quarter", task["quarter"],
        "--start-date", task["start_date"],
        "--end-date", task["end_date"]
    ]
    
    print(f"[{task['task_id']}/{len(tasks)}] 실행 중: {task['keyword']} - {task['quarter']}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ 완료: Task {task['task_id']}")
        return True
    else:
        print(f"❌ 실패: Task {task['task_id']}")
        print(result.stderr)
        return False

if __name__ == "__main__":
    print(f"총 {len(tasks)}개 작업 시작")
    print(f"시작 시간: {datetime.now()}")
    
    success_count = 0
    for task in tasks:
        if run_task(task):
            success_count += 1
    
    print(f"\\n완료 시간: {datetime.now()}")
    print(f"성공: {success_count}/{len(tasks)}")
    
    # 병합
    print("\\n병합 시작...")
    subprocess.run(["python", "collect_news.py", "--mode", "merge"])
""")
        
        print(f"✅ run_batch.py 생성 완료! (총 {len(tasks)}개 작업)")
    
    return tasks

def generate_parallel_scripts(keywords, start_year=2022, num_workers=4):
    """병렬 실행용 스크립트 생성
    
    Args:
        keywords: 키워드 리스트
        start_year: 시작 연도
        num_workers: 워커 수
    """
    quarters = generate_quarterly_ranges(start_year)
    
    all_tasks = []
    task_num = 1
    
    for keyword in keywords:
        for quarter_name, start_date, end_date in quarters:
            all_tasks.append({
                "task_id": task_num,
                "keyword": keyword,
                "quarter": quarter_name,
                "start_date": start_date,
                "end_date": end_date
            })
            task_num += 1
    
    # 작업을 워커 수만큼 분할
    tasks_per_worker = len(all_tasks) // num_workers
    
    for worker_id in range(num_workers):
        start_idx = worker_id * tasks_per_worker
        if worker_id == num_workers - 1:
            # 마지막 워커는 남은 모든 작업 처리
            end_idx = len(all_tasks)
        else:
            end_idx = (worker_id + 1) * tasks_per_worker
        
        worker_tasks = all_tasks[start_idx:end_idx]
        
        # 워커별 스크립트 생성
        filename = f"run_worker_{worker_id + 1}.sh"
        with open(filename, "w", encoding="utf-8") as f:
            f.write("#!/bin/bash\n\n")
            f.write(f"# Worker {worker_id + 1}/{num_workers}\n")
            f.write(f"# 담당 작업: {len(worker_tasks)}개\n\n")
            
            for task in worker_tasks:
                cmd = (f'python collect_news.py --mode single '
                      f'--keyword "{task["keyword"]}" '
                      f'--quarter "{task["quarter"]}" '
                      f'--start-date "{task["start_date"]}" '
                      f'--end-date "{task["end_date"]}"\n')
                f.write(f"# Task {task['task_id']}\n")
                f.write(cmd)
                f.write("\n")
        
        print(f"✅ {filename} 생성 완료! ({len(worker_tasks)}개 작업)")
    
    # 마스터 실행 스크립트 생성
    with open("run_all_workers.sh", "w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n\n")
        f.write(f"# 모든 워커 병렬 실행 스크립트\n")
        f.write(f"# 워커 수: {num_workers}\n\n")
        
        # 백그라운드로 모든 워커 실행
        for worker_id in range(num_workers):
            f.write(f"bash run_worker_{worker_id + 1}.sh &\n")
        
        f.write("\n# 모든 워커가 완료될 때까지 대기\n")
        f.write("wait\n\n")
        f.write("# 병합\n")
        f.write("python collect_news.py --mode merge\n")
    
    print(f"\n✅ run_all_workers.sh 생성 완료!")
    print(f"📝 실행 방법: bash run_all_workers.sh")

if __name__ == "__main__":
    keywords = ["청도혁신센터", "경북시민재단", "로컬임팩트랩", "경북지속가능캠프", "청도군"]
    
    print("=" * 60)
    print("작업 목록 생성")
    print("=" * 60)
    print(f"키워드: {keywords}")
    print(f"시작 연도: 2022")
    print()
    
    # 1. 순차 실행 스크립트
    print("\n[1] 순차 실행 스크립트 생성")
    generate_task_list(keywords, start_year=2022, output_format="bash")
    generate_task_list(keywords, start_year=2022, output_format="json")
    generate_task_list(keywords, start_year=2022, output_format="python")
    
    # 2. 병렬 실행 스크립트 (4개 워커)
    print("\n[2] 병렬 실행 스크립트 생성 (4개 워커)")
    generate_parallel_scripts(keywords, start_year=2022, num_workers=4)
    
    print("\n" + "=" * 60)
    print("완료!")
    print("=" * 60)
    print("\n실행 방법:")
    print("  1. 순차 실행: bash run_all_tasks.sh")
    print("  2. Python 배치: python run_batch.py")
    print("  3. 병렬 실행 (4워커): bash run_all_workers.sh")
