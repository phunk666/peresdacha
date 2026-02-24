import csv
import json
import time
import random
import hashlib
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple
from collections import defaultdict

def generate_test_csv(filename: str, num_rows: int = 10000, duplicate_ratio: float = 0.3):
    unique_rows = num_rows - int(num_rows * duplicate_ratio)
    
    data = []
    for i in range(unique_rows):
        row = {
            'id': i,
            'name': f'User_{i}',
            'email': f'user{i}@example.com',
            'value': random.randint(1, 1000)
        }
        data.append(row)
    
    for i in range(num_rows - unique_rows):
        duplicate = random.choice(data[:100])
        data.append(duplicate.copy())
    
    random.shuffle(data)
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'email', 'value'])
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Сгенерирован файл {filename} с {num_rows} строк (примерно {int(num_rows * duplicate_ratio)} дубликатов)")
    return filename

class DataLoader:
    @staticmethod
    def load_csv(filename: str) -> List[Dict]:
        data = []
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['id'] = int(row['id'])
                row['value'] = int(row['value'])
                data.append(row)
        return data
    
    @staticmethod
    def load_json(filename: str) -> List[Dict]:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)

class Deduplicator:
    @staticmethod
    def naive_deduplicate(data: List[Dict]) -> Tuple[List[Dict], float]:
        start_time = time.time()
        unique_data = []
        for item in data:
            if item not in unique_data:
                unique_data.append(item)
        elapsed = time.time() - start_time
        return unique_data, elapsed
    
    @staticmethod
    def hash_deduplicate(data: List[Dict]) -> Tuple[List[Dict], float]:
        start_time = time.time()
        seen_hashes = set()
        unique_data = []
        
        for item in data:
            item_str = json.dumps(item, sort_keys=True)
            hash_obj = hashlib.md5(item_str.encode())
            item_hash = hash_obj.hexdigest()
            
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                unique_data.append(item)
        
        elapsed = time.time() - start_time
        return unique_data, elapsed
    
    @staticmethod
    def set_deduplicate(data: List[Dict]) -> Tuple[List[Dict], float]:
        start_time = time.time()
        seen = set()
        unique_data = []
        
        for item in data:
            item_tuple = tuple(sorted(item.items()))
            if item_tuple not in seen:
                seen.add(item_tuple)
                unique_data.append(item)
        
        elapsed = time.time() - start_time
        return unique_data, elapsed

class JoinOptimizer:
    @staticmethod
    def naive_cartesian_join(data1: List[Dict], data2: List[Dict], key: str) -> Tuple[List[Dict], float]:
        start_time = time.time()
        result = []
        
        for row1 in data1:
            for row2 in data2:
                if row1.get(key) == row2.get(key):
                    merged = {**row1, **{f"{k}_2": v for k, v in row2.items() if k != key}}
                    result.append(merged)
        
        elapsed = time.time() - start_time
        return result, elapsed
    
    @staticmethod
    def hash_join(data1: List[Dict], data2: List[Dict], key: str) -> Tuple[List[Dict], float]:
        start_time = time.time()
        
        hash_index = defaultdict(list)
        for row in data2:
            hash_index[row.get(key)].append(row)
        
        result = []
        for row1 in data1:
            matching_rows = hash_index.get(row1.get(key), [])
            for row2 in matching_rows:
                merged = {**row1, **{f"{k}_2": v for k, v in row2.items() if k != key}}
                result.append(merged)
        
        elapsed = time.time() - start_time
        return result, elapsed

class PerformanceVisualizer:
    @staticmethod
    def plot_deduplication_times(times: Dict[str, float], output_file: str = "dedup_performance.png"):
        plt.figure(figsize=(10, 6))
        methods = list(times.keys())
        time_values = list(times.values())
        
        bars = plt.bar(methods, time_values, color=['red', 'green', 'blue'])
        plt.title('Сравнение производительности удаления дубликатов')
        plt.xlabel('Метод')
        plt.ylabel('Время выполнения (секунды)')
        
        for bar, val in zip(bars, time_values):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                    f'{val:.4f}s', ha='center', va='bottom')
        
        plt.savefig(output_file)
        plt.show()
        print(f"График сохранен как {output_file}")
    
    @staticmethod
    def plot_join_times(times: Dict[str, float], output_file: str = "join_performance.png"):
        plt.figure(figsize=(8, 6))
        methods = list(times.keys())
        time_values = list(times.values())
        
        bars = plt.bar(methods, time_values, color=['orange', 'purple'])
        plt.title('Сравнение производительности JOIN операций')
        plt.xlabel('Метод')
        plt.ylabel('Время выполнения (секунды)')
        
        for bar, val in zip(bars, time_values):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.0005, 
                    f'{val:.4f}s', ha='center', va='bottom')
        
        plt.savefig(output_file)
        plt.show()
        print(f"График сохранен как {output_file}")

def run_etl_pipeline():
    print("=" * 60)
    print("ЗАПУСК ETL ПАЙПЛАЙНА С ОПТИМИЗАЦИЕЙ")
    print("=" * 60)
    
    print("\n[1] Генерация тестовых данных...")
    filename = "test_data.csv"
    generate_test_csv(filename, num_rows=5000, duplicate_ratio=0.25)
    
    print("\n[2] Загрузка данных...")
    data = DataLoader.load_csv(filename)
    print(f"Загружено строк: {len(data)}")
    
    print("\n[3] Удаление дубликатов...")
    
    unique_naive, time_naive = Deduplicator.naive_deduplicate(data)
    print(f"  Наивный метод: {len(unique_naive)} уникальных строк, время: {time_naive:.4f} сек")
    
    unique_hash, time_hash = Deduplicator.hash_deduplicate(data)
    print(f"  Хеш-метод (hashlib): {len(unique_hash)} уникальных строк, время: {time_hash:.4f} сек")
    
    unique_set, time_set = Deduplicator.set_deduplicate(data)
    print(f"  Set-метод: {len(unique_set)} уникальных строк, время: {time_set:.4f} сек")
    
    assert len(unique_naive) == len(unique_hash) == len(unique_set), "Количество уникальных строк не совпадает!"
    
    print("\n[4] Оптимизация JOIN операций...")
    
    data_left = unique_set[:1000]
    
    data_right = []
    for i in range(2000):
        data_right.append({
            'order_id': i,
            'user_id': random.choice([d['id'] for d in data_left[:500]]),
            'amount': random.randint(10, 500)
        })
    
    print(f"  Левый набор: {len(data_left)} записей")
    print(f"  Правый набор: {len(data_right)} записей")
    
    join_naive, time_join_naive = JoinOptimizer.naive_cartesian_join(data_left, data_right, 'id')
    print(f"  Наивный JOIN (декартово): {len(join_naive)} записей, время: {time_join_naive:.4f} сек")
    
    join_hash, time_join_hash = JoinOptimizer.hash_join(data_left, data_right, 'id')
    print(f"  Hash JOIN: {len(join_hash)} записей, время: {time_join_hash:.4f} сек")
    
    print("\n[5] Визуализация производительности...")
    
    viz = PerformanceVisualizer()
    viz.plot_deduplication_times({
        'Наивный O(n²)': time_naive,
        'Хеширование\n(hashlib)': time_hash,
        'Set-метод\n(оптимальный)': time_set
    })
    
    viz.plot_join_times({
        'Декартово\nпроизведение': time_join_naive,
        'Hash Join\n(оптимизация)': time_join_hash
    })
    
    print("\n[6] Сохранение очищенного датасета...")
    output_file = "cleaned_data.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'email', 'value'])
        writer.writeheader()
        writer.writerows(unique_set)
    
    print(f"  Очищенный датасет сохранен в {output_file}")
    print(f"\n✅ ETL пайплайн завершен успешно!")

if __name__ == "__main__":
    run_etl_pipeline()
