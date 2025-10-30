# [file name]: etl/main.py
"""
Главный модуль ETL пайплайна с CLI интерфейсом
"""
import argparse
import sys
from pathlib import Path

# Добавляем путь к пакету etl
sys.path.append(str(Path(__file__).parent))

from extract import extract_data
from transform import transform_data
from load import load_data
from validate import comprehensive_validation
import pandas as pd

def run_etl_pipeline(file_id: str = None, table_name: str = "rakhimkulov", 
                    max_rows: int = 100, skip_extraction: bool = False):
    """
    Запускает полный ETL пайплайн
    
    Args:
        file_id: ID файла на Google Drive
        table_name: имя таблицы в БД
        max_rows: максимальное количество строк для загрузки
        skip_extraction: пропустить этап извлечения (использовать существующие данные)
    """
    print("=" * 60)
    print("Запуск ETL пайплайна")
    print("=" * 60)
    
    try:
        # Этап 1: Extract (Извлечение)
        if skip_extraction:
            raw_file = "data/raw/raw_dataset.csv"
            if not Path(raw_file).exists():
                raise FileNotFoundError(f"Файл {raw_file} не найден")
            print("✓ Этап извлечения пропущен, используются существующие данные")
        else:
            print("\n1. ИЗВЛЕЧЕНИЕ ДАННЫХ")
            raw_file = extract_data(file_id)
            raw_data = pd.read_csv(raw_file)
            comprehensive_validation(raw_data, 'raw')
        
        # Этап 2: Transform (Трансформация)
        print("\n2. ТРАНСФОРМАЦИЯ ДАННЫХ")
        transformed_file = transform_data(raw_file)
        transformed_data = pd.read_csv(transformed_file)
        comprehensive_validation(transformed_data, 'transformed')
        
        # Этап 3: Load (Загрузка)
        print("\n3. ЗАГРУЗКА ДАННЫХ")
        success = load_data(transformed_file, table_name, max_rows)
        
        if success:
            print("\n" + "=" * 60)
            print("✓ ETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("✗ ETL ПАЙПЛАЙН ЗАВЕРШЕН С ОШИБКАМИ")
            print("=" * 60)
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Критическая ошибка в ETL пайплайне: {e}")
        sys.exit(1)

def main():
    """Основная функция с CLI интерфейсом"""
    parser = argparse.ArgumentParser(description='ETL пайплайн для обработки данных видеоигр')
    
    # Обязательный аргумент
    parser.add_argument(
        '--table-name',
        required=True,
        help='Имя таблицы в БД для загрузки данных (ОБЯЗАТЕЛЬНЫЙ)'
    )
    
    # Опциональные аргументы
    parser.add_argument(
        '--file-id',
        help='ID файла на Google Drive (по умолчанию: стандартный dataset)'
    )
    
    parser.add_argument(
        '--max-rows',
        type=int,
        default=100,
        help='Максимальное количество строк для загрузки в БД (по умолчанию: 100)'
    )
    
    parser.add_argument(
        '--skip-extraction',
        action='store_true',
        help='Пропустить этап извлечения (использовать существующие данные)'
    )
    
    args = parser.parse_args()
    
    # Запуск ETL пайплайна
    run_etl_pipeline(
        file_id=args.file_id,
        table_name=args.table_name,
        max_rows=args.max_rows,
        skip_extraction=args.skip_extraction
    )

if __name__ == "__main__":
    main()