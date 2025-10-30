# [file name]: etl/validate.py
"""
Модуль для валидации данных на разных этапах ETL
"""
import pandas as pd
from pathlib import Path

def validate_schema(df: pd.DataFrame) -> bool:
    """
    Валидация схемы данных
    
    Args:
        df: DataFrame для валидации
        
    Returns:
        bool: True если схема валидна
    """
    required_columns = [
        'year_of_release', 'na_sales', 'eu_sales', 'jp_sales', 
        'other_sales', 'global_sales'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Ошибка валидации схемы: отсутствуют колонки {missing_columns}")
        return False
    
    print("✓ Валидация схемы пройдена")
    return True

def validate_data_quality(df: pd.DataFrame) -> bool:
    """
    Валидация качества данных
    
    Args:
        df: DataFrame для валидации
        
    Returns:
        bool: True если качество данных приемлемое
    """
    issues = []
    
    # Проверка на пустые значения в ключевых колонках
    key_columns = ['year_of_release', 'global_sales']
    for col in key_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append(f"Колонка {col}: {null_count} пустых значений")
    
    # Проверка аномальных значений
    if 'year_of_release' in df.columns:
        invalid_years = df[df['year_of_release'] < 1950]['year_of_release'].count()
        if invalid_years > 0:
            issues.append(f"Найдено {invalid_years} записей с годом выпуска до 1950")
    
    if 'global_sales' in df.columns:
        negative_sales = df[df['global_sales'] < 0]['global_sales'].count()
        if negative_sales > 0:
            issues.append(f"Найдено {negative_sales} записей с отрицательными продажами")
    
    if issues:
        print("Предупреждения по качеству данных:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✓ Валидация качества данных пройдена")
    
    return True  # Возвращаем True даже с предупреждениями

def validate_output_files() -> bool:
    """
    Валидация выходных файлов
    
    Returns:
        bool: True если файлы созданы корректно
    """
    files_to_check = [
        "data/raw/raw_dataset.csv",
        "data/processed/transformed_dataset.csv", 
        "data/processed/processed_data.parquet"
    ]
    
    missing_files = []
    for file_path in files_to_check:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"Ошибка: отсутствуют файлы: {missing_files}")
        return False
    
    print("✓ Все выходные файлы созданы корректно")
    return True

def comprehensive_validation(df: pd.DataFrame, stage: str) -> bool:
    """
    Комплексная валидация данных
    
    Args:
        df: DataFrame для валидации
        stage: этап валидации ('raw', 'transformed', 'output')
        
    Returns:
        bool: True если валидация пройдена
    """
    print(f"\n=== Валидация на этапе: {stage} ===")
    
    if df.empty:
        print("Ошибка: DataFrame пустой")
        return False
    
    # Базовая информация
    print(f"Данные: {len(df)} строк, {len(df.columns)} колонок")
    
    # Валидация в зависимости от этапа
    if stage in ['transformed', 'output']:
        if not validate_schema(df):
            return False
    
    if not validate_data_quality(df):
        return False
    
    if stage == 'output':
        if not validate_output_files():
            return False
    
    print(f"✓ Валидация этапа {stage} завершена успешно")
    return True