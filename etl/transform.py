# [file name]: etl/transform.py
"""
Модуль для трансформации данных
"""
import pandas as pd
from pathlib import Path

def transform_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приведение типов данных (из твоего скрипта almas_data_loader_privedenie.py)
    
    Args:
        df: исходный DataFrame
        
    Returns:
        pd.DataFrame: DataFrame с приведенными типами
    """
    print("Начало трансформации типов данных...")
    print("Типы до обработки:")
    print(df.dtypes)
    
    numeric_columns = [
        "Year_of_Release",
        "NA_Sales",
        "EU_Sales", 
        "JP_Sales",
        "Other_Sales",
        "Global_Sales",
        "Critic_Score",
        "Critic_Count",
        "User_Score",
        "User_Count"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Особое преобразование для Year_of_Release
    if 'Year_of_Release' in df.columns:
        df['Year_of_Release'] = pd.to_numeric(df['Year_of_Release'], errors='coerce').fillna(0).astype(int)

    print("Типы после обработки:")
    print(df.dtypes)
    
    return df

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Нормализация названий колонок (из almas_write_to_db2.py)
    
    Args:
        df: исходный DataFrame
        
    Returns:
        pd.DataFrame: DataFrame с нормализованными названиями колонок
    """
    df.columns = [
        col.strip().lower().replace(' ', '_') 
        for col in df.columns
    ]
    print("Названия колонок нормализованы")
    return df

def transform_data(input_file: str, output_file: str = "transformed_dataset.csv") -> str:
    """
    Основная функция трансформации данных
    
    Args:
        input_file: путь к входному CSV файлу
        output_file: имя выходного файла
        
    Returns:
        str: путь к трансформированному файлу
    """
    # Создаем директорию data/processed если не существует
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = processed_dir / output_file
    
    # Чтение данных
    df = pd.read_csv(input_file)
    print(f"Загружено {len(df)} строк для трансформации")
    
    # Трансформации
    df = transform_data_types(df)
    df = normalize_column_names(df)
    
    # Сохранение
    df.to_csv(output_path, index=False)
    print(f"Трансформированные данные сохранены в {output_path}")
    
    return str(output_path)