# [file name]: etl/extract.py
"""
Модуль для извлечения и валидации сырых данных
"""
import gdown
import pandas as pd
import os
from pathlib import Path

def download_dataset(file_id: str = "1kk5pYKqE--lqhxEAfqsks3QVpK-LP-Ta", 
                    output_file: str = "raw_dataset.csv") -> str:
    """
    Загружает dataset с Google Drive
    
    Args:
        file_id: ID файла на Google Drive
        output_file: имя выходного файла
        
    Returns:
        str: путь к скачанному файлу
    """
    url = f"https://drive.google.com/uc?id={file_id}"
    
    # Создаем директорию data/raw если не существует
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = raw_dir / output_file
    
    print(f"Загрузка данных с {url}...")
    gdown.download(url, str(output_path), quiet=False)
    
    if not output_path.exists():
        raise FileNotFoundError(f"Файл {output_path} не был загружен")
    
    print(f"Данные сохранены в {output_path}")
    return str(output_path)

def validate_raw_data(file_path: str) -> bool:
    """
    Базовая валидация сырых данных
    
    Args:
        file_path: путь к CSV файлу
        
    Returns:
        bool: True если данные валидны
    """
    try:
        df = pd.read_csv(file_path)
        
        # Проверяем что файл не пустой
        if df.empty:
            print("Ошибка: файл пустой")
            return False
        
        # Проверяем минимальный набор колонок
        expected_columns = ['Year_of_Release', 'NA_Sales', 'EU_Sales', 'JP_Sales', 
                           'Other_Sales', 'Global_Sales']
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"Предупреждение: отсутствуют колонки: {missing_columns}")
        
        print(f"Сырые данные: {len(df)} строк, {len(df.columns)} колонок")
        print("Первые 5 строк:")
        print(df.head())
        print("\nТипы данных сырых данных:")
        print(df.dtypes)
        
        return True
        
    except Exception as e:
        print(f"Ошибка валидации сырых данных: {e}")
        return False

def extract_data(file_id: str = None, output_file: str = "raw_dataset.csv") -> str:
    """
    Основная функция извлечения данных
    
    Args:
        file_id: ID файла на Google Drive (если None, используется стандартный)
        output_file: имя выходного файла
        
    Returns:
        str: путь к скачанному файлу
    """
    if file_id is None:
        file_id = "1kk5pYKqE--lqhxEAfqsks3QVpK-LP-Ta"
    
    file_path = download_dataset(file_id, output_file)
    
    if validate_raw_data(file_path):
        print("✓ Извлечение и валидация сырых данных завершены успешно")
        return file_path
    else:
        raise ValueError("Валидация сырых данных не пройдена")