# [file name]: etl/load.py
"""
Модуль для загрузки данных в БД и сохранения в Parquet
"""
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

class DataLoader:
    """Класс для загрузки данных в БД (адаптирован из almas_write_to_db2.py)"""
    
    def __init__(self):
        self.engine = None
        self.dataset = None
    
    def get_connection_params(self, db_path: str = "creds.db") -> dict:
        """
        Извлекает параметры подключения из SQLite базы
        
        Args:
            db_path: путь к SQLite базе с credentials
            
        Returns:
            dict: параметры подключения
        """
        conn_params = {}
        try:
            sqlite_conn = sqlite3.connect(db_path)
            query = "SELECT url, port, user, pass FROM access;"
            result = pd.read_sql_query(query, sqlite_conn)
            sqlite_conn.close()
            
            if not result.empty:
                conn_params = result.iloc[0].to_dict()
                print("Параметры подключения получены")
            return conn_params
        except Exception as err:
            print(f"Ошибка получения параметров: {err}")
            return None
    
    def setup_database_connection(self, database_name: str = "homeworks") -> bool:
        """
        Устанавливает соединение с PostgreSQL
        
        Args:
            database_name: имя базы данных
            
        Returns:
            bool: True если подключение успешно
        """
        conn_params = self.get_connection_params()
        if not conn_params:
            return False
        
        connection_string = (
            f"postgresql+psycopg2://{conn_params['user']}:{conn_params['pass']}"
            f"@{conn_params['url']}:{conn_params['port']}/{database_name}"
        )
        
        try:
            self.engine = create_engine(
                connection_string,
                pool_recycle=3600,
                connect_args={'connect_timeout': 15}
            )
            # Проверка подключения
            with self.engine.connect() as test_conn:
                print(f"Подключение к {database_name} установлено")
            return True
        except Exception as err:
            print(f"Ошибка подключения к БД: {err}")
            return False
    
    def load_to_database(self, data: pd.DataFrame, table_name: str = "rakhimkulov", max_rows: int = 100) -> bool:
        """
        Загружает данные в PostgreSQL (максимум max_rows строк)
        
        Args:
            data: DataFrame с данными
            table_name: имя таблицы в БД
            max_rows: максимальное количество строк для загрузки
            
        Returns:
            bool: True если загрузка успешна
        """
        if self.engine is None:
            print("Подключение к БД не установлено")
            return False
        
        # Ограничиваем количество строк
        data_limited = data.head(max_rows)
        
        try:
            with self.engine.begin() as db_connection:
                data_limited.to_sql(
                    name=table_name,
                    con=db_connection,
                    schema="public",
                    if_exists="replace",
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            print(f"Данные записаны в таблицу {table_name} ({len(data_limited)} строк)")
            return True
        except Exception as err:
            print(f"Ошибка записи данных в БД: {err}")
            return False
    
    def save_to_parquet(self, data: pd.DataFrame, filename: str = "processed_data.parquet") -> str:
        """
        Сохраняет данные в Parquet формате
        
        Args:
            data: DataFrame с данными
            filename: имя файла
            
        Returns:
            str: путь к сохраненному файлу
        """
        processed_dir = Path("data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = processed_dir / filename
        
        try:
            data.to_parquet(output_path, index=False)
            print(f"Данные сохранены в Parquet: {output_path}")
            return str(output_path)
        except Exception as err:
            print(f"Ошибка сохранения в Parquet: {err}")
            return None
    
    def validate_output_data(self, table_name: str = "rakhimkulov", expected_rows: int = None) -> bool:
        """
        Валидация загруженных данных
        
        Args:
            table_name: имя таблицы для проверки
            expected_rows: ожидаемое количество строк
            
        Returns:
            bool: True если данные валидны
        """
        if self.engine is None:
            print("Подключение к БД не установлено")
            return False
        
        try:
            # Проверяем что таблица существует и содержит данные
            check_query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = pd.read_sql(check_query, self.engine)
            
            row_count = result.iloc[0]['count']
            print(f"В таблице {table_name}: {row_count} строк")
            
            if expected_rows and row_count != expected_rows:
                print(f"Предупреждение: ожидалось {expected_rows} строк, получено {row_count}")
                return False
            
            # Показываем пример данных
            sample_query = f"SELECT * FROM {table_name} LIMIT 3"
            sample_data = pd.read_sql(sample_query, self.engine)
            print(f"\nПример данных из БД ({len(sample_data)} записи):")
            print(sample_data.to_string(index=False))
            
            return True
            
        except Exception as err:
            print(f"Ошибка валидации выходных данных: {err}")
            return False

def load_data(transformed_file: str, table_name: str = "rakhimkulov", max_rows: int = 100) -> bool:
    """
    Основная функция загрузки данных
    
    Args:
        transformed_file: путь к трансформированному CSV файлу
        table_name: имя таблицы в БД
        max_rows: максимальное количество строк для загрузки
        
    Returns:
        bool: True если весь процесс успешен
    """
    loader = DataLoader()
    
    # Загрузка данных
    data = pd.read_csv(transformed_file)
    print(f"Загружено {len(data)} строк для загрузки в БД")
    
    # Подключение к БД
    if not loader.setup_database_connection():
        return False
    
    # Загрузка в БД (максимум max_rows строк)
    if not loader.load_to_database(data, table_name, max_rows):
        return False
    
    # Сохранение в Parquet
    parquet_path = loader.save_to_parquet(data)
    if not parquet_path:
        return False
    
    # Валидация
    if not loader.validate_output_data(table_name, min(max_rows, len(data))):
        return False
    
    print("✓ Загрузка данных завершена успешно")
    return True