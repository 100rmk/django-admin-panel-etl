import dataclasses
import os
import sqlite3
from typing import Generator, Any

import psycopg2
from attr import dataclass
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_to_postgres.entities import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork

TABLES = {
    'film_work': Filmwork,
    'genre': Genre,
    'person': Person,
    'genre_film_work': GenreFilmwork,
    'person_film_work': PersonFilmwork
}
# Load environments
load_dotenv()

DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432)
}

BATCH = 500
SQLITE_DB_PATH = 'db.sqlite'


class SQLiteLoader:
    """
    Класс для выгрузки данных из sqlite
    """
    def __init__(self, *, connection: sqlite3.Connection):
        self.con = connection

    def load_movies(self, table: str, model: dataclass) -> Generator:
        """
        Метод выгрузки данных из sqlite
        :param table: таблица из которой будет производиться выгрузка данных
        :param model: датакласс
        :yield: generator
        """
        cursor = self.con.cursor()
        cursor.execute('SELECT {0} FROM {1}'.format(model.sqlite_columns(), table))

        while rows := cursor.fetchmany(BATCH):
            yield self._make_rows_pretty(rows, model)

    @staticmethod
    def _make_rows_pretty(rows: list, model: dataclass) -> list:
        """
        Функция загрузки записей из sqlite
        :param rows: поля для форматирования
        :param model: датакласс
        :return: отформатированный список пригодный длязагрузки в postgres
        """
        pretty_rows = []
        for row in rows:
            pretty_rows.append(model(*row))

        return pretty_rows


class PostgresSaver:
    """
    Класс для загрузки данных в postgresql
    """
    def __init__(self, connection):
        self.con = connection

    def save_data_batch(self, data: list, table: str, model: dataclass) -> None:
        """
        Метод загрузки записей в postresql
        :param data: данные для загрузки
        :param table: таблица в которую будет  произведена загрузка
        :param model датакласс
        :return: отформатированный список пригодный длязагрузки в postgres
        """
        cursor = self.con.cursor()
        columns = tuple(model.__annotations__.keys())
        columns_pretty = ', '.join(columns)
        mogrify_pattern = ', '.join(['%s'] * len(columns))
        args = ','.join(
            cursor.mogrify(f'({mogrify_pattern})', dataclasses.astuple(item)).decode() for item in data
        )
        cursor.execute(f"""
                        INSERT INTO content.{table} ({columns_pretty})
                        VALUES {args}
                        ON CONFLICT (id) DO NOTHING
                        """)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """
    Основная функция загрузки данных из SQLite в Postgres
    :param connection: подключение к sqlite
    :param pg_conn: подключение к postgres
    """
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection=connection)
    for table, model in TABLES.items():
        data = sqlite_loader.load_movies(table, model)
        for batch in data:
            postgres_saver.save_data_batch(batch, table, model)


if __name__ == '__main__':
    psycopg2.extras.register_uuid()

    with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(**DSL, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)

    sqlite_conn.close()
    pg_conn.close()
