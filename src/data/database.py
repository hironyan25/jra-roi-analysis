"""
データベース接続モジュール

JVDデータベースへの接続と基本的なクエリ実行機能を提供します。
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()

# データベース接続情報
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'pckeiba')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')

# キャッシュディレクトリ
CACHE_DIR = os.getenv('CACHE_DIR', './data/cache')

# キャッシュディレクトリの作成
os.makedirs(CACHE_DIR, exist_ok=True)


def get_engine():
    """SQLAlchemy engineを取得"""
    conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_string)


def execute_query(query, params=None):
    """
    SQLクエリを実行し、結果をPandas DataFrameで返す
    
    Args:
        query (str): 実行するSQLクエリ
        params (dict, optional): クエリパラメータ
        
    Returns:
        pandas.DataFrame: クエリ結果のデータフレーム
    """
    engine = get_engine()
    try:
        if params:
            return pd.read_sql_query(text(query), engine, params=params)
        return pd.read_sql_query(text(query), engine)
    except Exception as e:
        logger.error(f"クエリ実行中にエラーが発生しました: {e}")
        logger.error(f"実行クエリ: {query}")
        raise


def query_with_cache(query, cache_name, params=None, force_refresh=False):
    """
    キャッシュ機能付きでSQLクエリを実行
    
    同じクエリが何度も実行される場合にデータベースへのアクセスを減らすため、
    結果をキャッシュに保存し、次回以降はキャッシュから結果を返します。
    
    Args:
        query (str): 実行するSQLクエリ
        cache_name (str): キャッシュファイル名
        params (dict, optional): クエリパラメータ
        force_refresh (bool): キャッシュを無視して強制的に再取得するかどうか
        
    Returns:
        pandas.DataFrame: クエリ結果のデータフレーム
    """
    cache_path = os.path.join(CACHE_DIR, f"{cache_name}.pkl")
    
    if not force_refresh and os.path.exists(cache_path):
        logger.info(f"キャッシュから{cache_name}を読み込み中...")
        return pd.read_pickle(cache_path)
    
    logger.info(f"データベースから{cache_name}を取得中...")
    df = execute_query(query, params)
    
    # キャッシュに保存
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    df.to_pickle(cache_path)
    
    return df


if __name__ == "__main__":
    # 接続テスト
    try:
        engine = get_engine()
        with engine.connect() as conn:
            logger.info("データベース接続に成功しました")
        
        # テストクエリ
        df = execute_query("SELECT * FROM ref_track_code LIMIT 5")
        logger.info(f"テストクエリの結果:\n{df}")
    except Exception as e:
        logger.error(f"データベース接続テスト中にエラーが発生しました: {e}")
