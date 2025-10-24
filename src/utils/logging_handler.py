import logging
import sys
from pythonjsonlogger import jsonlogger

def get_logger(name: str):
    """構造化JSONロガーを取得する"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 既にハンドラが設定されている場合は追加しない (Functionの再利用対策)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger
