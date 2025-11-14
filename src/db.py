from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.config import Cfg

def make_engine() -> AsyncEngine:
    url = (
        f"mysql+aiomysql://{Cfg.DB_USER}:{Cfg.DB_PASS}"
        f"@{Cfg.DB_HOST}:{Cfg.DB_PORT}/{Cfg.DB_NAME}"
        "?charset=utf8mb4"
    )
    return create_async_engine(
        url,
        pool_size=Cfg.DB_POOL,
        max_overflow=Cfg.DB_OVER,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )

engine = make_engine()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
