from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, Text, Float
from datetime import datetime
from config import settings


engine = create_async_engine(settings.sqlite_url, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id:          Mapped[int]      = mapped_column(Integer, primary_key=True)
    file_path:   Mapped[str]      = mapped_column(String(1024), unique=True)
    file_name:   Mapped[str]      = mapped_column(String(256))
    file_type:   Mapped[str]      = mapped_column(String(16))
    market:      Mapped[str]      = mapped_column(String(32), nullable=True)
    file_hash:   Mapped[str]      = mapped_column(String(64))
    chunk_count: Mapped[int]      = mapped_column(Integer, default=0)
    indexed_at:  Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    modified_at: Mapped[datetime] = mapped_column(DateTime)
    size_bytes:  Mapped[int]      = mapped_column(Integer, default=0)


class QueryLog(Base):
    __tablename__ = "query_logs"

    id:               Mapped[int]   = mapped_column(Integer, primary_key=True)
    query:            Mapped[str]   = mapped_column(Text)
    response_summary: Mapped[str]   = mapped_column(Text, nullable=True)
    market_filter:    Mapped[str]   = mapped_column(String(32), nullable=True)
    chunks_used:      Mapped[int]   = mapped_column(Integer, default=0)
    latency_ms:       Mapped[float] = mapped_column(Float, default=0.0)
    created_at:       Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session
