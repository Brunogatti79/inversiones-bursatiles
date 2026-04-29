from core.database import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from typing import Annotated

DBSession = Annotated[AsyncSession, Depends(get_session)]
