# backend/db.py

import os
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# IMPORTANT:
# In docker-compose, backend container should use host "db"
# In Codespaces host VM (running uvicorn directly), Postgres is reachable on localhost:5432
#
# Keep DATABASE_URL in .env to avoid confusion.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://projectm:projectm123@localhost:5432/projectm",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def db_ping() -> int:
    """Quick connectivity test."""
    with engine.connect() as conn:
        return conn.execute(text("SELECT 1")).scalar_one()


# Workspace helpers (UUID DB)
def ensure_workspace(db: Session, slug: str = "default"):
    """
    Ensures workspace exists. Returns UUID (as uuid.UUID object if models use UUID(as_uuid=True)).
    """
    # local import to avoid circular imports at import time
    from backend.models import Workspace

    s = (slug or "default").strip().lower()
    ws = db.query(Workspace).filter(Workspace.slug == s).first()
    if not ws:
        ws = Workspace(slug=s, name=("Default Workspace" if s == "default" else s.title()))
        db.add(ws)
        db.commit()
        db.refresh(ws)
    return ws.id  # uuid.UUID


def resolve_workspace_id(db: Session, slug_or_id: str | None):
    """
    Accepts:
      - None -> default workspace id
      - UUID string -> that UUID (uuid.UUID)
      - slug -> ensure & return workspace id
    """
    if not slug_or_id:
        return ensure_workspace(db, "default")

    s = str(slug_or_id).strip()

    # if already UUID -> return uuid.UUID
    try:
        return uuid.UUID(s)
    except Exception:
        return ensure_workspace(db, s)
