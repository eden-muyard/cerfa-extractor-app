from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from database import Base


class LabelModel(Base):
    __tablename__ = "labels"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(120), unique=True, nullable=False, index=True)
    label = Column(String(255), nullable=False)
    patterns_json = Column(Text, nullable=False)
    source_tabs_json = Column(Text, nullable=False, default="[]")
    value_type = Column(String(64), nullable=False, default="text")
    required = Column(Boolean, nullable=False, default=False)
    sort_order = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ExtractionModel(Base):
    __tablename__ = "extractions"

    id = Column(Integer, primary_key=True, index=True)
    source_file = Column(String(500), nullable=False)
    extracted_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
