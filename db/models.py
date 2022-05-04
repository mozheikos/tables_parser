from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Float, Boolean

Base = declarative_base()


class Account(Base):
    __tablename__ = "mainapp_account"

    id = Column(Integer, primary_key=True)
    address = Column(String(255), nullable=False)
    type = Column(String(255))
    name = Column(String(255))
    risk_score = Column(Float, default=0.0)
    balance = Column(Float, default=0.0)
    creator_addr = Column(String(255))
    deployed_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)
    created_at = Column(Date)
    loaded = Column(Boolean, default=True)


class Link(Base):
    __tablename__ = "mainapp_link"

    id = Column(Integer, primary_key=True)
    src_id = Column(Integer, nullable=False)
    dst_id = Column(Integer, nullable=False)


class Transaction(Base):
    __tablename__ = "mainapp_transaction"

    id = Column(Integer, primary_key=True)
    balance_delta = Column(Float)
    created_At = Column(Date)
    link_id = Column(Integer, nullable=False)


class Tip3(Base):
    __tablename__ = "mainapp_tip3"

    id = Column(Integer, primary_key=True)
    token = Column(String(255))
    amount = Column(Float)
    account_id = Column(String(255))
