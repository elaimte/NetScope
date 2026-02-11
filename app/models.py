"""SQLAlchemy ORM models for the Internet Usage Monitoring Service."""

from sqlalchemy import Column, DateTime, Float, Index, Integer, String

from app.database import Base


class UsageRecord(Base):
    """Represents a single internet usage session for a user.

    Attributes:
        id: Auto-incremented primary key.
        username: The user's unique identifier/name.
        mac_address: The MAC address of the user's device.
        start_time: When the usage session started.
        usage_time_seconds: Duration of the session in seconds.
        upload_kb: Upload data consumed in Kilobits.
        download_kb: Download data consumed in Kilobits.
        total_kb: Total data consumed (upload + download) in Kilobits.
                  Stored as a denormalized field for query performance.
    """

    __tablename__ = "usage_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False)
    mac_address = Column(String(17), nullable=False)
    start_time = Column(DateTime, nullable=False)
    usage_time_seconds = Column(Integer, nullable=False)
    upload_kb = Column(Float, nullable=False)
    download_kb = Column(Float, nullable=False)
    total_kb = Column(Float, nullable=False)

    __table_args__ = (
        Index("idx_username", "username"),
        Index("idx_start_time", "start_time"),
        Index("idx_username_start_time", "username", "start_time"),
    )

    def __repr__(self):
        return (
            f"<UsageRecord(username={self.username!r}, "
            f"start_time={self.start_time}, total_kb={self.total_kb})>"
        )
