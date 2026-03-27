from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    ForeignKey,
    UniqueConstraint,
    func,
    Float,
    Boolean,
    Time,
    DateTime,
    func,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

Base = declarative_base()

class Machine(Base):
    """Table 1: The physical host"""
    __tablename__ = 'fleet_inventory'

    machine_id = Column(String, primary_key=True) # e.g., Hostname or FQDN
    os_version = Column(String)
    last_updated = Column(DateTime, default=func.current_timestamp())

    # Relationship back to devices
    devices = relationship("Device", back_populates="machine")

class FirmwareCatalog(Base):
    """Table 2: The dictionary of all known firmwares"""
    __tablename__ = 'firmware_catalog'

    id = Column(String, primary_key=True) # Hash of vendor+model+version for easy referencing
    vendor = Column(String, nullable=False)
    model = Column(String, nullable=False)
    version_string = Column(String, nullable=False)
    release_date = Column(String)

    __table_args__ = (
        UniqueConstraint('vendor', 'model', 'version_string', name='_vendor_model_version_uc'),
    )

class Device(Base):
    """Table 3: The actual hardware found inside the machines"""
    __tablename__ = 'device'

    device_id = Column(String, primary_key=True) # e.g., WWN, MAC, or PCI path
    machine_id = Column(String, ForeignKey('fleet_inventory.machine_id'))
    device_type = Column(String) # 'NVMe', 'BIOS', etc.
    vendor = Column(String, nullable=False)
    model = Column(String, nullable=False)
    current_firmware_id = Column(String, ForeignKey('firmware_catalog.id'))

    machine = relationship("Machine", back_populates="devices")
    firmware = relationship("FirmwareCatalog")

class FirmwareBaseline(Base):
    """Table 4: Your desired state"""
    __tablename__ = 'firmware_baselines'

    # Composite Primary Key!
    vendor = Column(String, primary_key=True)
    model = Column(String, primary_key=True)
    approved_firmware_id = Column(String, ForeignKey('firmware_catalog.id'))

    firmware = relationship("FirmwareCatalog")