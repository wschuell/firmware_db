
import sqlite3


from sqlalchemy import (
    create_engine,
    text,
)

from sqlalchemy.orm import Session

from .sql_model import Base
from . import sql_model


import json
import os
import glob
import datetime
from itertools import islice

import subprocess
import socket
import re

def chunked(iterable, size):
    it = iter(iterable)
    while (batch := list(islice(it, size))):
        yield batch


class Importer:
    def __init__(self,query,chunk_size=10**4,param_processor=None,autocommit=True,param_split=False):
        self.chunk_size = chunk_size
        self.query = query
        self.param_processor = param_processor
        self.param_split = param_split
        self.autocommit = autocommit

    def import_all(self,engine,params):
        with engine.connect() as conn:
            param_iter = iter(params)
            while (chunk := list(islice(param_iter, self.chunk_size))):
                if self.param_processor is not None:
                    if self.param_split:
                        chunk_tmp = []
                        for p in chunk:
                            chunk_tmp += self.param_processor(p)
                        chunk = chunk_tmp
                    else:
                        chunk = [self.param_processor(p) for p in chunk]
                self.import_chunk(conn=conn,params=chunk)
            if self.autocommit:
                conn.commit()

    def import_chunk(self,conn,params):
        if isinstance(self.query,str):
            conn.execute(
                text(self.query),
                params,
            )
        elif isinstance(self.query,list):
            for q in self.query:
                conn.execute(
                    text(q),
                    params,
                )
        else:
            msg = f"Unsupported query type:{type(self.query)}"
            raise NotImplementedError(msg)



class Firmware_db:
    """Wrapper class."""


    sql_base = Base

    def __init__(
        self,
        engine_url="sqlite:///firmware_db.db",
        engine=None,
        **kwargs,
    ):
        self.engine_url = engine_url
        if engine is not None:
            self.engine=engine
        else:
            self.init_engine()
        self.sql_base.metadata.create_all(self.engine,checkfirst=True)

    def init_engine(self )-> None:
        """Initiating the SQLAlchemy engine if not existing."""
        if not hasattr(self, "engine"):
            if self.engine_url.startswith("sqlite:"):
                connect_args = {
                    "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                }
            else:
                connect_args = {}
            self.engine_cfg = {
                "url": self.engine_url, "connect_args": connect_args,
            }
            self.engine = create_engine(**self.engine_cfg)

    def drop_all(self)-> None:
        """Clean slate for the database."""
        self.sql_base.metadata.drop_all(self.engine)

    @staticmethod
    def _generate_fw_id(vendor, model, version):
        """Creates a deterministic ID for the catalog."""
        return f"{vendor}_{model}_{version}".replace(" ", "_").lower()

    def extract_hostname(self, filename):
        """Safely extracts hostname from YYYY-MM-DD_hostname_fwupd.json"""
        match = re.match(r'\d{4}-\d{2}-\d{2}_(.*)_fwupd\.json', filename)
        if match:
            return match.group(1)
        return filename.split('_')[0] # Fallback if format doesn't match

    def process_directory(self, json_directory):
        """Iterates through all JSON files in the directory."""
        for filepath in glob.glob(os.path.join(json_directory, "*.json")):
            self.process_file(filepath)

    def process_file(self, filepath):
        """Reads a single JSON file and UPSERTs the data using a session context."""
        filename = os.path.basename(filepath)
        hostname = self.extract_hostname(filename)

        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Skipping {filename}: Invalid JSON")
            return

        # Use the context manager for the session
        # This automatically handles closing the session and rolling back on errors
        with Session(self.engine) as session:
            try:
                # 1. Update/Create sql_model.Machine
                machine = session.query(sql_model.Machine).filter_by(machine_id=hostname).first()
                if not machine:
                    machine = sql_model.Machine(machine_id=hostname)
                    session.add(machine)
                machine.last_updated =  datetime.datetime.now(datetime.UTC)

                # 2. Parse Devices
                devices_data = data.get('Devices', [])
                for dev in devices_data:
                    if not dev.get('Version') or dev.get('Plugin') == 'cpu':
                        continue

                    vendor = dev.get('Vendor', 'Unknown')
                    model = dev.get('Name', 'Unknown')
                    version = dev.get('Version')
                    dev_id = dev.get('DeviceId')

                    # Grab the ReleaseDate (Ansible uses the key we just made, fwupdmgr uses 'Created' or similar sometimes, 
                    # but we'll stick to our explicitly mapped 'ReleaseDate' first)
                    release_date = dev.get('ReleaseDate') or dev.get('Created')

                    fw_id = self._generate_fw_id(vendor, model, version)

                    # 3. UPSERT Catalog
                    fw_entry = session.query(sql_model.FirmwareCatalog).filter_by(id=fw_id).first()
                    if not fw_entry:
                        fw_entry = sql_model.FirmwareCatalog(
                            id=fw_id,
                            vendor=vendor,
                            model=model,
                            version_string=version,
                            release_date=release_date  # <--- Added this line
                        )
                        session.add(fw_entry)

                    # 4. UPSERT Telemetry
                    telemetry = session.query(sql_model.Device).filter_by(device_id=dev_id).first()
                    if not telemetry:
                        telemetry = sql_model.Device(
                            device_id=dev_id,
                            machine_id=hostname,
                            device_type=dev.get('Flags', ['Unknown'])[0],
                            vendor=vendor,
                            model=model,
                            current_firmware_id=fw_id
                        )
                        session.add(telemetry)
                    else:
                        telemetry.current_firmware_id = fw_id
                        telemetry.machine_id = hostname

                # Commit only if everything in this file was processed successfully
                session.commit()
                print(f"Successfully ingested data for {hostname}")

            except Exception as e:
                # The context manager automatically rolls back, but we can log the error
                session.rollback()
                print(f"Database error processing {filename}: {e}")
                raise
    
    def generate_local_json(self, output_dir):
        """Runs fwupdmgr, appends local NVMe/BIOS data, and saves unified JSON."""
        hostname = socket.gethostname()
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        filename = f"{date_str}_{hostname}_local_fwupd.json"
        filepath = os.path.join(output_dir, filename)

        master_payload = {"Devices": []}

        # 1. Get fwupdmgr data
        try:
            result = subprocess.run(["fwupdmgr", "get-devices", "--json"], capture_output=True, text=True, check=True)
            fwupd_data = json.loads(result.stdout)
            master_payload["Devices"].extend(fwupd_data.get("Devices", []))
        except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
            print(f"fwupdmgr failed or not found, skipping base devices: {e}")

        # 2. Append local NVMe data
        nvme_data = self.get_local_nvme_info()
        if nvme_data:
            master_payload["Devices"].extend(nvme_data)

        # 3. Append local BIOS/Motherboard data (assuming you added these methods)
        bios_data = self.get_local_bios_info()
        if bios_data:
            master_payload["Devices"].append(bios_data)
            
        board_data = self.get_local_motherboard_info()
        if board_data:
            master_payload["Devices"].append(board_data)

        # 4. Save the unified JSON
        try:
            os.makedirs(output_dir, exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(master_payload, f, indent=4)
            print(f"Successfully generated unified local JSON: {filepath}")
        except Exception as e:
            print(f"Failed to write JSON file: {e}")
            raise

    def get_local_bios_info(self):
        """Reads local BIOS info and formats it to match fwupdmgr schema."""
        try:
            with open('/sys/class/dmi/id/sys_vendor', 'r') as f:
                vendor = f.read().strip()
            with open('/sys/class/dmi/id/bios_version', 'r') as f:
                version = f.read().strip()
            with open('/sys/class/dmi/id/bios_date', 'r') as f:
                release_date = f.read().strip()
            
            # Try to get UUID for a unique DeviceId, fallback if unavailable
            try:
                with open('/sys/class/dmi/id/product_uuid', 'r') as f:
                    uuid = f.read().strip()
            except FileNotFoundError:
                uuid = "unknown"
            
            return {
                "Vendor": vendor,
                "Name": "System BIOS",
                "Version": version,
                "ReleaseDate": release_date,
                "DeviceId": f"bios_{uuid}",
                "Flags": ["BIOS"]
            }
        except PermissionError:
            print("Warning: Permission denied reading Motherboard DMI data. Run script with sudo.")
            return None
        except FileNotFoundError:
            return None

    def get_local_motherboard_info(self):
        """Reads local motherboard info and formats it to match fwupdmgr schema."""
        try:
            with open('/sys/class/dmi/id/board_vendor', 'r') as f:
                vendor = f.read().strip()
            with open('/sys/class/dmi/id/board_name', 'r') as f:
                name = f.read().strip()
            with open('/sys/class/dmi/id/board_version', 'r') as f:
                version = f.read().strip()
                
            # Try to get UUID for a unique DeviceId
            try:
                with open('/sys/class/dmi/id/product_uuid', 'r') as f:
                    uuid = f.read().strip()
            except FileNotFoundError:
                uuid = "unknown"
            
            return {
                "Vendor": vendor,
                "Name": name,
                "Version": version,
                "DeviceId": f"board_{uuid}",
                "Flags": ["Motherboard"]
            }
        except PermissionError:
            print("Warning: Permission denied reading Motherboard DMI data. Run script with sudo.")
            return None
        except FileNotFoundError:
            return None
    def get_local_nvme_info(self):
        """Runs nvme-cli locally to get NVMe drive information."""

        nvme_devices = []
        try:
            # Run the command and capture the output
            result = subprocess.run(
                ["nvme", "list", "-o", "json"], 
                capture_output=True, 
                text=True, 
                check=True
            )
            
            # Parse the JSON output
            data = json.loads(result.stdout)
            devices = data.get('Devices', [])
            
            # Map the nvme-cli fields to our standardized schema
            for item in devices:
                nvme_devices.append({
                    "Vendor": "NVMe", 
                    "Name": item.get("ModelNumber", "Unknown").strip(),
                    "Version": item.get("Firmware", "Unknown").strip(),
                    "DeviceId": item.get("SerialNumber", "Unknown").strip(),
                    "Flags": ["NVMe"]
                })
                
        except subprocess.CalledProcessError as e:
            # nvme-cli exits with a non-zero code if it fails (e.g., needs sudo)
            print(f"nvme-cli failed (are you running as root?): {e.stderr.strip()}")
        except FileNotFoundError:
            print("nvme-cli command not found. Ensure the nvme-cli package is installed.")
        except json.JSONDecodeError:
            print("Failed to parse nvme-cli output as JSON.")
        except Exception as e:
            print(f"An unexpected error occurred while gathering NVMe info: {e}")

        return nvme_devices