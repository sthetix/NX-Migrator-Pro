"""
Cleanup Engine - Remove unwanted partitions and expand FAT32 on single SD card
"""

import time
import logging
import subprocess
import tempfile
import os
from pathlib import Path
from typing import Callable, Optional
from core.disk_manager import DiskManager
from core.partition_writer import PartitionWriter
from core.partition_models import DiskLayout

logger = logging.getLogger(__name__)

SECTOR_SIZE = 512

class CleanupEngine:
    """Handles the cleanup process for a single SD card"""

    def __init__(self, disk, source_layout: DiskLayout, target_layout: DiskLayout, options: dict):
        self.disk = disk
        self.source_layout = source_layout
        self.target_layout = target_layout
        self.options = options

        self.disk_manager = DiskManager()
        self.partition_writer = PartitionWriter(self.disk_manager)

        # Callbacks
        self.on_progress: Optional[Callable] = None
        self.on_complete: Optional[Callable] = None
        self.on_error: Optional[Callable] = None

        self.cancelled = False

    def run(self):
        """Execute cleanup operation"""
        # Initialize COM for this thread (needed for WMI operations)
        import pythoncom
        pythoncom.CoInitialize()

        try:
            self._report_progress("Initializing", 0, "Preparing cleanup...")

            # Stage 1: Backup FAT32 data to temporary location
            self._backup_fat32_data()

            # Stage 2: Clean disk (delete all partitions)
            self._clean_disk()

            # Stage 3: Write new partition table
            self._write_partition_tables()

            # Stage 4: Create FAT32 filesystem
            self._create_fat32_filesystem()

            # Stage 5: Restore FAT32 data
            self._restore_fat32_data()

            # Stage 6: Update emuMMC config if emuMMC is preserved
            if not self.options.get('remove_emummc', False) and self.source_layout.has_emummc:
                self._update_emummc_config()

            # Complete
            if self.on_complete:
                self.on_complete()

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            if self.on_error:
                self.on_error(str(e))
        finally:
            # Cleanup temp directory if it exists
            if hasattr(self, 'temp_backup_dir') and os.path.exists(self.temp_backup_dir):
                try:
                    import shutil
                    shutil.rmtree(self.temp_backup_dir)
                    logger.info(f"Cleaned up temporary backup directory: {self.temp_backup_dir}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp directory: {e}")

            # Uninitialize COM when done
            pythoncom.CoUninitialize()

    def _backup_fat32_data(self):
        """Backup FAT32 data to temporary location"""
        self._report_progress("Backing up FAT32", 5, "Creating temporary backup of FAT32 data...")

        # Get FAT32 partition from source layout
        fat32_part = None
        for part in self.source_layout.partitions:
            if part.category == 'FAT32':
                fat32_part = part
                break

        if not fat32_part:
            raise Exception("No FAT32 partition found!")

        # Get drive letter for FAT32 partition
        drive_letter = self._get_drive_letter_for_partition(fat32_part.start_sector)

        if not drive_letter:
            raise Exception("FAT32 partition not mounted - cannot backup data")

        logger.info(f"FAT32 is mounted as {drive_letter}")

        # Create temporary backup directory
        self.temp_backup_dir = tempfile.mkdtemp(prefix="nx_partition_backup_")
        logger.info(f"Created temporary backup directory: {self.temp_backup_dir}")

        self._report_progress("Backing up FAT32", 10, f"Backing up from {drive_letter} to temp folder...")

        # Use robocopy to backup FAT32 data
        self._copy_files_robocopy(
            drive_letter,
            self.temp_backup_dir,
            "Backing up FAT32",
            10
        )

        self._report_progress("Backing up FAT32", 35, "FAT32 data backed up successfully")

    def _clean_disk(self):
        """Clean the disk (delete all partitions)"""
        self._report_progress("Cleaning Disk", 40, "Deleting all partitions...")

        if not self.disk_manager.clean_disk(self.disk['path']):
            raise Exception("Failed to clean disk. Please manually delete partitions in Disk Management.")

        # Wait for Windows to release the disk
        self._report_progress("Cleaning Disk", 43, "Waiting for Windows to release disk...")
        logger.info("Waiting 3 seconds for Windows to release disk...")
        time.sleep(3)

        # Additional refresh
        logger.info("Performing additional disk refresh...")
        self.disk_manager._prepare_disk_for_write(self.disk['path'])
        time.sleep(1)

        self._report_progress("Cleaning Disk", 45, "Disk cleaned successfully")

    def _write_partition_tables(self):
        """Write new partition table"""
        self._report_progress("Writing Partition Table", 50, "Creating new partition layout...")

        self.partition_writer.write_partition_table(
            self.disk['path'],
            self.target_layout
        )

        # Refresh disk to make new partitions visible
        logger.info("Refreshing disk to make partitions visible...")
        self._refresh_disk_partitions(self.disk['path'])
        time.sleep(2)

        self._report_progress("Writing Partition Table", 55, "Partition table written")

    def _create_fat32_filesystem(self):
        """Create FAT32 filesystem using fat32format.exe"""
        self._report_progress("Creating FAT32", 60, "Formatting FAT32 partition...")

        # Get FAT32 partition from target layout
        fat32_part = None
        for part in self.target_layout.partitions:
            if part.category == 'FAT32':
                fat32_part = part
                break

        if not fat32_part:
            raise Exception("No FAT32 partition in target layout!")

        logger.info("Waiting for Windows to recognize new partitions...")
        time.sleep(3)

        logger.info("Refreshing disk before formatting...")
        self._refresh_disk_partitions(self.disk['path'])
        time.sleep(2)

        logger.info("Formatting FAT32 partition with fat32format.exe...")

        # Calculate optimal cluster size (128 sectors = 64KB)
        sectors_per_cluster = 128

        # Get the tool path
        tool_dir = Path(__file__).parent.parent / "tool"
        fat32format_exe = tool_dir / "fat32format.exe"

        if not fat32format_exe.exists():
            raise FileNotFoundError(f"fat32format.exe not found at {fat32format_exe}")

        # Assign and lock drive letter
        logger.info("Assigning and locking drive letter for FAT32 partition...")
        self.fat32_drive = self._assign_and_lock_drive_letter(fat32_part)

        logger.info(f"FAT32 partition locked to drive letter: {self.fat32_drive}")

        # Run fat32format.exe
        format_cmd = [
            str(fat32format_exe),
            f"-c{sectors_per_cluster}",
            f"{self.fat32_drive}"
        ]

        logger.info(f"Running format command: {' '.join(format_cmd)}")

        result = subprocess.run(
            format_cmd,
            input="Y\n",  # Auto-confirm
            capture_output=True,
            text=True,
            timeout=300
        )

        logger.info(f"Format command output:\n{result.stdout}")

        if result.returncode != 0:
            logger.error(f"Format failed: {result.stderr}")
            raise RuntimeError(f"FAT32 format failed: {result.stderr}")

        logger.info("FAT32 filesystem created successfully")
        self._report_progress("Creating FAT32", 70, "FAT32 partition formatted")

    def _restore_fat32_data(self):
        """Restore FAT32 data from temporary backup"""
        self._report_progress("Restoring FAT32", 75, "Restoring FAT32 data...")

        if not hasattr(self, 'temp_backup_dir') or not os.path.exists(self.temp_backup_dir):
            raise Exception("Temporary backup directory not found!")

        if not hasattr(self, 'fat32_drive') or not self.fat32_drive:
            raise Exception("FAT32 drive letter not available!")

        logger.info(f"Restoring FAT32 data from {self.temp_backup_dir} to {self.fat32_drive}")

        # Use robocopy to restore FAT32 data
        self._copy_files_robocopy(
            self.temp_backup_dir,
            self.fat32_drive,
            "Restoring FAT32",
            75
        )

        self._report_progress("Restoring FAT32", 90, "FAT32 data restored successfully")

        # Clean up bootloader ini files for removed partitions
        self._cleanup_bootloader_ini_files()

        self._report_progress("Restoring FAT32", 95, "Cleanup complete")

    def _cleanup_bootloader_ini_files(self):
        """Remove bootloader ini files for deleted partitions"""
        try:
            if not hasattr(self, 'fat32_drive') or not self.fat32_drive:
                logger.warning("FAT32 drive letter not available - cannot cleanup bootloader ini files")
                return

            bootloader_ini_path = Path(self.fat32_drive) / "bootloader" / "ini"

            if not bootloader_ini_path.exists():
                logger.info("bootloader/ini directory does not exist - nothing to cleanup")
                return

            logger.info(f"Checking bootloader ini files in {bootloader_ini_path}")

            # Determine which ini files to remove based on cleanup options
            ini_files_to_remove = []

            # Remove Android ini files if Android partition was deleted
            if self.options.get('remove_android', False):
                # Look for android-related ini files
                android_patterns = ['android.ini', '*android*.ini']
                for pattern in android_patterns:
                    for ini_file in bootloader_ini_path.glob(pattern):
                        if ini_file.is_file():
                            ini_files_to_remove.append(ini_file)
                            logger.info(f"Found Android ini file to remove: {ini_file.name}")

            # Remove Linux ini files if Linux partition was deleted
            if self.options.get('remove_linux', False):
                # Look for linux-related ini files (L4T variants and Lakka)
                linux_patterns = ['L4T*.ini', 'lakka.ini']
                for pattern in linux_patterns:
                    for ini_file in bootloader_ini_path.glob(pattern):
                        if ini_file.is_file() and ini_file not in ini_files_to_remove:
                            ini_files_to_remove.append(ini_file)
                            logger.info(f"Found Linux ini file to remove: {ini_file.name}")

            # Remove the identified ini files
            if ini_files_to_remove:
                logger.info(f"Removing {len(ini_files_to_remove)} bootloader ini file(s)...")
                for ini_file in ini_files_to_remove:
                    try:
                        ini_file.unlink()
                        logger.info(f"Removed: {ini_file.name}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {ini_file.name}: {e}")

                logger.info("Bootloader ini files cleanup completed")
            else:
                logger.info("No bootloader ini files to remove")

        except Exception as e:
            logger.error(f"Error cleaning up bootloader ini files: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't raise - this is not critical enough to fail the entire cleanup

    def _update_emummc_config(self):
        """Update emuMMC configuration if emuMMC is preserved"""
        self._report_progress("Updating emuMMC", 97, "Updating emuMMC configuration...")

        # Get emuMMC partitions
        target_emummc = self.target_layout.get_emummc_partitions()

        if not target_emummc:
            logger.info("No emuMMC partitions to update")
            return

        try:
            # Similar logic to migration_engine._update_emummc_config
            # Create/update emuMMC configuration on FAT32

            if not hasattr(self, 'fat32_drive') or not self.fat32_drive:
                logger.warning("FAT32 drive letter not available - cannot update emuMMC config")
                return

            base_path = Path(self.fat32_drive + "\\")
            emummc_path = base_path / "emuMMC"
            emummc_path.mkdir(exist_ok=True)

            # Calculate emuMMC sector offset
            target_emummc_gpt_start = target_emummc[0].start_sector
            EMUMMC_INI_OFFSET = 0x17000
            emummc_ini_sector = target_emummc_gpt_start + EMUMMC_INI_OFFSET

            logger.info(f"emuMMC GPT start: 0x{target_emummc_gpt_start:X}")
            logger.info(f"emuMMC ini sector: 0x{emummc_ini_sector:X}")

            # Create RAW folder
            raw_folder_name = "RAW1"
            raw_folder_path = emummc_path / raw_folder_name
            raw_folder_path.mkdir(exist_ok=True)

            # Create raw_based file
            raw_based_file = raw_folder_path / "raw_based"
            with open(raw_based_file, 'wb') as f:
                f.write(emummc_ini_sector.to_bytes(4, byteorder='little'))

            logger.info(f"Created raw_based file with sector: 0x{emummc_ini_sector:x}")

            # Create emummc.ini
            folder_id = int.from_bytes(raw_folder_name.encode('ascii')[:4].ljust(4, b'\x00'), byteorder='little')
            emummc_ini_path = emummc_path / "emummc.ini"

            ini_content = (
                "[emummc]\n"
                "enabled=1\n"
                f"sector=0x{emummc_ini_sector:X}\n"
                f"id=0x{folder_id:X}\n"
                f"path=emuMMC/{raw_folder_name}\n"
                f"nintendo_path=emuMMC/{raw_folder_name}/Nintendo\n"
            )

            with open(emummc_ini_path, 'w', encoding='utf-8') as f:
                f.write(ini_content)

            logger.info(f"Created emummc.ini successfully")
            self._report_progress("Updating emuMMC", 99, "emuMMC config updated")

        except Exception as e:
            logger.error(f"Error updating emuMMC config: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _copy_files_robocopy(self, source, target, stage_name, base_progress):
        """Copy files using robocopy"""
        source = source.rstrip('\\')
        target = target.rstrip('\\')

        logger.info(f"Starting robocopy: {source} -> {target}")

        cmd = [
            'robocopy',
            source,
            target,
            '/E',           # Copy subdirectories including empty
            '/COPY:DAT',    # Copy data, attributes, timestamps
            '/DCOPY:DAT',   # Copy directory timestamps
            '/R:2',         # Retry 2 times
            '/W:1',         # Wait 1 second between retries
            '/NP',          # No progress percentage per file
            '/NDL',         # No directory listing
            '/TEE',         # Output to console and log
            '/MT:8'         # Multi-threaded (8 threads)
        ]

        logger.info(f"Robocopy command: {' '.join(cmd)}")
        self._report_progress(stage_name, base_progress, "Copying files with robocopy...")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,
                encoding='utf-8',
                errors='replace'
            )

            # Robocopy return codes: 0-7 are success, 8+ are errors
            if result.returncode >= 8:
                logger.error(f"Robocopy failed with return code {result.returncode}")
                logger.error(f"Output: {result.stdout}")
                logger.error(f"Errors: {result.stderr}")
                raise Exception(f"File copy failed with robocopy error code {result.returncode}")

            logger.info(f"Robocopy completed with return code {result.returncode}")
            logger.info(f"Output: {result.stdout}")

        except subprocess.TimeoutExpired:
            logger.error("Robocopy timed out")
            raise Exception("File copy timed out")
        except Exception as e:
            logger.error(f"Robocopy error: {e}")
            raise

    def _get_drive_letter_for_partition(self, start_sector):
        """Get drive letter for a partition at a specific sector"""
        disk_index = self.disk['path'].replace("\\\\.\\PhysicalDrive", "")

        try:
            # Create a new WMI connection for this thread
            import wmi
            wmi_conn = wmi.WMI()

            partitions = wmi_conn.query(
                f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
            )

            for partition in partitions:
                part_start = int(partition.StartingOffset) // SECTOR_SIZE

                if abs(part_start - start_sector) < 2048:  # Within 1MB tolerance
                    # Get associated logical disk
                    logical_disks = wmi_conn.query(
                        f"ASSOCIATORS OF {{Win32_DiskPartition.DeviceID='{partition.DeviceID}'}} "
                        f"WHERE AssocClass=Win32_LogicalDiskToPartition"
                    )

                    if logical_disks:
                        drive_letter = logical_disks[0].DeviceID
                        logger.info(f"Found drive letter: {drive_letter}")
                        return drive_letter

        except Exception as e:
            logger.error(f"Error finding drive letter: {e}")

        return None

    def _assign_and_lock_drive_letter(self, partition):
        """Assign and lock a drive letter for a partition"""
        disk_index = self.disk['path'].replace("\\\\.\\PhysicalDrive", "")

        # Find partition number
        partition_num = self._find_partition_number(partition.start_sector)

        if partition_num is None:
            raise RuntimeError(f"Could not find partition at sector {partition.start_sector}")

        # Assign drive letter
        diskpart_script = f"""select disk {disk_index}
select partition {partition_num}
assign
"""

        result = subprocess.run(
            ['diskpart'],
            input=diskpart_script,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0 and "already assigned" not in result.stdout.lower():
            logger.warning(f"Diskpart assign returned: {result.stderr}")

        time.sleep(2)

        # Get the actual drive letter
        drive_letter = self._get_drive_letter_for_partition(partition.start_sector)

        if not drive_letter:
            raise RuntimeError(f"Failed to get drive letter for partition")

        logger.info(f"Partition locked to drive letter: {drive_letter}")
        return drive_letter

    def _find_partition_number(self, start_sector):
        """Find partition number for a partition at a specific sector"""
        disk_index = self.disk['path'].replace("\\\\.\\PhysicalDrive", "")
        MAX_RETRIES = 10
        RETRY_DELAY = 2

        for attempt in range(MAX_RETRIES):
            try:
                # Create a new WMI connection for this thread
                import wmi
                wmi_conn = wmi.WMI()

                partitions = wmi_conn.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                for part in partitions:
                    part_start = int(part.StartingOffset) // SECTOR_SIZE

                    if abs(part_start - start_sector) < 2048:
                        logger.info(f"Found matching partition number: {part.Index}")
                        return int(part.Index)

                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Partition not found, refreshing...")
                    diskpart_script = f"select disk {disk_index}\nrescan\n"
                    subprocess.run(['diskpart'], input=diskpart_script, capture_output=True, text=True, timeout=30)
                    time.sleep(RETRY_DELAY)

            except Exception as e:
                logger.warning(f"Error finding partition: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        return None

    def _refresh_disk_partitions(self, disk_path):
        """Refresh disk to make new partitions visible"""
        try:
            disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")

            diskpart_script = f"""select disk {disk_index}
rescan
"""

            subprocess.run(
                ['diskpart'],
                input=diskpart_script,
                capture_output=True,
                text=True,
                timeout=10
            )

            logger.info("Disk partitions refreshed")

        except Exception as e:
            logger.warning(f"Could not refresh disk partitions: {e}")

    def _report_progress(self, stage: str, percent: float, message: str):
        """Report progress to callback"""
        if self.on_progress:
            self.on_progress(stage, percent, message)

    def cancel(self):
        """Cancel cleanup operation"""
        self.cancelled = True
