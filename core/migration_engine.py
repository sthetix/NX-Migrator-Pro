"""
Migration Engine - Copy partitions from source to target
"""

import time
import threading
import queue
import psutil
import shutil
import logging
import subprocess
import tempfile
import struct
import re
import os
from pathlib import Path
from typing import Callable, Optional
from core.disk_manager import DiskManager
from core.partition_writer import PartitionWriter
from core.partition_models import DiskLayout

logger = logging.getLogger(__name__)

SECTOR_SIZE = 512

# Auto-detect optimal chunk size based on available RAM
def _get_optimal_chunk_size():
    """Determine optimal chunk size based on available system RAM"""
    try:
        available_ram = psutil.virtual_memory().available
        if available_ram > 8 * 1024**3:  # 8GB+
            return 128 * 1024 * 1024, 3  # 128MB, triple buffering
        elif available_ram > 4 * 1024**3:  # 4GB+
            return 64 * 1024 * 1024, 2   # 64MB, double buffering
        else:  # Low memory
            return 32 * 1024 * 1024, 1   # 32MB, single buffer (no threading)
    except:
        return 32 * 1024 * 1024, 1  # Safe fallback

CHUNK_SIZE, NUM_BUFFERS = _get_optimal_chunk_size()

class MigrationEngine:
    """Handles the complete migration process"""

    def __init__(self, source_disk, target_disk, source_layout: DiskLayout,
                 target_layout: DiskLayout, options: dict):
        self.source_disk = source_disk
        self.target_disk = target_disk
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
        """Execute migration"""
        # Initialize COM for this thread (needed for WMI operations)
        import pythoncom
        pythoncom.CoInitialize()

        try:
            self._report_progress("Initializing", 0, "Preparing target disk...")

            # Stage 0: Clean target disk (delete all partitions)
            # This is CRUCIAL - it releases all Windows locks on the disk
            self._report_progress("Cleaning Disk", 1, "Deleting all partitions on target disk...")
            if not self.disk_manager.clean_disk(self.target_disk['path']):
                raise Exception("Failed to clean target disk. Please manually delete partitions in Disk Management.")

            # Wait for Windows to fully release the disk after cleaning
            self._report_progress("Cleaning Disk", 3, "Waiting for Windows to release disk...")
            logger.info("Waiting 3 seconds for Windows to release disk after clean...")
            time.sleep(3)

            # Additional force refresh to ensure disk is fully released
            logger.info("Performing additional disk refresh after clean...")
            self.disk_manager._prepare_disk_for_write(self.target_disk['path'])
            time.sleep(1)

            # Stage 1: Clear target disk headers
            self._clear_target_disk()

            # Stage 2: Write partition tables first (required for fat32format.exe to work)
            self._write_partition_tables()

            # Stage 3: Create FAT32 filesystem using fat32format.exe
            # This must happen AFTER partition table is written so Windows can mount the partition
            self._report_progress("Preparing FAT32", 8, "Creating FAT32 filesystem...")
            fat32_part = None
            for part in self.target_layout.partitions:
                if part.category == 'FAT32':
                    fat32_part = part
                    break
            if fat32_part:
                logger.info("Waiting for Windows to recognize new partitions...")
                time.sleep(3)  # Give Windows extra time to recognize partitions
                logger.info("Refreshing disk before formatting...")
                self._refresh_disk_partitions(self.target_disk['path'])
                time.sleep(2)
                logger.info("Formatting FAT32 partition with fat32format.exe...")
                self._create_fat32_filesystem(fat32_part)

            # Stage 4: Copy partition data
            self._copy_partitions()

            # Stage 5: Update emuMMC configuration (if needed)
            if self.options['migrate_emummc'] and self.source_layout.has_emummc:
                self._update_emummc_config()

            # Note: Linux (L4T) boot configuration does not need updating
            # L4T uses filesystem labels (e.g., LABEL=SWR-NOB) which are preserved
            # during sector-by-sector copy, so it will boot correctly on the new SD card

            # Complete
            if self.on_complete:
                self.on_complete()

        except Exception as e:
            if self.on_error:
                self.on_error(str(e))
        finally:
            # Uninitialize COM when done
            pythoncom.CoUninitialize()

    def _clear_target_disk(self):
        """Clear first 16MB of target disk (MBR, GPT, etc.)"""
        self._report_progress("Preparing Disk", 5, "Clearing target disk headers...")

        # Prepare disk once at the beginning
        self.disk_manager._prepare_disk_for_write(self.target_disk['path'])

        # Clear first 16MB (0x8000 sectors) in large chunks for speed
        CLEAR_CHUNK_SECTORS = CHUNK_SIZE // SECTOR_SIZE  # Use same chunk size as data copy
        total_sectors = 0x8000  # 16MB
        zeros = b'\x00' * CHUNK_SIZE

        sectors_cleared = 0
        while sectors_cleared < total_sectors:
            remaining = total_sectors - sectors_cleared
            sectors_to_clear = min(CLEAR_CHUNK_SECTORS, remaining)

            # Write chunk of zeros (only allocate needed size for last chunk)
            if sectors_to_clear < CLEAR_CHUNK_SECTORS:
                chunk_zeros = b'\x00' * (sectors_to_clear * SECTOR_SIZE)
            else:
                chunk_zeros = zeros

            # Skip prepare since we did it once at the beginning
            self.disk_manager.write_sectors(self.target_disk['path'], sectors_cleared, chunk_zeros, skip_prepare=True)
            sectors_cleared += sectors_to_clear

            # Update progress less frequently
            percent = 5 + (sectors_cleared / total_sectors) * 5
            self._report_progress("Preparing Disk", percent, f"Clearing headers ({sectors_cleared}/{total_sectors} sectors)...")

    def _copy_partitions(self):
        """Copy partition data from source to target"""
        total_partitions = len(self.source_layout.partitions)
        base_progress = 15  # After partition table writing (10-15%)

        # Prepare disk ONCE before copying all partitions (skip the slow diskpart operations per partition)
        self._report_progress("Preparing Disk", base_progress, "Preparing target disk for data copy...")
        self.disk_manager._prepare_disk_for_write(self.target_disk['path'])

        for idx, source_part in enumerate(self.source_layout.partitions):
            # Find corresponding target partition
            target_part = None
            for tpart in self.target_layout.partitions:
                if tpart.name == source_part.name:
                    target_part = tpart
                    break

            if not target_part:
                continue  # Skip partitions not being migrated

            # Check if we should migrate this partition
            if not self._should_migrate_partition(source_part):
                continue

            partition_progress = (idx / total_partitions) * 70
            stage_name = f"Copying {source_part.name}"

            self._copy_partition_data(
                source_part,
                target_part,
                stage_name,
                base_progress + partition_progress
            )

    def _should_migrate_partition(self, partition) -> bool:
        """Check if partition should be migrated based on options"""
        if partition.category == 'FAT32':
            return self.options['migrate_fat32']
        elif partition.category == 'Linux':
            return self.options['migrate_linux']
        elif partition.category == 'Android':
            return self.options['migrate_android']
        elif partition.category == 'emuMMC':
            return self.options['migrate_emummc']
        return False

    def _copy_partition_data(self, source_part, target_part, stage_name, base_progress):
        """Copy data from source partition to target partition - uses appropriate method based on type"""

        # FAT32: Use file-level copy (much faster, only copies actual files)
        if source_part.category == 'FAT32':
            logger.info(f"Using file-level copy for FAT32 partition")
            self._copy_fat32_files(source_part, target_part, stage_name, base_progress)
        else:
            # RAW partitions (emuMMC, Linux, Android): Use bit-by-bit sector copy
            logger.info(f"Using sector-level copy for {source_part.category} partition")

            # Calculate chunk size in sectors
            chunk_sectors = CHUNK_SIZE // SECTOR_SIZE
            total_sectors = source_part.size_sectors

            # Use threaded I/O only if we have multiple buffers
            if NUM_BUFFERS > 1:
                self._copy_partition_data_threaded(
                    source_part, target_part, stage_name, base_progress,
                    chunk_sectors, total_sectors
                )
            else:
                # Fallback to single-threaded for low-memory systems
                self._copy_partition_data_single(
                    source_part, target_part, stage_name, base_progress,
                    chunk_sectors, total_sectors
                )

    def _copy_partition_data_single(self, source_part, target_part, stage_name,
                                     base_progress, chunk_sectors, total_sectors):
        """Single-threaded copy (original method, for low-memory systems)"""
        sectors_copied = 0
        last_progress_update = 0
        start_time = time.time()
        last_log_time = start_time

        # Note: disk is already prepared by _copy_partitions(), no need to prepare again

        logger.info(f"Starting sector copy: {total_sectors} sectors ({(total_sectors * SECTOR_SIZE) / (1024**3):.2f} GB)")

        while sectors_copied < total_sectors:
            if self.cancelled:
                raise Exception("Migration cancelled by user")

            # Calculate sectors to copy in this iteration
            remaining = total_sectors - sectors_copied
            sectors_to_copy = min(chunk_sectors, remaining)

            # Read from source
            source_sector = source_part.start_sector + sectors_copied
            data = self.disk_manager.read_sectors(
                self.source_disk['path'],
                source_sector,
                sectors_to_copy
            )

            # Write to target (skip prepare since we did it once)
            target_sector = target_part.start_sector + sectors_copied
            self.disk_manager.write_sectors(
                self.target_disk['path'],
                target_sector,
                data,
                skip_prepare=True
            )

            sectors_copied += sectors_to_copy

            # Calculate progress and speed
            percent = (sectors_copied / total_sectors) * 100
            elapsed = time.time() - start_time
            current_time = time.time()

            # Report progress more frequently (every 1% or every 5 seconds)
            should_update = (percent - last_progress_update >= 1.0 or
                           sectors_copied == total_sectors or
                           (current_time - last_log_time) >= 5.0)

            if should_update:
                progress = base_progress + (percent / 100) * (70 / len(self.source_layout.partitions))
                mb_copied = (sectors_copied * SECTOR_SIZE) / (1024 * 1024)
                mb_total = (total_sectors * SECTOR_SIZE) / (1024 * 1024)

                # Calculate speed
                speed_mbps = mb_copied / elapsed if elapsed > 0 else 0

                # Estimate time remaining
                if speed_mbps > 0:
                    remaining_mb = mb_total - mb_copied
                    eta_seconds = remaining_mb / speed_mbps
                    eta_mins = int(eta_seconds / 60)
                    eta_secs = int(eta_seconds % 60)
                    eta_str = f" - ETA: {eta_mins}m {eta_secs}s"
                else:
                    eta_str = ""

                log_msg = (f"Copied {mb_copied:.1f} MB / {mb_total:.1f} MB ({percent:.1f}%) "
                          f"at {speed_mbps:.1f} MB/s{eta_str}")

                logger.info(log_msg)
                self._report_progress(stage_name, progress, log_msg)

                last_progress_update = percent
                last_log_time = current_time

        logger.info(f"Sector copy completed in {elapsed:.1f} seconds")

    def _copy_partition_data_threaded(self, source_part, target_part, stage_name,
                                       base_progress, chunk_sectors, total_sectors):
        """Threaded copy with double/triple buffering for overlapped I/O"""

        # Note: disk is already prepared by _copy_partitions(), no need to prepare again

        # Queues for producer-consumer pattern
        read_queue = queue.Queue(maxsize=NUM_BUFFERS)
        write_queue = queue.Queue(maxsize=NUM_BUFFERS)

        # Shared state
        sectors_copied = [0]  # Use list for mutable reference
        last_progress_update = [0]
        start_time = [time.time()]
        last_log_time = [time.time()]
        error_holder = [None]

        logger.info(f"Starting threaded sector copy: {total_sectors} sectors ({(total_sectors * SECTOR_SIZE) / (1024**3):.2f} GB)")
        logger.info(f"Using {NUM_BUFFERS} buffers with {chunk_sectors} sectors per chunk")

        def reader_thread():
            """Read chunks from source disk"""
            try:
                sectors_read = 0
                while sectors_read < total_sectors:
                    if self.cancelled:
                        return

                    remaining = total_sectors - sectors_read
                    sectors_to_read = min(chunk_sectors, remaining)

                    source_sector = source_part.start_sector + sectors_read
                    data = self.disk_manager.read_sectors(
                        self.source_disk['path'],
                        source_sector,
                        sectors_to_read
                    )

                    # Put into write queue with sector position
                    target_sector = target_part.start_sector + sectors_read
                    write_queue.put((target_sector, data, sectors_to_read))

                    sectors_read += sectors_to_read

                # Signal end of reading
                write_queue.put(None)

            except Exception as e:
                error_holder[0] = e
                write_queue.put(None)

        def writer_thread():
            """Write chunks to target disk"""
            try:
                while True:
                    if self.cancelled:
                        return

                    item = write_queue.get()
                    if item is None:  # End signal
                        break

                    target_sector, data, sectors_written = item

                    # Skip prepare since we did it once at the beginning
                    self.disk_manager.write_sectors(
                        self.target_disk['path'],
                        target_sector,
                        data,
                        skip_prepare=True
                    )

                    # Update progress
                    sectors_copied[0] += sectors_written

                    # Calculate progress and speed
                    percent = (sectors_copied[0] / total_sectors) * 100
                    elapsed = time.time() - start_time[0]
                    current_time = time.time()

                    # Report progress more frequently (every 1% or every 5 seconds)
                    should_update = (percent - last_progress_update[0] >= 1.0 or
                                   sectors_copied[0] == total_sectors or
                                   (current_time - last_log_time[0]) >= 5.0)

                    if should_update:
                        progress = base_progress + (percent / 100) * (70 / len(self.source_layout.partitions))
                        mb_copied = (sectors_copied[0] * SECTOR_SIZE) / (1024 * 1024)
                        mb_total = (total_sectors * SECTOR_SIZE) / (1024 * 1024)

                        # Calculate speed
                        speed_mbps = mb_copied / elapsed if elapsed > 0 else 0

                        # Estimate time remaining
                        if speed_mbps > 0:
                            remaining_mb = mb_total - mb_copied
                            eta_seconds = remaining_mb / speed_mbps
                            eta_mins = int(eta_seconds / 60)
                            eta_secs = int(eta_seconds % 60)
                            eta_str = f" - ETA: {eta_mins}m {eta_secs}s"
                        else:
                            eta_str = ""

                        log_msg = (f"Copied {mb_copied:.1f} MB / {mb_total:.1f} MB ({percent:.1f}%) "
                                  f"at {speed_mbps:.1f} MB/s{eta_str}")

                        logger.info(log_msg)
                        self._report_progress(stage_name, progress, log_msg)

                        last_progress_update[0] = percent
                        last_log_time[0] = current_time

                    write_queue.task_done()

            except Exception as e:
                error_holder[0] = e

        # Start threads
        reader = threading.Thread(target=reader_thread, daemon=True)
        writer = threading.Thread(target=writer_thread, daemon=True)

        reader.start()
        writer.start()

        # Wait for completion
        reader.join()
        writer.join()

        # Check for errors
        if error_holder[0]:
            raise error_holder[0]

        if self.cancelled:
            raise Exception("Migration cancelled by user")

        elapsed = time.time() - start_time[0]
        logger.info(f"Threaded sector copy completed in {elapsed:.1f} seconds")

    def _copy_fat32_files(self, source_part, target_part, stage_name, base_progress):
        """Copy FAT32 partition using file-level copy (much faster than sector copy)"""

        logger.info("=== FAT32 File-Level Copy ===")
        self._report_progress(stage_name, base_progress, "Preparing FAT32 copy...")

        source_drive_letter = None
        target_drive_letter = None

        try:
            # Step 1: Use the locked drive letter from formatting stage
            # This was already assigned and locked during _create_fat32_filesystem
            if hasattr(self, 'target_fat32_drive') and self.target_fat32_drive:
                target_drive_letter = self.target_fat32_drive
                logger.info(f"Using locked target drive letter: {target_drive_letter}")
            else:
                # Fallback: try to find and assign if not already locked
                logger.info("No locked drive letter found, assigning now...")
                self._format_fat32_partition(target_part)
                target_drive_letter = self._get_drive_letter_for_partition(
                    self.target_disk['path'],
                    target_part.start_sector
                )

            if not target_drive_letter:
                raise Exception("Could not find target FAT32 drive letter. The partition may not be mounted yet.")

            logger.info(f"Target FAT32 is mounted as {target_drive_letter}")
            self._report_progress(stage_name, base_progress + 5, f"Target: {target_drive_letter}")

            # Step 2: Get source drive letter
            logger.info("Finding source FAT32 drive letter...")
            source_drive_letter = self._get_drive_letter_for_partition(
                self.source_disk['path'],
                source_part.start_sector
            )

            if not source_drive_letter:
                raise Exception("Could not find source FAT32 drive letter. Please ensure the source SD card FAT32 partition is mounted.")

            logger.info(f"Source FAT32 is mounted as {source_drive_letter}")
            self._report_progress(stage_name, base_progress + 8, f"Source: {source_drive_letter}")

            # Step 4: Copy files using simple Python copy
            logger.info(f"Copying files from {source_drive_letter} to {target_drive_letter}...")
            self._report_progress(stage_name, base_progress + 10, "Copying files...")

            self._copy_files_simple(
                source_drive_letter,
                target_drive_letter,
                stage_name,
                base_progress + 10
            )

            logger.info("FAT32 file copy completed successfully")

        except Exception as e:
            logger.error(f"FAT32 file copy failed: {e}")
            raise

    def _format_fat32_partition(self, partition):
        """Assign drive letter to FAT32 partition (filesystem already created)"""
        logger.info(f"Assigning drive letter to partition at sector {partition.start_sector}...")

        # Note: FAT32 filesystem was already created in earlier stage
        # This function now only assigns a drive letter for file copying

        # Assign drive letter
        disk_index = self.target_disk['path'].replace("\\\\.\\PhysicalDrive", "")

        # We need to find the partition number by checking which partition matches our start sector
        partition_num = None
        MAX_RETRIES = 5
        RETRY_DELAY = 2

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Attempting to find partition number (attempt {attempt + 1}/{MAX_RETRIES})...")

                # Refresh WMI connection if retry (COM is already initialized for this thread)
                if attempt > 0:
                    logger.info("Refreshing WMI connection...")
                    import wmi
                    self.disk_manager.wmi = wmi.WMI()

                # Query partitions
                partitions = self.disk_manager.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                logger.info(f"Found {len(partitions)} partitions on disk {disk_index}")

                # Find the partition that matches our start sector
                for part in partitions:
                    part_start = int(part.StartingOffset) // SECTOR_SIZE
                    logger.debug(f"Partition {part.Index}: starts at sector {part_start}")

                    if abs(part_start - partition.start_sector) < 2048:  # Within 1MB tolerance
                        partition_num = int(part.Index)
                        logger.info(f"Found matching partition: {partition_num}")
                        break

                if partition_num is not None:
                    break

                # Partition not found yet, try refreshing with diskpart
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Partition not found, refreshing disk and waiting {RETRY_DELAY}s...")
                    diskpart_script = f"select disk {disk_index}\nrescan\n"
                    subprocess.run(
                        ['diskpart'],
                        input=diskpart_script,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        creationflags=subprocess.CREATE_NO_WINDOW
                    )
                    time.sleep(RETRY_DELAY)

            except Exception as e:
                logger.warning(f"Error querying partitions: {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Waiting {RETRY_DELAY}s before retry...")
                    time.sleep(RETRY_DELAY)

        if partition_num is None:
            logger.warning("Could not find partition number, assuming partition 1")
            partition_num = 1

        # Assign drive letter only (filesystem already created)
        diskpart_script = f"""select disk {disk_index}
select partition {partition_num}
assign
"""

        logger.info(f"Running diskpart to assign drive letter to partition {partition_num}...")
        result = subprocess.run(
            ['diskpart'],
            input=diskpart_script,
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW
        )

        if result.returncode != 0:
            logger.error(f"Diskpart assign failed: {result.stderr}")
            raise Exception(f"Failed to assign drive letter to FAT32 partition: {result.stderr}")

        logger.info("Successfully assigned drive letter to partition")

        # Wait for Windows to mount the partition
        time.sleep(3)

        logger.info("FAT32 partition should now be mounted")

    def _assign_and_lock_drive_letter(self, partition, preferred_letter=None):
        """
        Assign a specific drive letter to a partition and lock it.
        If preferred_letter is provided, try to use that. Otherwise find what Windows assigned.
        Returns the locked drive letter.
        """
        disk_index = self.target_disk['path'].replace("\\\\.\\PhysicalDrive", "")

        # First, find the partition number
        partition_num = self._find_partition_number(partition.start_sector)

        if partition_num is None:
            raise RuntimeError(f"Could not find partition at sector {partition.start_sector}")

        # If a preferred letter is specified, assign it explicitly
        if preferred_letter:
            logger.info(f"Assigning specific drive letter {preferred_letter} to partition {partition_num}...")
            diskpart_script = f"""select disk {disk_index}
select partition {partition_num}
assign letter={preferred_letter.replace(':', '')}
"""
        else:
            # Just ensure partition has a letter assigned (Windows usually auto-assigns)
            logger.info(f"Ensuring partition {partition_num} has a drive letter...")
            diskpart_script = f"""select disk {disk_index}
select partition {partition_num}
assign
"""

        result = subprocess.run(
            ['diskpart'],
            input=diskpart_script,
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW
        )

        if result.returncode != 0 and "already assigned" not in result.stdout.lower():
            logger.warning(f"Diskpart assign returned: {result.stderr}")

        # Wait for assignment to take effect
        time.sleep(2)

        # Get the actual drive letter
        drive_letter = self._get_drive_letter_for_partition(self.target_disk['path'], partition.start_sector)

        if not drive_letter:
            raise RuntimeError(f"Failed to get drive letter for partition at sector {partition.start_sector}")

        logger.info(f"Partition locked to drive letter: {drive_letter}")
        return drive_letter

    def _find_partition_number(self, start_sector):
        """Find the partition number (index) for a partition starting at a specific sector"""
        disk_index = self.target_disk['path'].replace("\\\\.\\PhysicalDrive", "")
        MAX_RETRIES = 10
        RETRY_DELAY = 2

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Looking for partition at sector {start_sector} (attempt {attempt + 1}/{MAX_RETRIES})...")

                # Refresh WMI if retry
                if attempt > 0:
                    import wmi
                    self.disk_manager.wmi = wmi.WMI()

                partitions = self.disk_manager.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                logger.info(f"Found {len(partitions)} partitions on disk {disk_index}")

                for part in partitions:
                    part_start = int(part.StartingOffset) // SECTOR_SIZE
                    logger.info(f"  Partition {part.Index}: starts at sector {part_start}")

                    if abs(part_start - start_sector) < 2048:  # Within 1MB tolerance
                        logger.info(f"Found matching partition number: {part.Index}")
                        return int(part.Index)

                # Not found, refresh and retry
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Partition not found, refreshing and waiting {RETRY_DELAY}s...")
                    diskpart_script = f"select disk {disk_index}\nrescan\n"
                    subprocess.run(['diskpart'], input=diskpart_script, capture_output=True, text=True, timeout=30, creationflags=subprocess.CREATE_NO_WINDOW)
                    time.sleep(RETRY_DELAY)

            except Exception as e:
                logger.warning(f"Error finding partition: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        return None

    def _create_fat32_filesystem(self, partition):
        """
        Create a FAT32 filesystem using fat32format.exe tool.
        This ensures proper compatibility with Windows and eliminates corruption issues.
        """
        logger.info("Creating FAT32 filesystem using fat32format.exe...")

        # Calculate optimal cluster size based on partition size
        # Use 128 sectors per cluster (64KB) which is optimal for SD cards
        sectors_per_cluster = 128

        # Get the tool path
        tool_dir = Path(__file__).parent.parent / "tool"
        fat32format_exe = tool_dir / "fat32format.exe"

        if not fat32format_exe.exists():
            raise FileNotFoundError(f"fat32format.exe not found at {fat32format_exe}")

        logger.info(f"FAT32 format tool: {fat32format_exe}")
        logger.info(f"Partition size: {partition.size_sectors * 512 / (1024**3):.2f} GB")
        logger.info(f"Cluster size: {sectors_per_cluster} sectors (64KB)")

        # Step 1: Assign and lock a drive letter for this partition
        # Store it in instance variable so it's locked for the entire migration
        logger.info("Assigning and locking drive letter for FAT32 partition...")
        self.target_fat32_drive = self._assign_and_lock_drive_letter(partition)

        logger.info(f"FAT32 partition locked to drive letter: {self.target_fat32_drive}")

        # Step 2: Run fat32format.exe with the appropriate cluster size
        format_cmd = [
            str(fat32format_exe),
            f"-c{sectors_per_cluster}",  # Cluster size
            f"{self.target_fat32_drive}"
        ]

        logger.info(f"Running format command: {' '.join(format_cmd)}")

        # Run the format command with "Y" piped to confirm
        # fat32format.exe asks for confirmation, so we need to answer "Y"
        result = subprocess.run(
            format_cmd,
            input="Y\n",  # Auto-confirm the format operation
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            creationflags=subprocess.CREATE_NO_WINDOW
        )

        logger.info(f"Format command output:\n{result.stdout}")

        if result.returncode != 0:
            logger.error(f"Format command failed with return code {result.returncode}")
            logger.error(f"Error output:\n{result.stderr}")
            raise RuntimeError(f"FAT32 format failed: {result.stderr}")

        logger.info(f"FAT32 filesystem created successfully with fat32format.exe")
        logger.info(f"Drive letter {self.target_fat32_drive} is locked for this migration")
        
        # CRITICAL: Verify FAT32 boot sector has correct partition size
        # Without this, Windows may see incorrect filesystem size and report "disk full" prematurely
        self._verify_and_fix_fat32_bpb(partition)

    def _verify_and_fix_fat32_bpb(self, partition):
        """
        Verify and fix FAT32 BPB (BIOS Parameter Block) to ensure filesystem size matches partition size.
        This is critical for Hekate to detect the correct FAT32 partition size.
        """
        try:
            # IMPORTANT: Dismount the volume before reading/writing boot sector
            # Otherwise Windows may have it cached and will overwrite our changes
            logger.info("Dismounting FAT32 volume before BPB update...")
            self._dismount_partition(partition)
            time.sleep(1)

            # Read the boot sector (sector 0 of the partition)
            boot_sector_data = self.disk_manager.read_sectors(
                self.target_disk['path'],
                partition.start_sector,
                1
            )

            # Parse FAT32 BPB
            bytes_per_sector = struct.unpack('<H', boot_sector_data[11:13])[0]
            sectors_per_cluster = boot_sector_data[13]
            reserved_sectors = struct.unpack('<H', boot_sector_data[14:16])[0]
            num_fats = boot_sector_data[16]

            # FAT32 specific fields
            fat_size_32 = struct.unpack('<I', boot_sector_data[36:40])[0]
            total_sectors_32 = struct.unpack('<I', boot_sector_data[32:36])[0]

            logger.info(f"Current FAT32 BPB values:")
            logger.info(f"  Bytes per sector: {bytes_per_sector}")
            logger.info(f"  Sectors per cluster: {sectors_per_cluster}")
            logger.info(f"  Reserved sectors: {reserved_sectors}")
            logger.info(f"  Number of FATs: {num_fats}")
            logger.info(f"  FAT size (sectors): {fat_size_32}")
            logger.info(f"  Total sectors in filesystem: {total_sectors_32}")

            # Calculate what the total sectors SHOULD be (based on partition size)
            expected_total_sectors = partition.size_sectors

            logger.info(f"Expected total sectors (from partition table): {expected_total_sectors}")
            logger.info(f"Actual total sectors (from FAT32 BPB): {total_sectors_32}")

            # Check if the BPB needs updating
            if total_sectors_32 != expected_total_sectors:
                logger.warning(f"FAT32 BPB mismatch! BPB reports {total_sectors_32} sectors, but partition has {expected_total_sectors} sectors")
                logger.info(f"Updating FAT32 BPB to reflect correct partition size...")

                # Create updated boot sector with corrected total sectors
                boot_sector_updated = bytearray(boot_sector_data)

                # Update total sectors (offset 32, 4 bytes, little-endian)
                boot_sector_updated[32:36] = struct.pack('<I', expected_total_sectors)

                # Recalculate and update the boot sector signature if needed
                # (Most implementations don't check this, but let's be thorough)

                # Write the updated boot sector back
                logger.info(f"Writing updated boot sector to partition...")
                self.disk_manager.write_sectors(
                    self.target_disk['path'],
                    partition.start_sector,
                    bytes(boot_sector_updated),
                    skip_prepare=True
                )

                # Also update backup boot sector (FAT32 keeps backup at sector 6)
                logger.info(f"Updating backup boot sector at sector 6...")
                self.disk_manager.write_sectors(
                    self.target_disk['path'],
                    partition.start_sector + 6,
                    bytes(boot_sector_updated),
                    skip_prepare=True
                )

                logger.info(f"FAT32 BPB updated successfully - filesystem now reports {expected_total_sectors} sectors")

                # Flush disk cache to ensure changes are written
                logger.info("Flushing disk cache to ensure BPB changes are committed...")
                self._refresh_disk_partitions(self.target_disk['path'])
                time.sleep(2)

            else:
                logger.info(f"FAT32 BPB is correct - filesystem size matches partition size")

        except Exception as e:
            logger.error(f"Error verifying/fixing FAT32 BPB: {e}")
            logger.warning("Continuing anyway - the partition may still work, but Hekate might see wrong size")

    def _dismount_partition(self, partition):
        """Dismount/offline a partition using diskpart to release Windows locks"""
        try:
            disk_index = self.target_disk['path'].replace("\\\\.\\PhysicalDrive", "")

            # Find the partition number for this start sector
            partition_num = None
            try:
                partitions = self.disk_manager.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                for part in partitions:
                    part_start = int(part.StartingOffset) // SECTOR_SIZE
                    if abs(part_start - partition.start_sector) < 2048:  # Within 1MB tolerance
                        partition_num = int(part.Index)
                        break
            except:
                logger.warning("Could not find partition number via WMI")

            if partition_num is None:
                logger.warning("Could not determine partition number for dismount")
                return

            # Use diskpart to remove drive letter (dismount)
            diskpart_script = f"""select disk {disk_index}
select partition {partition_num}
remove
"""

            logger.info(f"Dismounting partition {partition_num} on disk {disk_index}...")
            result = subprocess.run(
                ['diskpart'],
                input=diskpart_script,
                capture_output=True,
                text=True,
                timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW
            )

            if result.returncode == 0:
                logger.info("Successfully dismounted partition")
            else:
                logger.warning(f"Diskpart dismount returned: {result.stderr}")

        except Exception as e:
            logger.warning(f"Could not dismount partition: {e}")

    def _get_drive_letter_for_partition(self, disk_path, start_sector):
        """Get the drive letter for a partition at a specific sector"""
        MAX_RETRIES = 5
        RETRY_DELAY = 2  # seconds

        for attempt in range(MAX_RETRIES):
            try:
                disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")

                # Reconnect WMI if needed (helps with stale cache issues, COM is already initialized for this thread)
                if attempt > 0:
                    logger.info(f"Retry {attempt + 1}/{MAX_RETRIES}: Refreshing WMI connection...")
                    try:
                        import wmi
                        self.disk_manager.wmi = wmi.WMI()
                    except Exception as wmi_err:
                        logger.warning(f"Could not refresh WMI connection: {wmi_err}")

                # Query all partitions on this disk
                partitions = self.disk_manager.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                logger.debug(f"Found {len(partitions)} partitions on disk {disk_index}")

                for partition in partitions:
                    # Check if this partition starts at our target sector
                    part_start = int(partition.StartingOffset) // SECTOR_SIZE

                    logger.debug(f"Checking partition at sector {part_start} (looking for {start_sector})")

                    # Allow some tolerance (within 2048 sectors = 1MB)
                    if abs(part_start - start_sector) < 2048:
                        # Get associated logical disk
                        logical_disks = self.disk_manager.wmi.query(
                            f"ASSOCIATORS OF {{Win32_DiskPartition.DeviceID='{partition.DeviceID}'}} "
                            f"WHERE AssocClass=Win32_LogicalDiskToPartition"
                        )

                        if logical_disks:
                            drive_letter = logical_disks[0].DeviceID
                            logger.info(f"Found drive letter: {drive_letter} for partition at sector {start_sector}")
                            return drive_letter

                logger.warning(f"No drive letter found for partition at sector {start_sector}")

                # If we found partitions but no match, try alternative method
                # This handles hybrid MBR/GPT disks where WMI doesn't see all partitions
                if len(partitions) > 0 and attempt == 0:
                    logger.info("WMI didn't find matching partition, trying alternative method using diskpart...")
                    alt_drive = self._find_drive_letter_by_diskpart(disk_path, start_sector)
                    if alt_drive:
                        return alt_drive
                    return None

                # If no partitions found at all, WMI might need to refresh - retry
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"No partitions found on disk {disk_index}, waiting {RETRY_DELAY}s before retry...")
                    time.sleep(RETRY_DELAY)
                else:
                    return None

            except Exception as e:
                error_str = str(e)
                logger.error(f"Error finding drive letter (attempt {attempt + 1}/{MAX_RETRIES}): {e}")

                # Check if it's a WMI COM error
                if "COM Error" in error_str or "-2147352567" in error_str:
                    if attempt < MAX_RETRIES - 1:
                        logger.info(f"WMI COM error detected, waiting {RETRY_DELAY}s before retry...")
                        time.sleep(RETRY_DELAY)
                    else:
                        logger.error("WMI COM error persists after all retries")
                        return None
                else:
                    # Non-WMI error, don't retry
                    return None

        return None

    def _find_drive_letter_by_diskpart(self, disk_path, start_sector):
        """
        Alternative method to find drive letter using diskpart
        Used when WMI can't see GPT partitions on hybrid MBR/GPT disks
        """
        import subprocess
        try:
            disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")
            logger.info(f"Using diskpart to find partitions on disk {disk_index}...")

            # Use diskpart to list all volumes and find FAT32 volumes
            script = f"""list volume
exit
"""
            process = subprocess.Popen(
                ['diskpart'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            stdout, stderr = process.communicate(input=script, timeout=30)

            logger.debug(f"Diskpart output:\n{stdout}")

            # Parse diskpart output to find FAT32 volumes
            # Look for lines like: "  Volume 3     E   SWITCH SD    FAT32   Removable     51 GB  Healthy"
            import re
            for line in stdout.split('\n'):
                # Match volume lines with drive letter
                match = re.search(r'Volume\s+\d+\s+([A-Z])\s+.*FAT32.*Removable', line, re.IGNORECASE)
                if match:
                    drive_letter = match.group(1) + ':'
                    logger.info(f"Found potential source FAT32 volume: {drive_letter}")

                    # Verify this is the correct disk by checking volume info
                    # Query detailed info for this volume
                    verify_script = f"""select volume {match.group(1)}
detail volume
exit
"""
                    verify_process = subprocess.Popen(
                        ['diskpart'],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        creationflags=subprocess.CREATE_NO_WINDOW
                    )
                    verify_out, _ = verify_process.communicate(input=verify_script, timeout=30)

                    # Check if this volume is on the correct disk
                    if f"Disk {disk_index}" in verify_out:
                        logger.info(f"Confirmed {drive_letter} is on disk {disk_index}")
                        return drive_letter

            logger.warning(f"Could not find FAT32 volume on disk {disk_index} using diskpart")
            return None

        except subprocess.TimeoutExpired:
            logger.error("Diskpart command timed out")
            return None
        except Exception as e:
            logger.error(f"Error using diskpart to find drive letter: {e}")
            return None

    def _refresh_disk_partitions(self, disk_path):
        """Refresh disk to make new partitions visible to Windows"""
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
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW
            )

            logger.info("Disk partitions refreshed")

        except Exception as e:
            logger.warning(f"Could not refresh disk partitions: {e}")

    def _copy_files_simple(self, source_drive, target_drive, stage_name, base_progress):
        """Copy files using simple Python shutil - more reliable than robocopy"""
        import os
        from pathlib import Path

        # Ensure drive letters are properly formatted (e.g., "G:\" or "G:/")
        # Don't strip the colon - only strip trailing backslashes
        source = Path(source_drive.rstrip('\\') + '\\')
        target = Path(target_drive.rstrip('\\') + '\\')

        logger.info(f"Starting file copy: {source} -> {target}")
        logger.info(f"Checking if source exists and is accessible...")

        # Verify source path exists and is accessible
        if not source.exists():
            raise Exception(f"Source path does not exist: {source}")

        if not source.is_dir():
            raise Exception(f"Source path is not a directory: {source}")

        # Count total files first for better progress reporting
        logger.info(f"Scanning source directory for files...")
        total_files = sum(1 for root, dirs, files in os.walk(source) for file in files)
        logger.info(f"Found {total_files} files to copy")
        
        start_time = time.time()
        files_copied = 0
        bytes_copied = 0
        
        try:
            # Walk through source directory
            for root, dirs, files in os.walk(source):
                # Calculate relative path
                rel_path = Path(root).relative_to(source)
                target_dir = target / rel_path
                
                # Create target directory if it doesn't exist
                target_dir.mkdir(parents=True, exist_ok=True)
                
                # Copy each file
                for file in files:
                    if self.cancelled:
                        raise Exception("Migration cancelled by user")
                    
                    source_file = Path(root) / file
                    target_file = target_dir / file
                    
                    try:
                        # Copy file
                        shutil.copy2(source_file, target_file)
                        files_copied += 1
                        bytes_copied += source_file.stat().st_size
                        
                        # Update progress every 10 files or every 100MB
                        if files_copied % 10 == 0 or (bytes_copied // (100 * 1024 * 1024)) > ((bytes_copied - source_file.stat().st_size) // (100 * 1024 * 1024)):
                            elapsed = time.time() - start_time
                            speed_mbps = (bytes_copied / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                            percent_complete = (files_copied / total_files * 100) if total_files > 0 else 0
                            logger.info(f"Copied {files_copied}/{total_files} files ({percent_complete:.1f}%), {bytes_copied / (1024**3):.2f} GB at {speed_mbps:.1f} MB/s")

                            # Calculate progress within the FAT32 copy stage (10% to 85% of the stage)
                            file_progress = (files_copied / total_files * 75) if total_files > 0 else 0
                            self._report_progress(stage_name, base_progress + 10 + file_progress,
                                                f"Copied {files_copied}/{total_files} files ({percent_complete:.0f}%)")
                    except Exception as e:
                        logger.error(f"Failed to copy {source_file}: {e}")
                        raise
            
            elapsed_time = time.time() - start_time
            mb_copied = bytes_copied / (1024 * 1024)
            speed_mbps = mb_copied / elapsed_time if elapsed_time > 0 else 0

            logger.info(f"File copy completed successfully in {elapsed_time:.1f} seconds")
            logger.info(f"Files copied: {files_copied} of {total_files}")
            logger.info(f"Data copied: {mb_copied:.1f} MB ({bytes_copied / (1024**3):.2f} GB) at {speed_mbps:.1f} MB/s")

            if files_copied == 0:
                logger.warning(f"No files were found in source directory: {source}")
                logger.warning(f"The source FAT32 partition appears to be empty!")

            self._report_progress(stage_name, base_progress + 85,
                                f"✓ Copied {files_copied} files ({mb_copied:.0f} MB) in {elapsed_time:.0f}s")
            
        except Exception as e:
            logger.error(f"File copy error: {e}")
            raise

    def _copy_files_robocopy(self, source_drive, target_drive, stage_name, base_progress):
        """Copy files using robocopy with progress tracking"""

        # Remove trailing backslash if present
        source = source_drive.rstrip('\\')
        target = target_drive.rstrip('\\')

        logger.info(f"Starting robocopy: {source} -> {target}")

        # Robocopy command with optimized options for SD card migration:
        # /E - copy subdirectories including empty ones
        # /COPY:D - copy ONLY data (skip attributes/timestamps for speed)
        # /R:1 - retry only 1 time on failure (reduced from 2)
        # /W:1 - wait 1 second between retries
        # /NP - no progress percentage (we'll track ourselves)
        # /J - unbuffered I/O for large files (faster for sequential writes)
        # /MT:2 - only 2 threads (reduces contention on USB/SD)
        # /BYTES - show file sizes in bytes (helps with progress tracking)

        cmd = [
            'robocopy',
            source,
            target,
            '/E',           # Copy subdirectories including empty
            '/COPY:D',      # Copy ONLY data (skip attributes/timestamps for speed)
            '/R:1',         # Retry only 1 time (reduced overhead)
            '/W:1',         # Wait 1 second between retries
            '/NP',          # No progress percentage per file
            '/J',           # Unbuffered I/O for large files
            '/MT:2',        # Only 2 threads (better for USB/SD drives)
            '/BYTES'        # Show progress with byte counts
        ]

        logger.info(f"Robocopy command: {' '.join(cmd)}")
        self._report_progress(stage_name, base_progress, "Scanning source files...")

        try:
            # Run robocopy with real-time output
            start_time = time.time()
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                creationflags=subprocess.CREATE_NO_WINDOW,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )

            # Track progress by monitoring output
            files_copied = 0
            dirs_created = 0
            bytes_copied = 0
            last_progress_time = start_time
            last_file = ""
            monitor_running = [True]  # Shared flag for monitor thread

            logger.info("Robocopy started with optimized settings...")

            # Start a background thread to monitor target directory and provide heartbeat
            def monitor_target():
                """Background thread to monitor target directory for progress"""
                last_count = 0
                while monitor_running[0] and process.poll() is None:
                    try:
                        # Count files in target directory
                        file_count = sum(1 for _ in Path(target).rglob('*') if _.is_file())

                        if file_count != last_count:
                            logger.info(f"Target directory now has {file_count} files")
                            last_count = file_count

                        # Update progress
                        elapsed = time.time() - start_time
                        self._report_progress(stage_name, base_progress + 5,
                                            f"Copying... {file_count} files so far ({elapsed:.0f}s)")
                    except Exception as e:
                        logger.debug(f"Monitor thread error: {e}")

                    time.sleep(3)  # Check every 3 seconds

            monitor_thread = threading.Thread(target=monitor_target, daemon=True)
            monitor_thread.start()

            # Read output line by line in real-time
            for line in process.stdout:
                line = line.strip()

                if not line:
                    continue

                # Check if this line indicates a file being copied
                # Robocopy with default output shows files in various formats
                if line.startswith('New File') or line.startswith('Newer') or (line and '\\' in line and not line.startswith('-') and not line.startswith('Total')):
                    # This is likely a file operation
                    files_copied += 1
                    last_file = line

                    # Log periodically
                    if files_copied % 50 == 0:
                        logger.info(f"Copied {files_copied} files...")

                # Parse summary lines
                elif line.startswith('Dirs :'):
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            dirs_created = int(parts[2])
                            logger.info(f"Robocopy summary: {line}")
                        except ValueError:
                            pass

                elif line.startswith('Files :'):
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            files_total = int(parts[2])
                            logger.info(f"Robocopy summary: {line}")
                            # Use the summary count if it's higher (more accurate)
                            if files_total > files_copied:
                                files_copied = files_total
                        except ValueError:
                            pass

                elif line.startswith('Bytes :'):
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            bytes_copied = int(parts[2])
                            logger.info(f"Robocopy summary: {line}")
                        except ValueError:
                            pass

                elif 'Error' in line or 'ERROR' in line or 'Failed' in line:
                    logger.warning(f"Robocopy warning: {line}")

                # Log all output for debugging
                else:
                    logger.debug(f"Robocopy output: {line}")

            # Wait for process to complete (no timeout - large migrations can take hours)
            process.wait()

            # Stop monitor thread
            monitor_running[0] = False

            elapsed_time = time.time() - start_time

            # Check stderr for errors
            stderr_output = process.stderr.read()
            if stderr_output:
                logger.warning(f"Robocopy stderr: {stderr_output}")

            # Robocopy return codes:
            # 0 = No files copied
            # 1 = Files copied successfully
            # 2 = Extra files or directories detected
            # 4 = Mismatched files or directories
            # 8 = Copy errors
            # 16 = Fatal error

            if process.returncode >= 8:
                logger.error(f"Robocopy failed with return code {process.returncode}")
                logger.error(f"Robocopy errors: {stderr_output}")
                raise Exception(f"File copy failed with robocopy error code {process.returncode}")

            # Calculate performance metrics
            mb_copied = bytes_copied / (1024 * 1024) if bytes_copied > 0 else 0
            speed_mbps = mb_copied / elapsed_time if elapsed_time > 0 else 0

            logger.info(f"Robocopy completed successfully in {elapsed_time:.1f} seconds")
            logger.info(f"Files copied: {files_copied}, Directories: {dirs_created}")
            logger.info(f"Data copied: {mb_copied:.1f} MB at {speed_mbps:.1f} MB/s")
            logger.info(f"Return code: {process.returncode}")

            self._report_progress(stage_name, base_progress + 60,
                                f"Copied {files_copied} files in {elapsed_time:.0f}s")

        except Exception as e:
            logger.error(f"Robocopy error: {e}")
            raise

    def _write_partition_tables(self):
        """Write MBR and GPT partition tables"""
        self._report_progress("Writing Partition Tables", 10, "Creating partition table...")

        self.partition_writer.write_partition_table(
            self.target_disk['path'],
            self.target_layout
        )

        # Refresh disk to make new partitions visible to Windows
        logger.info("Refreshing disk to make partitions visible...")
        self._refresh_disk_partitions(self.target_disk['path'])
        time.sleep(2)  # Give Windows time to recognize new partitions

        self._report_progress("Writing Partition Tables", 15, "Partition table written")

    def _write_emummc_efi_signature(self, emummc_partition):
        """
        Write EFI signature at the correct offset within emuMMC partition
        for hekate's "Fix Raw" detection to work

        Hekate checks for "EFI PART" signature at:
        - partition_start + 0xC001 (for full-size emuMMC with protective offset)
        - partition_start + 0x4001 (for resized emuMMC without protective offset)

        The emuMMC structure within the partition is:
        - Sectors 0x0000-0x1FFF: BOOT0 (4 MB)
        - Sectors 0x2000-0x3FFF: BOOT1 (4 MB)
        - Sectors 0x4000-0xBFFF: Protective gap (16 MB)
        - Sector  0xC000: MBR of USER eMMC
        - Sector  0xC001: GPT header of USER eMMC ("EFI PART" signature)

        Strategy:
        1. Try to read GPT from source emuMMC partition
        2. If found, copy to target
        3. If not found, try to find it in already-copied target data
        4. If still not found, create minimal valid GPT header
        
        Returns: The detected offset (0xC001 or 0x4001) for use in emummc.ini calculation
        """
        detected_offset = 0xC001  # Default offset
        
        try:
            self._report_progress("Updating emuMMC Config", 95.5, "Writing EFI signature for Fix Raw detection...")
            logger.info("=" * 60)
            logger.info("Ensuring EFI signature exists for hekate Fix Raw detection")
            logger.info("=" * 60)

            target_partition_start = emummc_partition.start_sector

            # Find source emuMMC partition
            source_emummc = None
            for part in self.source_layout.partitions:
                if part.category == 'emuMMC':
                    source_emummc = part
                    break

            gpt_header_to_write = None
            gpt_entries_to_write = None

            # STEP 1: Try to read GPT from SOURCE emuMMC at offset 0xC001
            # GPT structure: Header (1 sector) + Partition Entries (32 sectors) = 33 sectors total
            if source_emummc:
                source_gpt_sector = source_emummc.start_sector + 0xC001
                logger.info(f"Step 1: Reading GPT from SOURCE emuMMC at sector {source_gpt_sector} (0x{source_gpt_sector:X})")

                try:
                    # Read GPT header (1 sector)
                    source_gpt_data = self.disk_manager.read_sectors(
                        self.source_disk['path'],
                        source_gpt_sector,
                        1
                    )

                    if source_gpt_data[:8] == b'EFI PART':
                        logger.info("✓ Found valid EFI signature in SOURCE at offset 0xC001")
                        gpt_header_to_write = source_gpt_data
                        detected_offset = 0xC001
                        
                        # Also read the GPT partition entries (32 sectors after header)
                        logger.info("Reading GPT partition entries from source...")
                        gpt_entries_to_write = self.disk_manager.read_sectors(
                            self.source_disk['path'],
                            source_gpt_sector + 1,
                            32
                        )
                        logger.info(f"✓ Read {len(gpt_entries_to_write)} bytes of GPT partition entries")
                    else:
                        logger.info("✗ No EFI signature in source at 0xC001, trying 0x4001...")

                        # Try offset 0x4001 (resized emuMMC)
                        source_gpt_sector_alt = source_emummc.start_sector + 0x4001
                        source_gpt_data_alt = self.disk_manager.read_sectors(
                            self.source_disk['path'],
                            source_gpt_sector_alt,
                            1
                        )

                        if source_gpt_data_alt[:8] == b'EFI PART':
                            logger.info("✓ Found valid EFI signature in SOURCE at offset 0x4001")
                            gpt_header_to_write = source_gpt_data_alt
                            detected_offset = 0x4001
                            
                            # Also read the GPT partition entries (32 sectors after header)
                            logger.info("Reading GPT partition entries from source...")
                            gpt_entries_to_write = self.disk_manager.read_sectors(
                                self.source_disk['path'],
                                source_gpt_sector_alt + 1,
                                32
                            )
                            logger.info(f"✓ Read {len(gpt_entries_to_write)} bytes of GPT partition entries")
                        else:
                            logger.info("✗ No EFI signature in source at 0x4001 either")
                            # No GPT found - try to detect actual BOOT0 location by searching for MBR
                            logger.info("Step 1.5: Searching for emuMMC structure (MBR) to detect offset...")
                            detected_offset = self._detect_emummc_offset_by_mbr(source_emummc.start_sector)
                            logger.info(f"Detected offset from MBR search: 0x{detected_offset:X}")

                except Exception as e:
                    logger.warning(f"Could not read from source emuMMC: {e}")
            
            # Store the detected offset for later use in emummc.ini calculation
            self.detected_emummc_offset = detected_offset
            logger.info(f"Detected emuMMC offset: 0x{detected_offset:X} ({detected_offset} sectors)")

            # STEP 2: If not found in source, check if it was already copied to target
            if not gpt_header_to_write:
                logger.info("Step 2: Checking if GPT already exists in copied TARGET data...")
                target_gpt_sector = target_partition_start + detected_offset

                target_gpt_data = self.disk_manager.read_sectors(
                    self.target_disk['path'],
                    target_gpt_sector,
                    1
                )

                if target_gpt_data[:8] == b'EFI PART':
                    logger.info(f"✓ GPT already present in target at offset 0x{detected_offset:X} - Fix Raw should work!")
                    return detected_offset  # Nothing to do, GPT already exists
                else:
                    logger.info(f"✗ No GPT in target at 0x{detected_offset:X}")

                    # Try alternate offset
                    alternate_offset = 0x4001 if detected_offset == 0xC001 else 0xC001
                    target_gpt_sector_alt = target_partition_start + alternate_offset
                    target_gpt_data_alt = self.disk_manager.read_sectors(
                        self.target_disk['path'],
                        target_gpt_sector_alt,
                        1
                    )

                    if target_gpt_data_alt[:8] == b'EFI PART':
                        logger.info(f"✓ GPT found in target at offset 0x{alternate_offset:X} - Fix Raw should work!")
                        detected_offset = alternate_offset
                        self.detected_emummc_offset = detected_offset
                        return detected_offset  # GPT exists at alternate location

            # STEP 3: If still not found, create minimal valid GPT header
            if not gpt_header_to_write:
                logger.info("Step 3: Creating minimal valid GPT header for hekate detection...")
                gpt_header_to_write = self._create_minimal_gpt_header()

            # STEP 4: Write GPT header to target at detected offset
            target_gpt_sector = target_partition_start + detected_offset
            logger.info(f"Step 4: Writing GPT header to target sector {target_gpt_sector} (0x{target_gpt_sector:X})")

            self.disk_manager.write_sectors(
                self.target_disk['path'],
                target_gpt_sector,
                gpt_header_to_write,
                skip_prepare=True
            )
            
            # STEP 5: Write GPT partition entries if we have them
            if gpt_entries_to_write:
                logger.info(f"Step 5: Writing GPT partition entries to target (32 sectors)...")
                self.disk_manager.write_sectors(
                    self.target_disk['path'],
                    target_gpt_sector + 1,
                    gpt_entries_to_write,
                    skip_prepare=True
                )
                logger.info("✓ Successfully wrote GPT partition entries")

            logger.info("✓ Successfully wrote EFI signature - Fix Raw should now work!")
            logger.info("=" * 60)
            
            return detected_offset

        except Exception as e:
            logger.error(f"Error writing emuMMC EFI signature: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't fail the migration, just log the error
            return 0xC001  # Return default offset

    def _detect_emummc_offset_by_mbr(self, partition_start_sector: int) -> int:
        """
        Detect emuMMC offset by searching for the MBR signature (0x55AA) at known offsets.
        
        emuMMC structure:
        - BOOT0 (0x2000 sectors = 4MB)
        - BOOT1 (0x2000 sectors = 4MB)
        - Protective gap (varies: 0x8000 or 0x4000 sectors)
        - MBR at offset 0xC000 or 0x8000 from BOOT0 start
        - GPT at offset 0xC001 or 0x4001 from BOOT0 start (if exists)
        
        Returns: Detected offset (0x0, 0x4001, or 0xC001)
        """
        logger.info(f"Searching for MBR in emuMMC partition starting at sector {partition_start_sector}...")
        
        # Offsets to check for MBR (sector 0xC000 relative to BOOT0)
        # MBR is always 1 sector before GPT
        # The MBR partition entry points to the start of the protective gap,
        # so BOOT0 is at partition_start + 0x8000
        possible_offsets = [
            (0x14000, 0x14001, "BOOT0 at partition +0x8000 (MBR at partition +0x14000, GPT at +0x14001)"),
            (0xC000, 0xC001, "BOOT0 at partition start (MBR at partition +0xC000, GPT at +0xC001)"),
        ]
        
        for mbr_offset, gpt_offset, description in possible_offsets:
            try:
                # Read the sector where MBR should be
                mbr_sector = partition_start_sector + mbr_offset
                logger.info(f"  Checking MBR at offset 0x{mbr_offset:X} ({description})...")
                
                mbr_data = self.disk_manager.read_sectors(
                    self.source_disk['path'],
                    mbr_sector,
                    1
                )
                
                # Check for MBR signature 0x55AA at offset 510-511
                if len(mbr_data) >= 512 and mbr_data[510:512] == b'\x55\xAA':
                    logger.info(f"  ✓ Found MBR signature at offset 0x{mbr_offset:X}")
                    logger.info(f"  → BOOT0 is at partition start + 0x{gpt_offset:X}")
                    return gpt_offset
                else:
                    logger.info(f"  ✗ No MBR signature at offset 0x{mbr_offset:X}")
                    
            except Exception as e:
                logger.warning(f"  Error reading MBR at offset 0x{mbr_offset:X}: {e}")
                continue
        
        # If nothing found, return default
        logger.warning("Could not detect offset from MBR - defaulting to 0xC001")
        return 0xC001

    def _create_minimal_gpt_header(self) -> bytes:
        """
        Create a minimal valid GPT header for Nintendo Switch emuMMC
        This is based on the standard GPT specification and what hekate expects.

        Returns 512 bytes containing a valid GPT header with "EFI PART" signature
        """
        import zlib

        logger.info("Creating minimal GPT header for Switch emuMMC...")

        # Create 512-byte sector
        header = bytearray(512)

        # GPT Header structure (first 92 bytes are defined, rest is reserved/zero)
        # Offset 0-7: Signature "EFI PART"
        header[0:8] = b'EFI PART'

        # Offset 8-11: Revision (1.0) = 0x00010000
        header[8:12] = struct.pack('<I', 0x00010000)

        # Offset 12-15: Header size = 92 bytes
        header[12:16] = struct.pack('<I', 92)

        # Offset 16-19: CRC32 of header (calculated later)
        header[16:20] = struct.pack('<I', 0)

        # Offset 20-23: Reserved (must be zero)
        header[20:24] = struct.pack('<I', 0)

        # Offset 24-31: Current LBA (location of this header)
        # In emuMMC context, GPT is at sector 0xC001 relative to BOOT0
        # This is the LBA within the emuMMC "disk", not the SD card absolute sector
        header[24:32] = struct.pack('<Q', 0xC001)

        # Offset 32-39: Backup LBA (location of backup header)
        # For a 29.1 GB eMMC USER partition: ~60,817,408 sectors
        # Backup GPT is at last sector, but we'll use a safe value
        header[32:40] = struct.pack('<Q', 0x1B4E000)  # Approximate Switch eMMC size in sectors

        # Offset 40-47: First usable LBA for partitions = 34
        # (1 MBR + 1 GPT header + 32 sectors for partition entries)
        # This is relative to the emuMMC "disk", so it's from the start of the USER partition (0xC000)
        header[40:48] = struct.pack('<Q', 0xC000 + 34)

        # Offset 48-55: Last usable LBA
        header[48:56] = struct.pack('<Q', 0x1B4DFE0)  # Backup GPT location - 33

        # Offset 56-71: Disk GUID (16 bytes, random)
        # Use a recognizable pattern for NXMigratorPro: "NXMigratorProGPT"
        disk_guid = b'NXMigratorProGPT'
        header[56:72] = disk_guid

        # Offset 72-79: Starting LBA of partition entries = 2
        # Partition entries start right after GPT header (0xC001 + 1 = 0xC002)
        header[72:80] = struct.pack('<Q', 0xC002)

        # Offset 80-83: Number of partition entries
        # Standard is 128, but we'll use minimum needed for Switch (around 32)
        header[80:84] = struct.pack('<I', 128)

        # Offset 84-87: Size of a single partition entry = 128 bytes
        header[84:88] = struct.pack('<I', 128)

        # Offset 88-91: CRC32 of partition entries array
        # Since we're not creating actual partition entries (only GPT header for detection),
        # we'll use CRC32 of empty entries (all zeros)
        empty_entries = b'\x00' * (128 * 128)  # 128 entries × 128 bytes
        entries_crc = zlib.crc32(empty_entries) & 0xFFFFFFFF
        header[88:92] = struct.pack('<I', entries_crc)

        # Calculate CRC32 of header (bytes 0-91)
        header_crc = zlib.crc32(bytes(header[0:92])) & 0xFFFFFFFF
        header[16:20] = struct.pack('<I', header_crc)

        logger.info("Created minimal GPT header:")
        logger.info(f"  Signature: {header[0:8]}")
        logger.info(f"  Revision: 1.0")
        logger.info(f"  Header CRC32: 0x{header_crc:08X}")
        logger.info(f"  Disk GUID: {disk_guid}")

        return bytes(header)

    def _update_emummc_config(self):
        """Update emuMMC configuration files if sectors changed"""
        self._report_progress("Updating emuMMC Config", 95, "Checking emuMMC configuration...")

        # Get emuMMC partitions
        source_emummc = self.source_layout.get_emummc_partitions()
        target_emummc = self.target_layout.get_emummc_partitions()

        if not source_emummc or not target_emummc:
            logger.info("No emuMMC partitions to update")
            self._report_progress("Updating emuMMC Config", 99, "No emuMMC config to update")
            return

        # First, write the EFI signature at the correct offset for hekate's Fix Raw detection
        # This also detects the actual offset used in the source emuMMC
        detected_offset = self._write_emummc_efi_signature(target_emummc[0])

        # Need to create/update hekate emuMMC configuration
        self._report_progress("Updating emuMMC Config", 96, "Creating hekate emuMMC configuration...")

        try:
            # Get the target FAT32 drive letter
            fat32_part = None
            for part in self.target_layout.partitions:
                if part.category == 'FAT32':
                    fat32_part = part
                    break

            if not fat32_part:
                logger.error("Cannot find FAT32 partition to update emuMMC config")
                self._report_progress("Updating emuMMC Config", 97, "⚠️ Could not find FAT32 partition")
                return

            # Get drive letter for target FAT32 partition
            drive_letter = self._get_drive_letter_for_partition(
                self.target_disk['path'],
                fat32_part.start_sector
            )

            if not drive_letter:
                logger.error("FAT32 partition not mounted - cannot create emuMMC config")
                self._report_progress("Updating emuMMC Config", 97, "⚠️ FAT32 not mounted - manual update needed")
                return

            logger.info(f"FAT32 is mounted as {drive_letter}")

            # Ensure proper path separator
            if not drive_letter.endswith(':'):
                drive_letter += ':'

            # Create hekate emuMMC configuration structure
            # This is required for hekate's "Fix RAW" button to work
            base_path = Path(drive_letter + "\\")
            emummc_path = base_path / "emuMMC"

            # Create emuMMC directory if it doesn't exist
            emummc_path.mkdir(exist_ok=True)
            logger.info(f"Created/verified emuMMC directory at {emummc_path}")

            # Get the target emuMMC partition start sector (this is the GPT partition start)
            target_emummc_gpt_start = target_emummc[0].start_sector

            # Calculate MBR partition offset and emummc.ini sector
            #
            # The sector in emummc.ini must match the actual offset where the emuMMC data is located.
            # We detected this offset by finding the GPT header in the source emuMMC.
            #
            # Common offsets:
            #   - 0xC001 (49153 sectors / ~24.09 MB) - Full-size emuMMC with protective offset
            #   - 0x4001 (16385 sectors / ~8.01 MB) - Resized emuMMC without full protective offset
            #
            # The calculation is:
            #   1. Start with the MBR partition start sector
            #   2. Add the detected offset from source (0xC001 or 0x4001)
            #   3. This gives the actual sector where the emuMMC data begins
            #
            # NOTE: We do NOT add an additional 0x8000 base offset or round to 0x10000 alignment,
            # because the data was copied bit-by-bit from source to target, preserving the 
            # internal structure. The sector in emummc.ini must point to where the data actually is.

            # Get MBR partition start
            mbr_partition_start = target_emummc_gpt_start

            # Calculate the emummc.ini sector based on how hekate structures emuMMC
            #
            # Hekate's emuMMC structure (standard layout):
            #   - Partition start (MBR entry)
            #   - +0x0000: Reserved/padding (cleared by hekate)
            #   - +0x8000: BOOT0 starts HERE (16MB offset) ← emummc.ini sector points here
            #   - +0xA000: BOOT1 (4MB after BOOT0)
            #   - +0xC000: USER partition MBR  
            #   - +0xC001: USER partition GPT header
            #
            # Hekate ALWAYS expects emummc.ini sector to point to partition_start + 0x8000,
            # regardless of how the emuMMC was created. This is because:
            #   1. Hekate's "Fix RAW" rewrites it to partition + 0x8000
            #   2. The emuMMC structure has a 16MB protective offset before BOOT0
            #   3. Even when bit-by-bit copying, the internal offsets are preserved
            #
            # When we do bit-by-bit copy from source, we copy the ENTIRE partition including
            # the 16MB protective offset, so BOOT0 ends up at target_partition + 0x8000.
            
            # Hekate's standard offset for BOOT0 within the emuMMC partition
            HEKATE_BOOT0_OFFSET = 0x8000
            
            # Calculate the correct sector for emummc.ini
            boot0_start_sector = mbr_partition_start + HEKATE_BOOT0_OFFSET
            emummc_ini_sector = boot0_start_sector

            logger.info(f"emuMMC sector calculation:")
            logger.info(f"  MBR partition start: 0x{mbr_partition_start:X} ({mbr_partition_start:,})")
            logger.info(f"  Hekate standard BOOT0 offset: 0x{HEKATE_BOOT0_OFFSET:X} ({HEKATE_BOOT0_OFFSET:,} sectors)")
            logger.info(f"  BOOT0 location: 0x{boot0_start_sector:X} ({boot0_start_sector:,})")
            logger.info(f"  Final sector for emummc.ini: 0x{emummc_ini_sector:X} ({emummc_ini_sector:,})")

            # Determine which RAW folder to use (RAW1, RAW2, or RAW3)
            # Based on which MBR partition the emuMMC is in
            # For now, we'll use RAW1 as default
            raw_folder_name = "RAW1"
            raw_folder_path = emummc_path / raw_folder_name

            # Create RAW folder
            raw_folder_path.mkdir(exist_ok=True)
            logger.info(f"Created RAW folder at {raw_folder_path}")

            # Create raw_based file with the sector offset
            # This file contains a 4-byte little-endian integer of the sector value for emummc.ini
            raw_based_file = raw_folder_path / "raw_based"
            with open(raw_based_file, 'wb') as f:
                # Write sector as 4-byte little-endian integer
                f.write(emummc_ini_sector.to_bytes(4, byteorder='little'))
            logger.info(f"Created raw_based file at {raw_based_file} with sector: 0x{emummc_ini_sector:x}")

            # Create emummc.ini file for hekate
            # The id field should be the RAW folder name encoded as hex (e.g., "RAW1" = 0x31574152)
            # Calculate id from folder name: "RAW1" = 0x31 0x57 0x41 0x31 (little-endian ASCII)
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

            logger.info(f"Created emummc.ini at {emummc_ini_path}")
            logger.info(f"emuMMC configuration:\n{ini_content}")

            self._report_progress("Updating emuMMC Config", 99, "✓ emuMMC config created successfully")
            logger.info("Successfully created hekate emuMMC configuration")

        except Exception as e:
            logger.error(f"Error creating emuMMC config: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self._report_progress("Updating emuMMC Config", 97, f"⚠️ Error creating emuMMC config: {e}")

    def _update_linux_boot_config(self):
        """
        Linux (L4T) boot configuration does not need updating after migration.

        L4T uses filesystem labels (e.g., root=LABEL=SWR-NOB or root=UUID=xxx)
        to identify the root partition, NOT partition numbers or absolute sectors.

        Since we perform a sector-by-sector copy of the Linux partition, the
        filesystem UUID and LABEL are automatically preserved, so L4T will boot
        correctly on the new SD card without any configuration changes.

        This function is kept as a stub for backward compatibility but does nothing.
        """
        # No action needed - filesystem labels are preserved during sector copy
        logger.info("Linux partition migrated - filesystem labels preserved, no config update needed")
        pass

    def _report_progress(self, stage: str, percent: float, message: str):
        """Report progress to callback"""
        if self.on_progress:
            self.on_progress(stage, percent, message)

    def cancel(self):
        """Cancel migration"""
        self.cancelled = True
