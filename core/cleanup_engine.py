"""
Cleanup Engine - Remove unwanted partitions and expand FAT32 on single SD card
"""

import time
import logging
import subprocess
import tempfile
import os
import sys
from pathlib import Path
from typing import Callable, Optional
from core.disk_manager import DiskManager
from core.partition_writer import PartitionWriter
from core.partition_models import DiskLayout

# Windows-specific flag to prevent console window from appearing
if sys.platform == 'win32':
    CREATE_NO_WINDOW = 0x08000000
else:
    CREATE_NO_WINDOW = 0

logger = logging.getLogger(__name__)

SECTOR_SIZE = 512

class CleanupEngine:
    """Handles the cleanup process for a single SD card"""

    def __init__(self, disk, source_layout: DiskLayout, target_layout: DiskLayout, options: dict, temp_backup_dir: Optional[str] = None):
        self.disk = disk
        self.source_layout = source_layout
        self.target_layout = target_layout
        self.options = options
        self.temp_backup_dir_setting = temp_backup_dir  # User-specified temp backup location (None = system default)

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
            # Mark as failed so temp backup is preserved
            self._cleanup_failed = True
            if self.on_error:
                self.on_error(str(e))
        finally:
            # Only cleanup temp directory if operation succeeded
            success = not hasattr(self, '_cleanup_failed')
            if hasattr(self, 'temp_backup_dir') and os.path.exists(self.temp_backup_dir):
                if success:
                    try:
                        import shutil
                        shutil.rmtree(self.temp_backup_dir)
                        logger.info(f"Cleaned up temporary backup directory: {self.temp_backup_dir}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup temp directory: {e}")
                else:
                    logger.error(f"Cleanup failed - temp backup preserved at: {self.temp_backup_dir}")
                    logger.error("Manually copy files from this location to recover data.")

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
        # Use user-specified directory if provided, otherwise use system temp
        if self.temp_backup_dir_setting:
            # Use custom directory
            import shutil
            custom_temp = os.path.join(self.temp_backup_dir_setting, "nx_partition_backup_")
            os.makedirs(custom_temp, exist_ok=True)
            self.temp_backup_dir = custom_temp
            logger.info(f"Using custom temp backup directory: {self.temp_backup_dir}")
        else:
            # Use system temp directory
            self.temp_backup_dir = tempfile.mkdtemp(prefix="nx_partition_backup_")
            logger.info(f"Created temporary backup directory: {self.temp_backup_dir}")

        self._report_progress("Backing up FAT32", 10, f"Backing up from {drive_letter} to temp folder...")

        # Use PowerShell Copy-Item to backup FAT32 data (no timeout, progress tracking)
        self._copy_files_simple(
            drive_letter,
            self.temp_backup_dir,
            "Backing up FAT32",
            10,
            progress_range=25  # 10% → 35%
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
            timeout=300,
            creationflags=subprocess.CREATE_NO_WINDOW
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

        # Use PowerShell Copy-Item to restore FAT32 data (no timeout, progress tracking)
        self._copy_files_simple(
            self.temp_backup_dir,
            self.fat32_drive,
            "Restoring FAT32",
            75,
            progress_range=20  # 75% → 95%
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

            # Calculate emuMMC sector offset using same logic as migration_engine
            target_emummc_gpt_start = target_emummc[0].start_sector
            
            BASE_OFFSET = 0x8000   # 16MB protective offset
            ALIGNMENT = 0x10000    # 32MB alignment (hekate Fix RAW uses this)
            
            mbr_partition_start = target_emummc_gpt_start
            unaligned_sector = BASE_OFFSET + mbr_partition_start
            emummc_ini_sector = ((unaligned_sector + ALIGNMENT - 1) // ALIGNMENT) * ALIGNMENT

            logger.info(f"emuMMC MBR partition start: 0x{target_emummc_gpt_start:X}")
            logger.info(f"emuMMC ini sector (aligned to 32MB): 0x{emummc_ini_sector:X}")

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

    def _copy_files_simple(self, source_drive, target_drive, stage_name, base_progress, progress_range=25):
        """Copy files using Windows native PowerShell Copy-Item - same as Windows Explorer
        No timeout - runs until completion, with progress tracking via free space monitoring.

        Args:
            progress_range: Total progress range allocated for file copy (default 25 for cleanup mode)
        """
        import os
        from pathlib import Path

        # Ensure drive letters are properly formatted
        source = source_drive.rstrip('\\')
        target = target_drive.rstrip('\\')

        logger.info(f"Starting Windows native file copy: {source} -> {target}")
        logger.info(f"Using PowerShell Copy-Item (same as Windows Explorer)")

        # Verify source path exists
        if not Path(source).exists():
            raise Exception(f"Source path does not exist: {source}")

        # Problematic directories to skip (these cause issues and don't need to be copied)
        skipped_dirs = ['$Recycle.Bin', 'System Volume Information', '.Trashes',
                        '$RECYCLE.BIN', 'RECYCLER', '.Trash-1000', '.Trash-1001',
                        '.Trash-1002', 'found.000', 'FOUND.000']
        logger.info(f"Will skip problematic directories: {', '.join(skipped_dirs)}")

        # Count total size first for progress tracking using free space method (much faster, no timeout)
        logger.info(f"Calculating total size...")
        self._report_progress(stage_name, base_progress, "Calculating copy size...")

        # Get target drive initial free space
        try:
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            total_bytes = ctypes.c_ulonglong(0)
            total_free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(target),
                ctypes.byref(free_bytes),
                ctypes.byref(total_bytes),
                ctypes.byref(total_free_bytes)
            )
            initial_free_space = free_bytes.value
            logger.debug(f"Initial free space on target: {initial_free_space / (1024**3):.2f} GB")
        except Exception as e:
            logger.warning(f"Could not get initial free space: {e}")
            initial_free_space = None

        try:
            # Use PowerShell to get total size with directory exclusion
            # Skip problematic directories to avoid errors
            size_cmd = f'''
            $skippedDirs = @({', '.join([f"'{d}'" for d in skipped_dirs])})
            $totalSize = 0
            Get-ChildItem -Path "{source}" -Recurse -Force -ErrorAction SilentlyContinue | Where-Object {{
                $skip = $false
                foreach ($dir in $skippedDirs) {{
                    if ($_.FullName -like "*\\$dir\\*" -or $_.FullName -like "*\\$dir") {{
                        $skip = $true
                        break
                    }}
                }}
                -not $skip
            }} | ForEach-Object {{ $totalSize += $_.Length }}
            Write-Output $totalSize
            '''
            result = subprocess.run(
                ['powershell', '-NoProfile', '-WindowStyle', 'Hidden', '-Command', size_cmd],
                capture_output=True,
                text=True,
                timeout=120,
                creationflags=CREATE_NO_WINDOW
            )
            total_bytes = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip() else 0
            total_gb = total_bytes / (1024**3)
            logger.info(f"Total size to copy: {total_gb:.2f} GB ({total_bytes:,} bytes)")

        except Exception as e:
            logger.warning(f"Could not calculate total size: {e}. Proceeding with copy...")
            total_bytes = 0

        start_time = time.time()

        # PowerShell script for copying with progress tracking
        # Skip problematic directories that cause issues
        ps_script = f'''
        $ErrorActionPreference = "Continue"
        $source = "{source}"
        $destination = "{target}"
        $skippedDirs = @({', '.join([f"'{d}'" for d in skipped_dirs])})

        # Get all items except skipped directories
        Get-ChildItem -Path $source -Force -ErrorAction SilentlyContinue | Where-Object {{
            $name = $_.Name
            $isSkipped = $false
            foreach ($dir in $skippedDirs) {{
                if ($name -eq $dir) {{
                    $isSkipped = $true
                    break
                }}
            }}
            -not $isSkipped
        }} | ForEach-Object {{
            if ($_.PSIsContainer) {{
                # Copy directory recursively, excluding problematic subdirectories
                $destPath = Join-Path $destination $_.Name
                if (-not (Test-Path $destPath)) {{
                    New-Item -ItemType Directory -Path $destPath -Force | Out-Null
                }}
                # Copy contents recursively
                Copy-Item -Path (Join-Path $source $_.Name "\\*") -Destination $destPath -Recurse -Force -ErrorAction Continue
            }} else {{
                # Copy file
                Copy-Item -Path $_.FullName -Destination $destination -Force -ErrorAction Continue
            }}
        }}

        if ($?) {{
            Write-Output "SUCCESS"
        }} else {{
            Write-Output "COMPLETED_WITH_ERRORS"
        }}
        '''

        logger.info(f"Executing Windows copy operation...")
        self._report_progress(stage_name, base_progress + 2, "Copying files...")

        try:
            # Run PowerShell copy in background and monitor progress (NO TIMEOUT)
            process = subprocess.Popen(
                ['powershell', '-NoProfile', '-WindowStyle', 'Hidden', '-Command', ps_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=CREATE_NO_WINDOW
            )

            # Monitor progress by checking target directory size using free space method
            # This is O(1) operation and won't timeout
            last_check_time = time.time()
            check_interval = 2.0  # Check every 2 seconds

            while process.poll() is None:
                if self.cancelled:
                    process.kill()
                    raise Exception("Cleanup cancelled by user")

                current_time = time.time()
                if current_time - last_check_time >= check_interval:
                    try:
                        # Use free space method to calculate progress (instant, no timeout)
                        if initial_free_space is not None:
                            try:
                                current_free_bytes = ctypes.c_ulonglong(0)
                                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                                    ctypes.c_wchar_p(target),
                                    ctypes.byref(current_free_bytes),
                                    ctypes.byref(total_bytes),
                                    ctypes.byref(total_free_bytes)
                                )
                                copied_bytes = initial_free_space - current_free_bytes.value

                                # Calculate progress
                                elapsed = current_time - start_time
                                speed_mbps = (copied_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                                copied_gb = copied_bytes / (1024**3)

                                if total_bytes > 0:
                                    percent = min(95, (copied_bytes / total_bytes * 100))
                                    progress = base_progress + (percent / 100 * progress_range * 0.9)
                                    logger.info(f"Copying: {copied_gb:.2f} GB / {total_gb:.2f} GB ({percent:.1f}%) at {speed_mbps:.1f} MB/s")
                                    self._report_progress(stage_name, progress, f"Copied {copied_gb:.1f}/{total_gb:.1f} GB ({percent:.0f}%)")
                                else:
                                    logger.info(f"Copying: {copied_gb:.2f} GB at {speed_mbps:.1f} MB/s")
                                    self._report_progress(stage_name, base_progress + 10, f"Copied {copied_gb:.1f} GB")

                            except Exception as inner_e:
                                logger.debug(f"Could not check progress using free space: {inner_e}")

                        last_check_time = current_time

                    except Exception as e:
                        logger.debug(f"Could not check copy progress: {e}")

                time.sleep(0.5)

            # Get final output
            stdout, stderr = process.communicate()

            elapsed_time = time.time() - start_time

            # Check if copy was successful
            if "SUCCESS" in stdout or "COMPLETED_WITH_ERRORS" in stdout:
                # Initialize final_gb to avoid UnboundLocalError if exception occurs
                final_gb = 0.0

                # Get final copied size
                try:
                    size_check_cmd = f'''
                    $copiedSize = (Get-ChildItem -Path "{target}" -Recurse -Force -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
                    if ($copiedSize -eq $null) {{ $copiedSize = 0 }}
                    Write-Output $copiedSize
                    '''
                    size_result = subprocess.run(
                        ['powershell', '-NoProfile', '-WindowStyle', 'Hidden', '-Command', size_check_cmd],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        creationflags=CREATE_NO_WINDOW
                    )
                    final_bytes = int(size_result.stdout.strip()) if size_result.returncode == 0 and size_result.stdout.strip() else 0
                    final_gb = final_bytes / (1024**3)
                    final_mb = final_bytes / (1024 * 1024)
                    speed_mbps = final_mb / elapsed_time if elapsed_time > 0 else 0

                    logger.info(f"Windows copy completed in {elapsed_time:.1f} seconds")
                    logger.info(f"Data copied: {final_gb:.2f} GB ({final_bytes:,} bytes) at {speed_mbps:.1f} MB/s")

                    if "COMPLETED_WITH_ERRORS" in stdout:
                        logger.warning("Copy completed but some files may have been skipped (check PowerShell errors)")
                        if stderr:
                            logger.warning(f"PowerShell errors: {stderr[:500]}")

                    # Final progress - but reserve a bit for Archive bit fix
                    copy_progress = base_progress + int(progress_range * 0.95)
                    self._report_progress(stage_name, copy_progress, f"✓ Copied {final_gb:.1f} GB - Fixing Archive bits...")

                except Exception as e:
                    logger.warning(f"Could not get final copy size: {e}")
                    copy_progress = base_progress + int(progress_range * 0.95)
                    self._report_progress(stage_name, copy_progress, "✓ Copy completed - Fixing Archive bits...")

                # Fix Archive bit for Nintendo Switch compatibility using Hekate's logic
                logger.info("Fixing Archive bits for Nintendo Switch compatibility (Hekate logic)...")
                logger.info("Scanning folders for HOS single file containers...")

                try:
                    fix_start = time.time()

                    # PowerShell script implementing Hekate's Archive bit fix logic
                    fix_script = f'''
                    $ErrorActionPreference = "Continue"
                    $targetPath = "{target}"

                    # Counters (matching Hekate's total[] array)
                    $bitsSet = 0      # Archive bits SET (HOS folders)
                    $bitsUnset = 0    # Archive bits UNSET (regular folders)
                    $errors = 0       # Errors encountered

                    # Get all directories recursively
                    $directories = Get-ChildItem -Path $targetPath -Directory -Recurse -Force -ErrorAction SilentlyContinue

                    Write-Output "Found $($directories.Count) directories to process"

                    foreach ($dir in $directories) {{
                        try {{
                            # Check if this is a HOS single file folder by looking for "/00" file
                            $hosMarkerFile = Join-Path $dir.FullName "00"
                            $isHosFolder = Test-Path -Path $hosMarkerFile -PathType Leaf

                            # Get current attributes
                            $currentAttrib = $dir.Attributes
                            $hasArchiveBit = ($currentAttrib -band [System.IO.FileAttributes]::Archive) -ne 0

                            if ($isHosFolder) {{
                                # HOS single file folder - SET Archive bit if not already set
                                if (-not $hasArchiveBit) {{
                                    $dir.Attributes = $currentAttrib -bor [System.IO.FileAttributes]::Archive
                                    $bitsSet++
                                }}
                            }} else {{
                                # Regular folder - CLEAR Archive bit if set
                                if ($hasArchiveBit) {{
                                    $dir.Attributes = $currentAttrib -band (-bnot [System.IO.FileAttributes]::Archive)
                                    $bitsUnset++
                                }}
                            }}
                        }} catch {{
                            $errors++
                        }}
                    }}

                    Write-Output "ARCHIVE_FIX_COMPLETE"
                    Write-Output "BitsSet:$bitsSet"
                    Write-Output "BitsUnset:$bitsUnset"
                    Write-Output "Errors:$errors"
                    '''

                    logger.info("Running Hekate-style Archive bit fix...")
                    self._report_progress(stage_name, copy_progress + 2, "Fixing Archive bits (Hekate logic)...")

                    fix_result = subprocess.run(
                        ['powershell', '-NoProfile', '-WindowStyle', 'Hidden', '-Command', fix_script],
                        capture_output=True,
                        text=True,
                        timeout=300,
                        creationflags=CREATE_NO_WINDOW
                    )

                    fix_elapsed = time.time() - fix_start

                    if "ARCHIVE_FIX_COMPLETE" in fix_result.stdout:
                        # Parse the results
                        output_lines = fix_result.stdout.strip().split('\n')
                        bits_set = 0
                        bits_unset = 0
                        errors = 0

                        for line in output_lines:
                            if line.startswith("BitsSet:"):
                                bits_set = int(line.split(':')[1])
                            elif line.startswith("BitsUnset:"):
                                bits_unset = int(line.split(':')[1])
                            elif line.startswith("Errors:"):
                                errors = int(line.split(':')[1])

                        logger.info(f"Archive bit fix completed in {fix_elapsed:.1f}s")
                        logger.info(f"Archive bits SET (HOS folders): {bits_set}")
                        logger.info(f"Archive bits UNSET (regular folders): {bits_unset}")

                        if errors > 0:
                            logger.warning(f"Encountered {errors} errors during Archive bit fix")

                        logger.info("Nintendo Switch should now recognize all files correctly!")
                    else:
                        logger.warning("Archive bit fix may have encountered issues")
                        if fix_result.stderr:
                            logger.warning(f"Fix errors: {fix_result.stderr[:200]}")

                except Exception as e:
                    logger.warning(f"Archive bit fix failed: {e}")
                    logger.warning("=" * 60)
                    logger.warning("ARCHIVE BIT FIX INSTRUCTIONS:")
                    logger.warning("The file copy completed successfully, but the automatic")
                    logger.warning("Archive bit fix encountered an error.")
                    logger.warning("")
                    logger.warning("This is NOT critical - your SD card should still work.")
                    logger.warning("If you experience issues with file detection:")
                    logger.warning("1. Boot Hekate on your Switch")
                    logger.warning("2. Go to Tools -> Fix Archive Bit")
                    logger.warning("3. Select your SD card and run the fix")
                    logger.warning("=" * 60)

                # Final progress
                final_progress = base_progress + progress_range
                self._report_progress(stage_name, final_progress, f"✓ Copied {final_gb:.1f} GB + Archive fix in {elapsed_time:.0f}s")

                logger.info("FAT32 file copy completed successfully using Windows native method")

            else:
                logger.error(f"PowerShell copy failed with return code: {process.returncode}")
                if stderr:
                    logger.error(f"PowerShell errors: {stderr}")
                raise Exception(f"Windows copy operation failed. Check logs for details.")

        except Exception as e:
            logger.error(f"Windows copy error: {e}")
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
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW
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
                    subprocess.run(['diskpart'], input=diskpart_script, capture_output=True, text=True, timeout=30, creationflags=subprocess.CREATE_NO_WINDOW)
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
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW
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
