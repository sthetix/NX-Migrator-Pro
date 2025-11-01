"""
Disk Manager - List and access physical disks on Windows
"""

import sys
import os
import subprocess
import logging
import time

if sys.platform == 'win32':
    import wmi
    import win32file
    import win32api
    import pywintypes
    import winioctlcon

logger = logging.getLogger(__name__)

class DiskManager:
    """Manages disk enumeration and access"""

    def __init__(self):
        if sys.platform != 'win32':
            raise RuntimeError("This tool only supports Windows")

        try:
            self.wmi = wmi.WMI()
        except Exception as e:
            error_msg = str(e)
            # Provide more helpful error message
            if 'winmgmts' in error_msg.lower():
                raise RuntimeError(
                    "Failed to connect to Windows Management Instrumentation (WMI).\n\n"
                    "Possible solutions:\n"
                    "1. Restart the 'Windows Management Instrumentation' service\n"
                    "2. Run: net stop winmgmt && net start winmgmt (as admin)\n"
                    "3. Check if antivirus is blocking WMI access\n"
                    "4. Repair WMI repository: winmgmt /salvagerepository"
                )
            else:
                raise RuntimeError(f"Failed to initialize disk manager: {error_msg}")

    def list_disks(self):
        """
        List all physical disks
        Returns list of dict with disk info
        """
        disks = []

        try:
            for disk in self.wmi.Win32_DiskDrive():
                # Get disk properties
                disk_info = {
                    'name': disk.Caption or disk.Model or f"Disk {disk.Index}",
                    'path': f"\\\\.\\PhysicalDrive{disk.Index}",
                    'index': disk.Index,
                    'size_bytes': int(disk.Size) if disk.Size else 0,
                    'size_gb': int(disk.Size) / (1024**3) if disk.Size else 0,
                    'removable': disk.MediaType and 'Removable' in disk.MediaType,
                    'interface': disk.InterfaceType or 'Unknown'
                }

                disks.append(disk_info)

        except Exception as e:
            error_msg = str(e)
            if 'winmgmts' in error_msg.lower() or 'win32_diskdrive' in error_msg.lower():
                raise RuntimeError(
                    "Failed to query disk drives from WMI.\n\n"
                    "This may be a WMI service issue. Try:\n"
                    "1. Restart Windows Management Instrumentation service\n"
                    "2. Check Windows Event Viewer for WMI errors\n"
                    "3. Run as Administrator if not already"
                )
            else:
                raise RuntimeError(f"Failed to list disks: {error_msg}")

        return disks

    def list_drive_letters(self):
        """
        List all available drive letters with their partition info
        Returns list of dict with drive info (only removable/SD card drives)
        """
        drives = []

        try:
            # Get all logical disks
            for partition in self.wmi.Win32_DiskPartition():
                # Get associated logical disks (drive letters)
                for logical_disk in partition.associators("Win32_LogicalDiskToPartition"):
                    # Get the physical disk this partition belongs to
                    for physical_disk in partition.associators("Win32_DiskDriveToDiskPartition"):
                        # Only include removable media (SD cards, USB drives)
                        if physical_disk.MediaType and 'Removable' in physical_disk.MediaType:
                            drive_info = {
                                'letter': logical_disk.DeviceID,  # e.g., "H:"
                                'name': logical_disk.VolumeName or logical_disk.DeviceID,
                                'physical_drive': f"\\\\.\\PhysicalDrive{physical_disk.Index}",
                                'physical_index': physical_disk.Index,
                                'disk_name': physical_disk.Caption or physical_disk.Model or f"Disk {physical_disk.Index}",
                                'size_bytes': int(physical_disk.Size) if physical_disk.Size else 0,
                                'size_gb': int(physical_disk.Size) / (1024**3) if physical_disk.Size else 0,
                                'partition_size_bytes': int(logical_disk.Size) if logical_disk.Size else 0,
                                'partition_size_gb': int(logical_disk.Size) / (1024**3) if logical_disk.Size else 0,
                                'file_system': logical_disk.FileSystem or 'Unknown'
                            }
                            drives.append(drive_info)

        except Exception as e:
            error_msg = str(e)
            if 'winmgmts' in error_msg.lower():
                raise RuntimeError(
                    "Failed to query drives from WMI.\n\n"
                    "This may be a WMI service issue. Try:\n"
                    "1. Restart Windows Management Instrumentation service\n"
                    "2. Check Windows Event Viewer for WMI errors\n"
                    "3. Run as Administrator if not already"
                )
            else:
                raise RuntimeError(f"Failed to list drive letters: {error_msg}")

        return drives

    def get_physical_drive_from_letter(self, drive_letter):
        """
        Map a drive letter (e.g., 'H:') to its physical drive path
        Returns dict with physical drive info
        """
        if not drive_letter.endswith(':'):
            drive_letter += ':'

        try:
            # Find the logical disk
            logical_disks = self.wmi.query(f"SELECT * FROM Win32_LogicalDisk WHERE DeviceID='{drive_letter}'")
            if not logical_disks:
                raise ValueError(f"Drive {drive_letter} not found")

            logical_disk = logical_disks[0]

            # Get the partition
            partitions = self.wmi.query(
                f"ASSOCIATORS OF {{Win32_LogicalDisk.DeviceID='{drive_letter}'}} "
                f"WHERE AssocClass=Win32_LogicalDiskToPartition"
            )

            if not partitions:
                raise ValueError(f"No partition found for drive {drive_letter}")

            partition = partitions[0]

            # Get the physical disk
            physical_disks = self.wmi.query(
                f"ASSOCIATORS OF {{Win32_DiskPartition.DeviceID='{partition.DeviceID}'}} "
                f"WHERE AssocClass=Win32_DiskDriveToDiskPartition"
            )

            if not physical_disks:
                raise ValueError(f"No physical disk found for drive {drive_letter}")

            physical_disk = physical_disks[0]

            return {
                'letter': drive_letter,
                'name': physical_disk.Caption or physical_disk.Model or f"Disk {physical_disk.Index}",
                'path': f"\\\\.\\PhysicalDrive{physical_disk.Index}",
                'index': physical_disk.Index,
                'size_bytes': int(physical_disk.Size) if physical_disk.Size else 0,
                'size_gb': int(physical_disk.Size) / (1024**3) if physical_disk.Size else 0,
                'partition_size_bytes': int(logical_disk.Size) if logical_disk.Size else 0,
                'partition_size_gb': int(logical_disk.Size) / (1024**3) if logical_disk.Size else 0,
            }

        except Exception as e:
            raise RuntimeError(f"Failed to map drive letter {drive_letter}: {str(e)}")

    def read_sectors(self, disk_path, start_sector, count):
        """
        Read sectors from disk
        Returns bytes
        """
        SECTOR_SIZE = 512

        try:
            # Open disk for reading
            handle = win32file.CreateFile(
                disk_path,
                win32file.GENERIC_READ,
                win32file.FILE_SHARE_READ | win32file.FILE_SHARE_WRITE,
                None,
                win32file.OPEN_EXISTING,
                0,
                None
            )

            # Seek to position
            offset = start_sector * SECTOR_SIZE
            win32file.SetFilePointer(handle, offset, win32file.FILE_BEGIN)

            # Read data
            size = count * SECTOR_SIZE
            error_code, data = win32file.ReadFile(handle, size)

            win32file.CloseHandle(handle)

            if error_code != 0:
                raise IOError(f"Read error: {error_code}")

            return data

        except pywintypes.error as e:
            raise IOError(f"Failed to read from disk: {e}")

    def write_sectors(self, disk_path, start_sector, data, skip_prepare=False):
        """
        Write sectors to disk
        data: bytes to write (must be multiple of 512)
        skip_prepare: Skip disk preparation (used for batch writes)
        """
        SECTOR_SIZE = 512
        MAX_RETRIES = 3
        RETRY_DELAY = 1.0  # seconds

        if len(data) % SECTOR_SIZE != 0:
            raise ValueError("Data size must be multiple of 512 bytes")

        logger.debug(f"write_sectors: disk={disk_path}, start={start_sector}, size={len(data)} bytes")

        for attempt in range(MAX_RETRIES):
            handle = None
            try:
                # Log attempt
                if attempt > 0:
                    logger.warning(f"Retry attempt {attempt + 1}/{MAX_RETRIES} for sector {start_sector}")

                # First, try to lock and dismount the volume (only if not skipped and first attempt)
                if attempt == 0 and not skip_prepare:
                    self._prepare_disk_for_write(disk_path)

                # Open disk for writing
                logger.debug(f"Opening disk {disk_path} for writing...")

                # Try to open with different access modes
                access_modes = [
                    # Mode 1: Exclusive, no buffering (strictest)
                    {
                        'desc': 'exclusive + no buffering',
                        'access': win32file.GENERIC_WRITE | win32file.GENERIC_READ,
                        'share': 0,
                        'flags': win32file.FILE_FLAG_WRITE_THROUGH | win32file.FILE_FLAG_NO_BUFFERING
                    },
                    # Mode 2: Shared read, no buffering
                    {
                        'desc': 'shared read + no buffering',
                        'access': win32file.GENERIC_WRITE | win32file.GENERIC_READ,
                        'share': win32file.FILE_SHARE_READ,
                        'flags': win32file.FILE_FLAG_WRITE_THROUGH | win32file.FILE_FLAG_NO_BUFFERING
                    },
                    # Mode 3: Standard buffered write
                    {
                        'desc': 'exclusive + buffered',
                        'access': win32file.GENERIC_WRITE | win32file.GENERIC_READ,
                        'share': 0,
                        'flags': win32file.FILE_FLAG_WRITE_THROUGH
                    }
                ]

                handle = None
                last_error = None

                for mode in access_modes:
                    try:
                        logger.debug(f"Trying to open with mode: {mode['desc']}")
                        handle = win32file.CreateFile(
                            disk_path,
                            mode['access'],
                            mode['share'],
                            None,
                            win32file.OPEN_EXISTING,
                            mode['flags'],
                            None
                        )
                        logger.debug(f"Successfully opened disk handle: {handle} with mode: {mode['desc']}")
                        break
                    except pywintypes.error as e:
                        last_error = e
                        logger.debug(f"Failed to open with mode {mode['desc']}: {e}")

                if handle is None:
                    raise last_error

                # Seek to position
                offset = start_sector * SECTOR_SIZE
                logger.debug(f"Seeking to offset {offset} (sector {start_sector})...")
                new_pos = win32file.SetFilePointer(handle, offset, win32file.FILE_BEGIN)
                logger.debug(f"Seek successful, new position: {new_pos}")

                # Write data
                logger.debug(f"Writing {len(data)} bytes...")
                error_code, bytes_written = win32file.WriteFile(handle, data)
                logger.debug(f"Write completed: error_code={error_code}, bytes_written={bytes_written}")

                # Flush to ensure data is written
                win32file.FlushFileBuffers(handle)
                logger.debug("Flushed file buffers")

                # Close handle
                win32file.CloseHandle(handle)
                handle = None
                logger.debug("Closed disk handle")

                # Check for errors
                if error_code != 0:
                    raise IOError(f"Write error code: {error_code}")

                if bytes_written != len(data):
                    raise IOError(f"Incomplete write: {bytes_written}/{len(data)} bytes")

                # Success
                logger.debug(f"Successfully wrote {bytes_written} bytes to sector {start_sector}")
                return

            except pywintypes.error as e:
                # Close handle if open
                if handle is not None:
                    try:
                        win32file.CloseHandle(handle)
                    except:
                        pass

                error_code = e.winerror if hasattr(e, 'winerror') else None
                error_msg = e.strerror if hasattr(e, 'strerror') else str(e)

                logger.error(f"Write failed (attempt {attempt + 1}/{MAX_RETRIES}): error_code={error_code}, msg={error_msg}")
                logger.error(f"Details: disk={disk_path}, sector={start_sector}, size={len(data)}")

                # Error 5 = Access Denied
                if error_code == 5:
                    logger.error("ACCESS DENIED (Error 5) - Possible causes:")
                    logger.error("  1. Disk is locked by Windows/Explorer")
                    logger.error("  2. Volume is mounted and in use")
                    logger.error("  3. Insufficient permissions (not running as admin)")
                    logger.error("  4. Disk has read-only attribute")
                    logger.error("  5. Anti-virus or disk protection software blocking access")

                    # Check if disk is online
                    self._check_disk_status(disk_path)

                # Error 32 = Sharing violation
                elif error_code == 32:
                    logger.error("SHARING VIOLATION (Error 32) - Disk is in use by another process")

                # If last attempt, raise the error
                if attempt == MAX_RETRIES - 1:
                    detailed_error = (
                        f"Failed to write to disk after {MAX_RETRIES} attempts.\n"
                        f"Error code: {error_code}\n"
                        f"Error message: {error_msg}\n"
                        f"Disk: {disk_path}\n"
                        f"Sector: {start_sector}\n\n"
                        f"The target disk may be in an inconsistent state.\n\n"
                    )

                    if error_code == 5:
                        detailed_error += (
                            "ACCESS DENIED - Possible solutions:\n"
                            "1. Close File Explorer and any programs accessing the disk\n"
                            "2. Unmount/eject all drive letters from Disk Management\n"
                            "3. Run 'diskpart' > 'list disk' > 'select disk N' > 'clean' (WARNING: destroys data)\n"
                            "4. Check disk properties for read-only attribute\n"
                            "5. Disable anti-virus temporarily\n"
                            "6. Ensure running as Administrator"
                        )

                    raise IOError(detailed_error)

                # Wait before retry
                logger.info(f"Waiting {RETRY_DELAY}s before retry...")
                time.sleep(RETRY_DELAY)

            except Exception as e:
                # Close handle if open
                if handle is not None:
                    try:
                        win32file.CloseHandle(handle)
                    except:
                        pass

                logger.error(f"Unexpected error in write_sectors: {type(e).__name__}: {e}")
                raise IOError(f"Failed to write to disk: {e}")

    def clean_disk(self, disk_path):
        """
        Delete all partitions on the disk using diskpart clean command
        This is essential before migration to release all Windows locks
        """
        try:
            logger.info(f"Cleaning disk {disk_path} (deleting all partitions)...")

            # Extract disk index
            disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")
            logger.debug(f"Disk index: {disk_index}")

            # Use diskpart to clean the disk
            diskpart_script = f"select disk {disk_index}\nclean\n"

            logger.info("Running diskpart clean command...")
            result = subprocess.run(
                ['diskpart'],
                input=diskpart_script,
                capture_output=True,
                text=True,
                timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW
            )

            if result.returncode == 0:
                logger.info("Successfully cleaned disk with diskpart")
                logger.info("All partitions have been deleted")
                logger.info("Waiting for Windows to update partition cache...")
                time.sleep(2)  # Give Windows time to update partition cache
                return True
            else:
                logger.error(f"Diskpart clean failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("Diskpart clean command timed out")
            return False
        except Exception as e:
            logger.error(f"Error cleaning disk: {e}")
            return False

    def _prepare_disk_for_write(self, disk_path):
        """
        Attempt to prepare disk for writing by locking volumes and updating disk cache
        """
        try:
            logger.info(f"Preparing disk {disk_path} for write operations...")

            # Extract disk index
            disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")
            logger.debug(f"Disk index: {disk_index}")

            # First, try using diskpart to offline/online the disk (most reliable method)
            try:
                logger.info("Attempting to refresh disk using diskpart...")
                diskpart_script = f"select disk {disk_index}\noffline disk noerr\nonline disk noerr\n"

                result = subprocess.run(
                    ['diskpart'],
                    input=diskpart_script,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )

                if result.returncode == 0:
                    logger.info("Successfully refreshed disk with diskpart")
                    time.sleep(0.5)  # Give Windows time to update
                else:
                    logger.warning(f"Diskpart returned error: {result.stderr}")

            except Exception as e:
                logger.warning(f"Could not use diskpart to refresh disk: {e}")

            # Try to lock all volumes on this disk
            try:
                # Query for all partitions on this disk
                partitions = self.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )

                logger.debug(f"Found {len(partitions)} partitions on disk {disk_index}")

                for partition in partitions:
                    # Get associated logical disks
                    logical_disks = self.wmi.query(
                        f"ASSOCIATORS OF {{Win32_DiskPartition.DeviceID='{partition.DeviceID}'}} "
                        f"WHERE AssocClass=Win32_LogicalDiskToPartition"
                    )

                    for logical_disk in logical_disks:
                        volume_path = f"\\\\.\\{logical_disk.DeviceID}"
                        logger.info(f"Attempting to lock volume {volume_path}...")

                        try:
                            # Open volume
                            vol_handle = win32file.CreateFile(
                                volume_path,
                                win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                                win32file.FILE_SHARE_READ | win32file.FILE_SHARE_WRITE,
                                None,
                                win32file.OPEN_EXISTING,
                                0,
                                None
                            )

                            # Try to lock volume
                            try:
                                win32file.DeviceIoControl(
                                    vol_handle,
                                    winioctlcon.FSCTL_LOCK_VOLUME,
                                    None,
                                    0
                                )
                                logger.info(f"Successfully locked volume {volume_path}")

                                # Try to dismount
                                try:
                                    win32file.DeviceIoControl(
                                        vol_handle,
                                        winioctlcon.FSCTL_DISMOUNT_VOLUME,
                                        None,
                                        0
                                    )
                                    logger.info(f"Successfully dismounted volume {volume_path}")
                                except:
                                    logger.warning(f"Could not dismount volume {volume_path}")

                            except pywintypes.error as e:
                                logger.warning(f"Could not lock volume {volume_path}: {e}")

                            win32file.CloseHandle(vol_handle)

                        except pywintypes.error as e:
                            logger.warning(f"Could not open volume {volume_path}: {e}")

            except Exception as e:
                # Don't log full exception details for COM errors (they're expected after disk clean)
                error_str = str(e)
                if "COM Error" in error_str or "-2147352567" in error_str:
                    logger.debug(f"Could not enumerate partitions (WMI cache may be stale after disk operations): {type(e).__name__}")
                else:
                    logger.warning(f"Could not enumerate partitions for locking: {e}")

            # Check for write protection
            try:
                logger.debug("Checking disk write protection status...")
                disk_handle = win32file.CreateFile(
                    disk_path,
                    win32file.GENERIC_READ,
                    win32file.FILE_SHARE_READ | win32file.FILE_SHARE_WRITE,
                    None,
                    win32file.OPEN_EXISTING,
                    0,
                    None
                )

                # IOCTL_DISK_IS_WRITABLE = 0x00070024
                try:
                    result = win32file.DeviceIoControl(
                        disk_handle,
                        0x00070024,  # IOCTL_DISK_IS_WRITABLE
                        None,
                        0
                    )
                    logger.info("Disk is writable (no write protection detected)")
                except pywintypes.error as e:
                    if e.winerror == 19:  # ERROR_WRITE_PROTECT
                        logger.error("DISK IS WRITE PROTECTED! Remove write protection before proceeding.")
                        logger.error("Check for physical write-protect switch on SD card or card reader.")
                    else:
                        logger.warning(f"Could not check write protection: {e}")

                # IOCTL_DISK_UPDATE_PROPERTIES = 0x00070140
                try:
                    win32file.DeviceIoControl(
                        disk_handle,
                        0x00070140,  # IOCTL_DISK_UPDATE_PROPERTIES
                        None,
                        0
                    )
                    logger.debug("Successfully sent IOCTL_DISK_UPDATE_PROPERTIES")
                except Exception as e2:
                    logger.debug(f"Could not send IOCTL_DISK_UPDATE_PROPERTIES: {e2}")

                win32file.CloseHandle(disk_handle)

            except Exception as e:
                logger.debug(f"Could not check disk properties: {e}")

            logger.info("Disk preparation complete")

        except Exception as e:
            logger.warning(f"Error preparing disk for write: {e}")
            # Don't fail here - just log and continue

    def _check_disk_status(self, disk_path):
        """
        Check and log disk status for debugging
        """
        try:
            logger.info("Checking disk status...")

            # Extract disk index
            disk_index = disk_path.replace("\\\\.\\PhysicalDrive", "")

            # Query disk information
            disks = self.wmi.query(f"SELECT * FROM Win32_DiskDrive WHERE Index={disk_index}")

            if disks:
                disk = disks[0]
                logger.info(f"Disk Name: {disk.Caption}")
                logger.info(f"Disk Status: {disk.Status}")
                logger.info(f"Disk Availability: {disk.Availability}")
                logger.info(f"Disk Size: {disk.Size}")
                logger.info(f"Interface Type: {disk.InterfaceType}")

                # Check partitions
                partitions = self.wmi.query(
                    f"SELECT * FROM Win32_DiskPartition WHERE DiskIndex={disk_index}"
                )
                logger.info(f"Number of partitions: {len(partitions)}")

                for idx, partition in enumerate(partitions):
                    logger.info(f"  Partition {idx}: {partition.DeviceID}, Size: {partition.Size}")

                    # Check if mounted
                    logical_disks = self.wmi.query(
                        f"ASSOCIATORS OF {{Win32_DiskPartition.DeviceID='{partition.DeviceID}'}} "
                        f"WHERE AssocClass=Win32_LogicalDiskToPartition"
                    )

                    for ld in logical_disks:
                        logger.warning(f"    MOUNTED AS: {ld.DeviceID} - This may prevent write access!")

        except Exception as e:
            logger.error(f"Error checking disk status: {e}")

    def get_disk_size(self, disk_path):
        """
        Get disk size in bytes
        """
        try:
            handle = win32file.CreateFile(
                disk_path,
                0,
                win32file.FILE_SHARE_READ | win32file.FILE_SHARE_WRITE,
                None,
                win32file.OPEN_EXISTING,
                0,
                None
            )

            # Get disk geometry
            geometry = win32file.DeviceIoControl(
                handle,
                winioctlcon.IOCTL_DISK_GET_DRIVE_GEOMETRY,
                None,
                24
            )

            win32file.CloseHandle(handle)

            # Parse geometry (simple approach)
            import struct
            cylinders, media_type, tracks_per_cylinder, sectors_per_track, bytes_per_sector = struct.unpack('QIIII', geometry)

            total_size = cylinders * tracks_per_cylinder * sectors_per_track * bytes_per_sector

            return total_size

        except Exception as e:
            # Fallback: get from WMI
            try:
                # Query WMI for disk information
                disks = self.wmi.query("SELECT Index, Size FROM Win32_DiskDrive")
                for disk in disks:
                    if f"\\\\.\\PhysicalDrive{disk.Index}" == disk_path:
                        return int(disk.Size) if disk.Size else 0

                # If disk not found, it means the disk was removed or doesn't exist
                raise IOError(
                    f"Disk {disk_path} not found.\n\n"
                    "The disk may have been disconnected or removed.\n"
                    "Please click 'Refresh Disks' and select the disk again."
                )

            except IOError:
                # Re-raise our custom IOError
                raise
            except Exception as wmi_error:
                raise IOError(f"Failed to get disk size for {disk_path}: {wmi_error}")
