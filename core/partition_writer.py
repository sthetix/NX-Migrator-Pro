"""
Partition Table Writer - Write MBR/GPT partition tables
Creates hekate-compatible partition tables
"""

import struct
import zlib
import os
from core.partition_models import DiskLayout

SECTOR_SIZE = 512

# Type GUIDs (from hekate)
GUID_FAT32 = bytes([0xA2, 0xA0, 0xD0, 0xEB, 0xE5, 0xB9, 0x33, 0x44,
                     0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7])

GUID_LINUX = bytes([0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47,
                     0x8E, 0x79, 0x3D, 0x69, 0xD8, 0x47, 0x7D, 0xE4])

GUID_EMUMMC = bytes([0x00, 0x7E, 0xCA, 0x11, 0x00, 0x00, 0x00, 0x00,
                      0x00, 0x00, ord('e'), ord('m'), ord('u'), ord('M'),
                      ord('M'), ord('C')])

class PartitionWriter:
    """Writes hekate-compatible partition tables"""

    def __init__(self, disk_manager):
        self.disk_manager = disk_manager

    def write_partition_table(self, disk_path: str, layout: DiskLayout):
        """
        Write complete partition table (MBR + GPT if needed)
        """
        # Prepare disk once at the beginning
        self.disk_manager._prepare_disk_for_write(disk_path)

        # Create MBR
        mbr_data = self._create_mbr(layout)

        # Write MBR to sector 0 (skip prepare since we just did it)
        self.disk_manager.write_sectors(disk_path, 0, mbr_data, skip_prepare=True)

        # Create and write GPT if needed
        if layout.has_gpt:
            gpt_data = self._create_gpt(layout)

            # Write main GPT header (sector 1)
            self.disk_manager.write_sectors(disk_path, 1, gpt_data['main_header'], skip_prepare=True)

            # Write GPT entries (sectors 2-33)
            self.disk_manager.write_sectors(disk_path, 2, gpt_data['entries'], skip_prepare=True)

            # Write backup GPT entries
            backup_entries_lba = layout.total_sectors - 33
            self.disk_manager.write_sectors(disk_path, backup_entries_lba, gpt_data['entries'], skip_prepare=True)

            # Write backup GPT header
            backup_header_lba = layout.total_sectors - 1
            self.disk_manager.write_sectors(disk_path, backup_header_lba, gpt_data['backup_header'], skip_prepare=True)

    def _create_mbr(self, layout: DiskLayout) -> bytes:
        """Create MBR (512 bytes)"""
        import logging
        logger = logging.getLogger(__name__)

        mbr = bytearray(512)

        # Random signature
        mbr[0x1B8:0x1BC] = os.urandom(4)

        # Boot signature
        mbr[0x1FE:0x200] = b'\x55\xAA'

        # Add MBR partitions in the correct order:
        # Slot 1: FAT32
        # Slot 2: Linux (if exists)
        # Slot 3: emuMMC (if exists)
        # Slot 4: GPT protective (if GPT exists)

        logger.info("=== Creating MBR partition table ===")

        # Sort MBR partitions by category order
        category_order = {'FAT32': 1, 'Linux': 2, 'emuMMC': 3}
        mbr_partitions = [p for p in layout.partitions if p.in_mbr]
        mbr_partitions.sort(key=lambda p: category_order.get(p.category, 99))

        logger.info(f"Found {len(mbr_partitions)} partitions marked for MBR (in_mbr=True)")
        for p in mbr_partitions:
            logger.info(f"  - {p.name} ({p.category}): type=0x{p.type_id:02X}, sector={p.start_sector}, size={p.size_mb}MB")

        mbr_idx = 0
        for partition in mbr_partitions:
            if mbr_idx >= 4:
                break  # MBR only supports 4 partitions

            offset = 0x1BE + (mbr_idx * 16)

            # Status (not bootable)
            mbr[offset] = 0x00

            # CHS start (0xFFFFFF for LBA)
            mbr[offset + 1:offset + 4] = b'\xFF\xFF\xFF'

            # Type
            mbr[offset + 4] = partition.type_id

            # CHS end (0xFFFFFF for LBA)
            mbr[offset + 5:offset + 8] = b'\xFF\xFF\xFF'

            # LBA start
            mbr[offset + 8:offset + 12] = struct.pack('<I', partition.start_sector)

            # Size in sectors
            mbr[offset + 12:offset + 16] = struct.pack('<I', partition.size_sectors)

            logger.info(f"MBR slot {mbr_idx}: {partition.name} ({partition.category}) - type=0x{partition.type_id:02X}, start={partition.start_sector}, size={partition.size_sectors} sectors")
            mbr_idx += 1

        # Add GPT protective partition if GPT exists
        if layout.has_gpt and mbr_idx < 4:
            offset = 0x1BE + (mbr_idx * 16)
            mbr[offset] = 0x00
            mbr[offset + 1:offset + 4] = b'\xFF\xFF\xFF'
            mbr[offset + 4] = 0xEE  # GPT protective
            mbr[offset + 5:offset + 8] = b'\xFF\xFF\xFF'
            mbr[offset + 8:offset + 12] = struct.pack('<I', 1)
            mbr[offset + 12:offset + 16] = struct.pack('<I', layout.total_sectors - 1)
            logger.info(f"MBR slot {mbr_idx}: GPT Protective - type=0xEE, start=1, size={layout.total_sectors - 1} sectors")

        logger.info(f"MBR creation complete: {mbr_idx + (1 if layout.has_gpt else 0)} total slots used")
        return bytes(mbr)

    def _create_gpt(self, layout: DiskLayout) -> dict:
        """Create GPT structures"""

        gpt_entries = bytearray(128 * 128)  # 128 entries × 128 bytes
        gpt_idx = 0

        # Add all GPT partitions
        for partition in layout.partitions:
            if not partition.in_gpt:
                continue

            if gpt_idx >= 128:
                break

            offset = gpt_idx * 128

            # Type GUID
            if partition.category == 'FAT32':
                type_guid = GUID_FAT32
            elif partition.category == 'Linux' or partition.category == 'Android':
                type_guid = GUID_LINUX
            elif partition.category == 'emuMMC':
                type_guid = GUID_EMUMMC
            else:
                type_guid = b'\x00' * 16

            gpt_entries[offset:offset + 16] = type_guid

            # Partition GUID (random)
            part_guid = bytearray(os.urandom(16))
            part_guid[7] = 0  # Clear Windows attributes
            gpt_entries[offset + 16:offset + 32] = part_guid

            # LBA start and end
            gpt_entries[offset + 32:offset + 40] = struct.pack('<Q', partition.start_sector)
            gpt_entries[offset + 40:offset + 48] = struct.pack('<Q', partition.start_sector + partition.size_sectors - 1)

            # Attributes (0)
            gpt_entries[offset + 48:offset + 56] = struct.pack('<Q', 0)

            # Name (UTF-16LE)
            name_utf16 = partition.name.encode('utf-16le')[:72]
            gpt_entries[offset + 56:offset + 56 + len(name_utf16)] = name_utf16

            gpt_idx += 1

        # Create disk GUID with NYXGPT marker
        disk_guid = os.urandom(10) + b'NYXGPT'

        # Create main GPT header
        main_header = self._create_gpt_header(
            my_lba=1,
            alt_lba=layout.total_sectors - 1,
            part_ent_lba=2,
            disk_guid=disk_guid,
            num_entries=gpt_idx,
            entries_data=bytes(gpt_entries[:gpt_idx * 128]),
            total_sectors=layout.total_sectors
        )

        # Create backup GPT header
        backup_header = self._create_gpt_header(
            my_lba=layout.total_sectors - 1,
            alt_lba=1,
            part_ent_lba=layout.total_sectors - 33,
            disk_guid=disk_guid,
            num_entries=gpt_idx,
            entries_data=bytes(gpt_entries[:gpt_idx * 128]),
            total_sectors=layout.total_sectors
        )

        return {
            'main_header': main_header,
            'entries': bytes(gpt_entries),
            'backup_header': backup_header
        }

    def _create_gpt_header(self, my_lba, alt_lba, part_ent_lba, disk_guid,
                          num_entries, entries_data, total_sectors) -> bytes:
        """Create GPT header (512 bytes)"""

        header = bytearray(512)

        # Signature
        header[0:8] = b'EFI PART'

        # Revision
        header[8:12] = struct.pack('<I', 0x00010000)

        # Header size
        header[12:16] = struct.pack('<I', 92)

        # CRC32 placeholder
        header[16:20] = struct.pack('<I', 0)

        # Reserved
        header[20:24] = struct.pack('<I', 0)

        # My LBA
        header[24:32] = struct.pack('<Q', my_lba)

        # Alternate LBA
        header[32:40] = struct.pack('<Q', alt_lba)

        # First usable LBA
        header[40:48] = struct.pack('<Q', 34)

        # Last usable LBA
        header[48:56] = struct.pack('<Q', total_sectors - 34)

        # Disk GUID
        header[56:72] = disk_guid

        # Partition entry LBA
        header[72:80] = struct.pack('<Q', part_ent_lba)

        # Number of partition entries
        header[80:84] = struct.pack('<I', num_entries)

        # Size of partition entry
        header[84:88] = struct.pack('<I', 128)

        # Partition entries CRC32
        entries_crc = zlib.crc32(entries_data) & 0xFFFFFFFF
        header[88:92] = struct.pack('<I', entries_crc)

        # Calculate header CRC32
        header_crc = zlib.crc32(bytes(header[0:92])) & 0xFFFFFFFF
        header[16:20] = struct.pack('<I', header_crc)

        return bytes(header)
