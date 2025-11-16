"""
Partition Scanner - Scan and analyze disk partitions
Detects hekate partition layouts (FAT32, Linux, Android, emuMMC)
"""

import struct
from typing import Dict, List
from core.disk_manager import DiskManager
from core.partition_models import DiskLayout, Partition, PartitionType

SECTOR_SIZE = 512
ALIGN_SECTORS = 0x8000  # 16MB alignment

class PartitionScanner:
    """Scans disks and detects hekate partition layouts"""

    def __init__(self):
        self.disk_manager = DiskManager()

    def scan_disk(self, disk_path: str) -> DiskLayout:
        """
        Scan disk and detect partition layout
        Returns DiskLayout object
        """
        import logging
        logger = logging.getLogger(__name__)

        layout = DiskLayout()

        # Get disk size
        disk_size = self.disk_manager.get_disk_size(disk_path)
        layout.total_sectors = disk_size // SECTOR_SIZE

        # Read MBR (sector 0)
        mbr_data = self.disk_manager.read_sectors(disk_path, 0, 1)
        self._parse_mbr(mbr_data, layout)
        logger.info(f"After MBR parse: {len(layout.partitions)} partitions")
        for p in layout.partitions:
            logger.info(f"  MBR: {p.name} ({p.category}) at sector {p.start_sector}, size {p.size_mb}MB")

        # Check for GPT (sector 1)
        gpt_header_data = self.disk_manager.read_sectors(disk_path, 1, 1)
        if gpt_header_data[0:8] == b'EFI PART':
            # Read GPT entries (sectors 2-33)
            gpt_entries_data = self.disk_manager.read_sectors(disk_path, 2, 32)
            mbr_count = len(layout.partitions)
            self._parse_gpt(gpt_entries_data, layout)
            logger.info(f"After GPT parse: {len(layout.partitions)} partitions (added {len(layout.partitions) - mbr_count} from GPT)")
            for i, p in enumerate(layout.partitions[mbr_count:], start=mbr_count):
                logger.info(f"  GPT: {p.name} ({p.category}) at sector {p.start_sector}, size {p.size_mb}MB")

        # Remove duplicate partitions (same start sector and size in both MBR and GPT)
        before_dedup = len(layout.partitions)
        self._deduplicate_partitions(layout)
        logger.info(f"After deduplication: {len(layout.partitions)} partitions (removed {before_dedup - len(layout.partitions)} duplicates)")

        # Set has_gpt based on Android presence (GPT is only needed for Android)
        # If Android exists, use hybrid MBR+GPT; otherwise use pure MBR
        layout.has_gpt = layout.has_android
        if layout.has_gpt:
            logger.info("Android detected - using hybrid MBR+GPT partition table")
        else:
            logger.info("No Android - using pure MBR partition table")

        # Detect Android type if present
        if layout.has_android:
            self._detect_android_type(layout)

        # Detect emuMMC type if present
        if layout.has_emummc:
            self._detect_emummc_type(layout)

        return layout

    def _parse_mbr(self, mbr_data: bytes, layout: DiskLayout):
        """Parse MBR partition table"""

        # Check boot signature
        if mbr_data[0x1FE:0x200] != b'\x55\xAA':
            raise ValueError("Invalid MBR: Missing boot signature")

        # Parse 4 partition entries
        for i in range(4):
            offset = 0x1BE + (i * 16)
            entry = mbr_data[offset:offset + 16]

            # Extract partition info
            status = entry[0]
            part_type = entry[4]
            start_sector = struct.unpack('<I', entry[8:12])[0]
            size_sectors = struct.unpack('<I', entry[12:16])[0]

            # Skip empty partitions
            if part_type == 0 or size_sectors == 0:
                continue

            # Skip GPT protective partition (will be handled by GPT parser)
            if part_type == 0xEE:
                continue

            # Determine partition category and name
            category, name = self._categorize_partition(part_type, f"MBR{i}")

            size_mb = (size_sectors * SECTOR_SIZE) // (1024 * 1024)

            partition = Partition(
                name=name,
                type_id=part_type,
                type_name=self._get_type_name(part_type),
                start_sector=start_sector,
                size_sectors=size_sectors,
                size_mb=size_mb,
                category=category,
                in_mbr=True,
                in_gpt=False
            )

            layout.add_partition(partition)

    def _parse_gpt(self, gpt_data: bytes, layout: DiskLayout):
        """Parse GPT partition entries"""

        # GPT entry size is 128 bytes
        # We have 32 sectors * 512 bytes = 16384 bytes
        # Maximum 128 entries

        for i in range(128):
            offset = i * 128
            entry = gpt_data[offset:offset + 128]

            # Type GUID
            type_guid = entry[0:16]

            # Check if entry is empty (all zeros)
            if type_guid == b'\x00' * 16:
                continue

            # LBA start and end
            lba_start = struct.unpack('<Q', entry[32:40])[0]
            lba_end = struct.unpack('<Q', entry[40:48])[0]

            size_sectors = lba_end - lba_start + 1
            size_mb = (size_sectors * SECTOR_SIZE) // (1024 * 1024)

            # Name (UTF-16LE, max 72 bytes = 36 characters)
            name_bytes = entry[56:56 + 72]
            name = name_bytes.decode('utf-16le', errors='ignore').rstrip('\x00')

            if not name:
                name = f"GPT{i}"

            # Categorize by GUID or name
            category = self._categorize_gpt_partition(type_guid, name)

            partition = Partition(
                name=name,
                type_id=0,  # GPT uses GUIDs, not type IDs
                type_name=category,
                start_sector=lba_start,
                size_sectors=size_sectors,
                size_mb=size_mb,
                category=category,
                in_mbr=False,
                in_gpt=True
            )

            layout.add_partition(partition)

    def _categorize_partition(self, type_id: int, default_name: str) -> tuple:
        """
        Categorize partition by MBR type ID
        Returns (category, name)
        """
        if type_id in [0x0C, 0x0B]:  # FAT32
            return ('FAT32', 'hos_data')
        elif type_id == 0x83:  # Linux
            return ('Linux', 'l4t')
        elif type_id == 0xE0:  # emuMMC
            return ('emuMMC', default_name.replace('MBR', 'emummc'))
        else:
            return ('Unknown', default_name)

    def _categorize_gpt_partition(self, type_guid: bytes, name: str) -> str:
        """
        Categorize GPT partition by GUID or name
        Returns category string
        """
        # Known GUIDs
        GUID_FAT32 = bytes([0xA2, 0xA0, 0xD0, 0xEB, 0xE5, 0xB9, 0x33, 0x44,
                             0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7])

        GUID_LINUX = bytes([0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47,
                             0x8E, 0x79, 0x3D, 0x69, 0xD8, 0x47, 0x7D, 0xE4])

        GUID_EMUMMC = bytes([0x00, 0x7E, 0xCA, 0x11, 0x00, 0x00, 0x00, 0x00,
                              0x00, 0x00, ord('e'), ord('m'), ord('u'), ord('M'),
                              ord('M'), ord('C')])

        if type_guid == GUID_FAT32:
            return 'FAT32'
        elif type_guid == GUID_LINUX:
            # Distinguish between Linux and Android by name
            name_lower = name.lower()
            if name_lower == 'l4t':
                return 'Linux'
            else:
                # Android partitions have specific names
                return 'Android'
        elif type_guid == GUID_EMUMMC:
            return 'emuMMC'
        else:
            return 'Unknown'

    def _get_type_name(self, type_id: int) -> str:
        """Get human-readable type name"""
        names = {
            0x0C: 'FAT32 (LBA)',
            0x0B: 'FAT32',
            0x83: 'Linux',
            0xE0: 'emuMMC',
            0xEE: 'GPT Protective'
        }
        return names.get(type_id, f'Unknown (0x{type_id:02X})')

    def _detect_android_type(self, layout: DiskLayout):
        """Detect if Android is Dynamic (10+) or Legacy (7-9)"""
        android_parts = layout.get_android_partitions()

        # Check for 'super' partition (Android 10+ dynamic)
        has_super = any(p.name.lower() == 'super' for p in android_parts)

        if has_super:
            layout.android_dynamic = True
        else:
            layout.android_dynamic = False

    def _detect_emummc_type(self, layout: DiskLayout):
        """Detect if emuMMC is single or dual"""
        emummc_parts = layout.get_emummc_partitions()

        # Check for two emuMMC partitions
        if len(emummc_parts) >= 2:
            layout.emummc_double = True
        else:
            layout.emummc_double = False

    def _deduplicate_partitions(self, layout: DiskLayout):
        """
        Remove duplicate partitions that exist in both MBR and GPT
        Keep GPT version if duplicate found (more detailed info)

        Handles both exact matches and near-matches (for misaligned MBR entries)
        """
        import logging
        logger = logging.getLogger(__name__)

        # Build a map of (start_sector, size_sectors) -> [partitions]
        partition_map = {}
        for p in layout.partitions:
            key = (p.start_sector, p.size_sectors)
            if key not in partition_map:
                partition_map[key] = []
            partition_map[key].append(p)

        # Find duplicates and keep only one (prefer GPT)
        unique_partitions = []
        used_partitions = set()  # Track which partitions we've already processed

        for i, p in enumerate(layout.partitions):
            if i in used_partitions:
                continue

            # Look for duplicates - exact match or near match
            duplicates = [p]
            for j, other in enumerate(layout.partitions[i+1:], start=i+1):
                if j in used_partitions:
                    continue

                # Check if this is a duplicate:
                # 1. Same size
                # 2. Same category (FAT32, emuMMC, etc.)
                # 3. Start sectors are either identical or very close (within 1% of size)
                if (other.size_sectors == p.size_sectors and
                    other.category == p.category):

                    sector_diff = abs(other.start_sector - p.start_sector)
                    tolerance = p.size_sectors // 100  # 1% tolerance

                    if sector_diff == 0 or sector_diff < tolerance:
                        duplicates.append(other)
                        used_partitions.add(j)
                        logger.info(f"Found near-duplicate: {other.name} at sector {other.start_sector} (diff: {sector_diff} sectors from {p.name})")

            # Keep the best version and merge MBR/GPT flags
            # Prefer GPT partition for metadata (name, etc.), but preserve both flags
            gpt_parts = [d for d in duplicates if d.in_gpt]
            if gpt_parts:
                chosen = gpt_parts[0]
            else:
                chosen = duplicates[0]

            # Merge flags: if partition exists in both MBR and GPT, mark both as True
            chosen.in_mbr = any(d.in_mbr for d in duplicates)
            chosen.in_gpt = any(d.in_gpt for d in duplicates)

            if len(duplicates) > 1:
                mbr_gpt_str = []
                if chosen.in_mbr:
                    mbr_gpt_str.append("MBR")
                if chosen.in_gpt:
                    mbr_gpt_str.append("GPT")
                logger.info(f"Duplicate found: keeping '{chosen.name}' at sector {chosen.start_sector} (in {'+'.join(mbr_gpt_str)}), dropping {len(duplicates)-1} duplicate(s)")

            unique_partitions.append(chosen)
            used_partitions.add(i)

            if len(duplicates) == 1:
                logger.info(f"Keeping unique partition: {chosen.name} at sector {chosen.start_sector}")

        # Replace partitions list and recalculate sizes
        # Sort by start_sector to ensure physical disk order (left to right)
        layout.partitions = sorted(unique_partitions, key=lambda p: p.start_sector)
        layout.has_linux = False
        layout.has_android = False
        layout.has_emummc = False
        layout.fat32_size_mb = 0
        layout.linux_size_mb = 0
        layout.android_size_mb = 0
        layout.emummc_size_mb = 0

        # Recalculate flags and sizes
        for p in layout.partitions:
            if p.category == 'Linux':
                layout.has_linux = True
                layout.linux_size_mb += p.size_mb
            elif p.category == 'Android':
                layout.has_android = True
                layout.android_size_mb += p.size_mb
            elif p.category == 'emuMMC':
                layout.has_emummc = True
                layout.emummc_size_mb += p.size_mb
            elif p.category == 'FAT32':
                layout.fat32_size_mb += p.size_mb

    def calculate_target_layout(self, source_layout: DiskLayout, target_size_bytes: int,
                                options: Dict) -> DiskLayout:
        """
        Calculate new partition layout for target disk
        Based on migration options
        """
        import logging
        logger = logging.getLogger(__name__)

        logger.info(f"=== calculate_target_layout called ===")
        logger.info(f"Source has_emummc: {source_layout.has_emummc}")
        logger.info(f"Source emummc_size_mb: {source_layout.emummc_size_mb}")
        logger.info(f"Migration options: {options}")

        target_layout = DiskLayout()
        target_layout.total_sectors = target_size_bytes // SECTOR_SIZE

        current_lba = ALIGN_SECTORS  # Start at 16MB

        # Calculate sizes based on what we're migrating
        total_reserved_mb = 0

        # Linux (fixed size if migrating)
        if source_layout.has_linux and options['migrate_linux']:
            total_reserved_mb += source_layout.linux_size_mb
            logger.info(f"Will migrate Linux: {source_layout.linux_size_mb} MB")

        # Android (fixed size if migrating)
        if source_layout.has_android and options['migrate_android']:
            total_reserved_mb += source_layout.android_size_mb
            target_layout.android_dynamic = source_layout.android_dynamic
            logger.info(f"Will migrate Android: {source_layout.android_size_mb} MB")

        # Set has_gpt based on whether we're migrating Android
        # GPT is only needed when Android partitions exist (too many to fit in MBR)
        target_layout.has_gpt = (source_layout.has_android and options['migrate_android'])
        logger.info(f"Target will use {'hybrid MBR+GPT' if target_layout.has_gpt else 'pure MBR'} partition table")

        # emuMMC (fixed size if migrating)
        if source_layout.has_emummc and options['migrate_emummc']:
            total_reserved_mb += source_layout.emummc_size_mb
            target_layout.emummc_double = source_layout.emummc_double
            logger.info(f"Will migrate emuMMC: {source_layout.emummc_size_mb} MB, double={source_layout.emummc_double}")
        else:
            logger.warning(f"NOT migrating emuMMC - has_emummc={source_layout.has_emummc}, migrate_emummc={options.get('migrate_emummc', 'NOT SET')}")

        # FAT32 - expand if requested
        if options['expand_fat32']:
            # Calculate available space
            total_disk_mb = (target_layout.total_sectors * SECTOR_SIZE) // (1024 * 1024)
            start_alignment_mb = ALIGN_SECTORS // 2048  # 16MB starting alignment
            end_reserve_mb = 9  # Reserve ~9MB at end (matching Hekate's layout)
            reserved_mb = total_reserved_mb + start_alignment_mb + end_reserve_mb
            fat32_size_mb = total_disk_mb - reserved_mb
        else:
            fat32_size_mb = source_layout.fat32_size_mb

        # Create partition objects (in order)
        # 1. FAT32
        fat32_sectors = (fat32_size_mb * 1024 * 1024) // SECTOR_SIZE
        fat32_part = Partition(
            name='hos_data',
            type_id=0x0C,
            type_name='FAT32 (LBA)',
            start_sector=current_lba,
            size_sectors=fat32_sectors,
            size_mb=fat32_size_mb,
            category='FAT32',
            in_mbr=True,
            in_gpt=target_layout.has_gpt
        )
        target_layout.add_partition(fat32_part)
        current_lba += fat32_sectors
        
        # Align to 32768 sectors (16MB) after FAT32
        if current_lba % ALIGN_SECTORS != 0:
            current_lba = ((current_lba + ALIGN_SECTORS - 1) // ALIGN_SECTORS) * ALIGN_SECTORS

        # 2. Linux (if migrating)
        if source_layout.has_linux and options['migrate_linux']:
            linux_sectors = (source_layout.linux_size_mb * 1024 * 1024) // SECTOR_SIZE
            linux_part = Partition(
                name='l4t',
                type_id=0x83,
                type_name='Linux',
                start_sector=current_lba,
                size_sectors=linux_sectors,
                size_mb=source_layout.linux_size_mb,
                category='Linux',
                # Only include in MBR if no GPT (i.e., no Android). When Android is present,
                # Linux should only be in GPT to match Hekate's hybrid MBR+GPT implementation.
                in_mbr=not target_layout.has_gpt,
                in_gpt=target_layout.has_gpt
            )
            target_layout.add_partition(linux_part)
            current_lba += linux_sectors
            
            # Align to 32768 sectors (16MB) after Linux
            if current_lba % ALIGN_SECTORS != 0:
                current_lba = ((current_lba + ALIGN_SECTORS - 1) // ALIGN_SECTORS) * ALIGN_SECTORS

        # 3. Android (if migrating) - recreate all Android partitions
        if source_layout.has_android and options['migrate_android']:
            android_parts = source_layout.get_android_partitions()
            for apart in android_parts:
                new_apart = Partition(
                    name=apart.name,
                    type_id=0,
                    type_name=apart.type_name,
                    start_sector=current_lba,
                    size_sectors=apart.size_sectors,
                    size_mb=apart.size_mb,
                    category='Android',
                    in_mbr=False,
                    in_gpt=True
                )
                target_layout.add_partition(new_apart)
                current_lba += apart.size_sectors
            
            # Align to 32768 sectors (16MB) after Android partitions
            if current_lba % ALIGN_SECTORS != 0:
                current_lba = ((current_lba + ALIGN_SECTORS - 1) // ALIGN_SECTORS) * ALIGN_SECTORS

        # 4. emuMMC (if migrating)
        if source_layout.has_emummc and options['migrate_emummc']:
            emummc_parts = source_layout.get_emummc_partitions()
            logger.info(f"Adding {len(emummc_parts)} emuMMC partitions to target layout")
            for epart in emummc_parts:
                new_epart = Partition(
                    name=epart.name,
                    type_id=0xE0,
                    type_name='emuMMC',
                    start_sector=current_lba,
                    size_sectors=epart.size_sectors,
                    size_mb=epart.size_mb,
                    category='emuMMC',
                    in_mbr=True,
                    in_gpt=target_layout.has_gpt
                )
                target_layout.add_partition(new_epart)
                logger.info(f"  Added emuMMC partition: {epart.name}, {epart.size_mb} MB")
                current_lba += epart.size_sectors
        else:
            logger.info(f"NOT adding emuMMC partitions - has_emummc={source_layout.has_emummc}, migrate_emummc={options.get('migrate_emummc', False)}")

        logger.info(f"Target layout final: {len(target_layout.partitions)} partitions")
        for p in target_layout.partitions:
            logger.info(f"  - {p.name} ({p.category}): {p.size_mb} MB")

        return target_layout
