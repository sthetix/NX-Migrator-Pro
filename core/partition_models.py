"""
Partition Data Models
"""

from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

class PartitionType(Enum):
    """Partition types"""
    FAT32 = 0x0C
    LINUX = 0x83
    EMUMMC = 0xE0
    GPT_PROTECTIVE = 0xEE
    ANDROID_DYNAMIC = 0xFF  # Virtual type for Android in GPT
    ANDROID_LEGACY = 0xFE   # Virtual type for Android in GPT

@dataclass
class Partition:
    """Represents a single partition"""
    name: str
    type_id: int
    type_name: str
    start_sector: int
    size_sectors: int
    size_mb: int
    category: str  # 'FAT32', 'Linux', 'Android', 'emuMMC', 'Free'
    in_mbr: bool = True
    in_gpt: bool = False

class DiskLayout:
    """Represents complete disk partition layout"""

    def __init__(self):
        self.partitions: List[Partition] = []
        self.total_sectors = 0
        self.has_gpt = False
        self.has_linux = False
        self.has_android = False
        self.has_emummc = False

        # Android details
        self.android_dynamic = False  # True = Android 10+, False = Android 7-9

        # emuMMC details
        self.emummc_double = False  # True = dual emuMMC

        # Sizes in MB
        self.fat32_size_mb = 0
        self.linux_size_mb = 0
        self.android_size_mb = 0
        self.emummc_size_mb = 0

    def add_partition(self, partition: Partition):
        """Add partition to layout"""
        self.partitions.append(partition)

        # Update flags
        if partition.category == 'Linux':
            self.has_linux = True
            self.linux_size_mb += partition.size_mb
        elif partition.category == 'Android':
            self.has_android = True
            self.android_size_mb += partition.size_mb
        elif partition.category == 'emuMMC':
            self.has_emummc = True
            self.emummc_size_mb += partition.size_mb
        elif partition.category == 'FAT32':
            self.fat32_size_mb += partition.size_mb

    def get_fat32_partition(self) -> Optional[Partition]:
        """Get FAT32 partition"""
        for p in self.partitions:
            if p.category == 'FAT32':
                return p
        return None

    def get_linux_partition(self) -> Optional[Partition]:
        """Get Linux partition"""
        for p in self.partitions:
            if p.category == 'Linux':
                return p
        return None

    def get_linux_partitions(self) -> List[Partition]:
        """Get all Linux partitions"""
        return [p for p in self.partitions if p.category == 'Linux']

    def get_emummc_partitions(self) -> List[Partition]:
        """Get all emuMMC partitions"""
        return [p for p in self.partitions if p.category == 'emuMMC']

    def get_android_partitions(self) -> List[Partition]:
        """Get all Android partitions"""
        return [p for p in self.partitions if p.category == 'Android']

    def get_fat32_size_mb(self) -> int:
        """Get FAT32 partition size in MB"""
        return self.fat32_size_mb

    def get_linux_size_mb(self) -> int:
        """Get Linux partition size in MB"""
        return self.linux_size_mb

    def get_android_size_mb(self) -> int:
        """Get total Android size in MB"""
        return self.android_size_mb

    def get_emummc_size_mb(self) -> int:
        """Get total emuMMC size in MB"""
        return self.emummc_size_mb

    def get_free_space_mb(self) -> int:
        """Get free/unallocated space in MB"""
        used_sectors = sum(p.size_sectors for p in self.partitions)
        free_sectors = self.total_sectors - used_sectors
        return (free_sectors * 512) // (1024 * 1024)

    def get_summary(self) -> str:
        """Get human-readable summary"""
        parts = []

        if self.fat32_size_mb > 0:
            parts.append(f"FAT32: {self.fat32_size_mb} MB")

        if self.has_linux:
            parts.append(f"Linux: {self.linux_size_mb} MB")

        if self.has_android:
            android_type = "Dynamic" if self.android_dynamic else "Legacy"
            parts.append(f"Android ({android_type}): {self.android_size_mb} MB")

        if self.has_emummc:
            emummc_type = "Dual" if self.emummc_double else "Single"
            parts.append(f"emuMMC ({emummc_type}): {self.emummc_size_mb} MB")

        free_mb = self.get_free_space_mb()
        if free_mb > 0:
            parts.append(f"Free: {free_mb} MB")

        return " | ".join(parts) if parts else "No partitions"
