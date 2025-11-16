"""
Quick diagnostic to check emuMMC structure
"""
import struct

SECTOR_SIZE = 512

def check_emummc(disk_path, partition_start):
    """Check emuMMC structure at given partition"""
    
    print(f"Checking emuMMC at disk {disk_path}, partition start sector {partition_start}")
    print("=" * 70)
    
    with open(disk_path, 'rb') as f:
        # Check BOOT0 at offset 0x8000 (standard)
        boot0_offset = partition_start + 0x8000
        f.seek(boot0_offset * SECTOR_SIZE)
        boot0_start = f.read(16)
        print(f"\nBOOT0 check (sector {boot0_offset}, 0x{boot0_offset:X}):")
        print(f"  First 16 bytes: {boot0_start.hex()}")
        
        # Check for GPT header at physical offset 0x14001
        gpt_phys_offset = partition_start + 0x14001
        f.seek(gpt_phys_offset * SECTOR_SIZE)
        gpt_data = f.read(512)
        print(f"\nGPT Header check (physical sector {gpt_phys_offset}, 0x{gpt_phys_offset:X}):")
        print(f"  Signature: {gpt_data[:8]}")
        if gpt_data[:8] == b'EFI PART':
            print("  ✓ Valid GPT header found!")
            # Parse partition entries count
            num_entries = struct.unpack('<I', gpt_data[80:84])[0]
            print(f"  Number of partition entries: {num_entries}")
            
            # Read first few partition entries
            entries_offset = gpt_phys_offset + 1
            f.seek(entries_offset * SECTOR_SIZE)
            entries_data = f.read(4096)  # Read first 32 entries (128 bytes each)
            
            print(f"\n  Partition entries:")
            for i in range(min(10, num_entries)):
                entry_offset = i * 128
                entry = entries_data[entry_offset:entry_offset + 128]
                type_guid = entry[0:16]
                
                # Check if entry is empty (all zeros)
                if type_guid == b'\x00' * 16:
                    continue
                    
                lba_start = struct.unpack('<Q', entry[32:40])[0]
                lba_end = struct.unpack('<Q', entry[40:48])[0]
                name_bytes = entry[56:128]
                name = name_bytes.decode('utf-16le', errors='ignore').rstrip('\x00')
                
                size_mb = ((lba_end - lba_start + 1) * 512) // (1024 * 1024)
                print(f"    [{i}] {name:20s} LBA {lba_start:10d} - {lba_end:10d} ({size_mb:6d} MB)")
        else:
            print("  ✗ No GPT header (not EFI PART)")
        
        # Check MBR at offset 0xC000 (USER partition MBR)
        mbr_offset = partition_start + 0xC000
        f.seek(mbr_offset * SECTOR_SIZE)
        mbr_data = f.read(512)
        print(f"\nUSER MBR check (sector {mbr_offset}, 0x{mbr_offset:X}):")
        if mbr_data[510:512] == b'\x55\xAA':
            print(f"  ✓ Valid MBR signature found")
        else:
            print(f"  ✗ No MBR signature")

if __name__ == "__main__":
    # Example - adjust these values for your source disk
    SOURCE_DISK = r"\\.\PhysicalDrive3"  # Change to your source disk
    SOURCE_EMUMMC_START = 241532928      # From your log
    
    print("SOURCE emuMMC Check:")
    check_emummc(SOURCE_DISK, SOURCE_EMUMMC_START)
    
    print("\n" + "=" * 70)
    print("\nTARGET emuMMC Check:")
    TARGET_DISK = r"\\.\PhysicalDrive5"  # Change to your target disk
    TARGET_EMUMMC_START = 1991278592     # From your log
    check_emummc(TARGET_DISK, TARGET_EMUMMC_START)
