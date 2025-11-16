#!/usr/bin/env python3
"""Generate correct 16-byte GUIDs for Switch partitions"""

# Partition names and their ASCII representation in the GUID
# GUID is 16 bytes total. Last 6 bytes contain the partition identifier.
partitions = [
    ("PRODINFO", "0050524f4449"),      # 00 + PRODI (5 bytes ASCII) = 6 bytes
    ("PRODINFOF", "0050524f4446"),     # 00 + PRODF (5 bytes ASCII) = 6 bytes  
    ("BCPKG2-1", "004243504b31"),      # 00 + BCPK1 (5 bytes ASCII) = 6 bytes
    ("BCPKG2-2", "004243504b32"),      # 00 + BCPK2 (5 bytes ASCII) = 6 bytes
    ("BCPKG2-3", "004243504b33"),      # 00 + BCPK3 (5 bytes ASCII) = 6 bytes
    ("BCPKG2-4", "004243504b34"),      # 00 + BCPK4 (5 bytes ASCII) = 6 bytes
    ("BCPKG2-5", "004243504b35"),      # 00 + BCPK5 (5 bytes ASCII) = 6 bytes
    ("BCPKG2-6", "004243504b36"),      # 00 + BCPK6 (5 bytes ASCII) = 6 bytes
    ("SAFE", "005341464500"),          # 00 + SAFE (4 bytes ASCII) + 00 = 6 bytes
    ("SYSTEM", "005359535445"),        # 00 + SYSTE (5 bytes ASCII) = 6 bytes
    ("USER", "005553455200"),          # 00 + USER (4 bytes ASCII) + 00 = 6 bytes
]

print("Correct 16-byte GUIDs for Switch partitions:\n")

for name, ascii_hex in partitions:
    # GUID format: 00007eca-1100-0000-0000-00XXXXXXXXXX
    # where last 12 hex chars (6 bytes) contain partition identifier
    guid_hex = f"00007eca110000000000{ascii_hex.upper()}"
    guid_bytes = bytes.fromhex(guid_hex)
    
    print(f"{name:30s}: bytes.fromhex(\"{guid_hex}\")")
    print(f"{'':30s}  Length: {len(guid_bytes)} bytes")
    print()
