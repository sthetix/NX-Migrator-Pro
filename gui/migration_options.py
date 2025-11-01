"""
Migration Options Widget - Select what to migrate
"""

import ttkbootstrap as ttk
from ttkbootstrap.constants import *

class MigrationOptionsFrame(ttk.Frame):
    """Widget for selecting migration options"""

    def __init__(self, parent, on_options_changed=None):
        super().__init__(parent)

        self.on_options_changed = on_options_changed
        self.current_mode = "migration"  # "migration" or "cleanup"

        # Options state
        self.options = {
            'migrate_fat32': True,
            'migrate_linux': True,
            'migrate_android': True,
            'migrate_emummc': True,
            'expand_fat32': True
        }

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        """Create widgets"""

        # Title
        self.title_label = ttk.Label(
            self,
            text="Select What to Migrate",
            font=("Segoe UI", 11, "bold")
        )

        # Checkbuttons
        self.fat32_var = ttk.BooleanVar(value=True)
        self.fat32_check = ttk.Checkbutton(
            self,
            text="✓ FAT32 Partition (hos_data)",
            variable=self.fat32_var,
            command=self._on_option_changed,
            bootstyle="success-round-toggle"
        )
        self.fat32_check.config(state=DISABLED)  # FAT32 is always migrated

        self.expand_var = ttk.BooleanVar(value=True)
        self.expand_check = ttk.Checkbutton(
            self,
            text="  └─ Expand to fill free space",
            variable=self.expand_var,
            command=self._on_option_changed,
            bootstyle="info-round-toggle"
        )

        self.linux_var = ttk.BooleanVar(value=True)
        self.linux_check = ttk.Checkbutton(
            self,
            text="✓ Linux Partition (L4T)",
            variable=self.linux_var,
            command=self._on_option_changed,
            bootstyle="info-round-toggle"  # Blue to match visual bar
        )

        self.android_var = ttk.BooleanVar(value=True)
        self.android_check = ttk.Checkbutton(
            self,
            text="✓ Android Partitions",
            variable=self.android_var,
            command=self._on_option_changed,
            bootstyle="info-round-toggle"  # Blue to match Linux toggle
        )

        self.emummc_var = ttk.BooleanVar(value=True)
        self.emummc_check = ttk.Checkbutton(
            self,
            text="✓ emuMMC Partitions",
            variable=self.emummc_var,
            command=self._on_option_changed,
            bootstyle="info-round-toggle"  # Blue to match Linux toggle
        )

        # Separator
        self.separator = ttk.Separator(self, orient=HORIZONTAL)

        # Quick select buttons
        self.quick_label = ttk.Label(
            self,
            text="Quick Select:",
            font=("Segoe UI", 9, "bold")
        )

        self.all_button = ttk.Button(
            self,
            text="All",
            command=self._select_all,
            bootstyle=SUCCESS,
            width=12
        )

        self.none_button = ttk.Button(
            self,
            text="None",
            command=self._select_none,
            bootstyle=SECONDARY,
            width=12
        )

        # Info label
        self.info_label = ttk.Label(
            self,
            text="💡 Unchecked partitions will be skipped.\nFAT32 will expand to use freed space.",
            font=("Segoe UI", 8),
            bootstyle=INFO,
            wraplength=200,
            justify=LEFT
        )

    def _layout_widgets(self):
        """Layout widgets"""

        self.title_label.pack(pady=(5, 15))

        self.fat32_check.pack(anchor=W, pady=5)
        self.expand_check.pack(anchor=W, pady=(0, 5), padx=(20, 0))

        self.linux_check.pack(anchor=W, pady=5)
        self.android_check.pack(anchor=W, pady=5)
        self.emummc_check.pack(anchor=W, pady=5)

        self.separator.pack(fill=X, pady=15)

        self.quick_label.pack(pady=(0, 5))

        self.all_button.pack(pady=3)
        self.none_button.pack(pady=3)

        self.separator2 = ttk.Separator(self, orient=HORIZONTAL)
        self.separator2.pack(fill=X, pady=15)

        self.info_label.pack(pady=5)

    def _on_option_changed(self):
        """Called when any option changes"""
        self.options = {
            'migrate_fat32': self.fat32_var.get(),
            'migrate_linux': self.linux_var.get(),
            'migrate_android': self.android_var.get(),
            'migrate_emummc': self.emummc_var.get(),
            'expand_fat32': self.expand_var.get()
        }

        if self.on_options_changed:
            self.on_options_changed(self.options)

    def _select_all(self):
        """Select all options"""
        self.linux_var.set(True)
        self.android_var.set(True)
        self.emummc_var.set(True)
        self.expand_var.set(True)
        self._on_option_changed()

    def _select_none(self):
        """Deselect optional partitions"""
        self.linux_var.set(False)
        self.android_var.set(False)
        self.emummc_var.set(False)
        self._on_option_changed()

    def set_mode(self, mode):
        """Switch between migration and cleanup modes"""
        self.current_mode = mode

        if mode == "migration":
            # Migration mode - checkboxes mean "migrate this"
            self.title_label.config(text="Select What to Migrate")
            self.linux_check.config(text="✓ Linux Partition (L4T)")
            self.android_check.config(text="✓ Android Partitions")
            self.emummc_check.config(text="✓ emuMMC Partitions")
            self.info_label.config(
                text="💡 Unchecked partitions will be skipped.\nFAT32 will expand to use freed space."
            )
            # Set all to checked by default in migration mode
            self.linux_var.set(True)
            self.android_var.set(True)
            self.emummc_var.set(True)
        else:  # cleanup mode
            # Cleanup mode - checkboxes mean "remove this"
            self.title_label.config(text="Select What to Remove")
            self.linux_check.config(text="❌ Remove Linux Partition")
            self.android_check.config(text="❌ Remove Android Partitions")
            self.emummc_check.config(text="❌ Remove emuMMC Partitions")
            self.info_label.config(
                text="⚠️ Checked partitions will be DELETED!\nFAT32 will expand to use freed space."
            )
            # Set all to unchecked by default in cleanup mode (safer)
            self.linux_var.set(False)
            self.android_var.set(False)
            self.emummc_var.set(False)

        self._on_option_changed()

    def set_enabled(self, enabled):
        """Enable/disable options"""
        state = NORMAL if enabled else DISABLED

        self.linux_check.config(state=state)
        self.android_check.config(state=state)
        self.emummc_check.config(state=state)
        self.expand_check.config(state=state)
        self.all_button.config(state=state)
        self.none_button.config(state=state)

    def update_available_partitions(self, has_linux, has_android, has_emummc):
        """Update which toggles are enabled based on what partitions exist on the SD card"""
        # Enable/disable Linux toggle based on whether Linux partition exists
        if has_linux:
            self.linux_check.config(state=NORMAL)
        else:
            self.linux_check.config(state=DISABLED)
            self.linux_var.set(False)  # Uncheck if disabled

        # Enable/disable Android toggle based on whether Android partitions exist
        if has_android:
            self.android_check.config(state=NORMAL)
        else:
            self.android_check.config(state=DISABLED)
            self.android_var.set(False)  # Uncheck if disabled

        # Enable/disable emuMMC toggle based on whether emuMMC partition exists
        if has_emummc:
            self.emummc_check.config(state=NORMAL)
        else:
            self.emummc_check.config(state=DISABLED)
            self.emummc_var.set(False)  # Uncheck if disabled

        # Update internal options state but DON'T trigger callback
        # This prevents double calculation when scanning
        self.options = {
            'migrate_fat32': self.fat32_var.get(),
            'migrate_linux': self.linux_var.get(),
            'migrate_android': self.android_var.get(),
            'migrate_emummc': self.emummc_var.get(),
            'expand_fat32': self.expand_var.get()
        }
