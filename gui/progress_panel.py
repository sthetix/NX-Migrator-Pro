"""
Progress Panel Widget - Show migration progress
"""

import ttkbootstrap as ttk
from ttkbootstrap.constants import *

class ProgressPanel(ttk.Frame):
    """Widget to display migration progress"""

    def __init__(self, parent):
        super().__init__(parent, bootstyle=DARK)

        self.current_stage = ""
        self.current_percent = 0
        self.mode = "migration"  # Default mode

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        """Create widgets"""

        # Stage label
        self.stage_label = ttk.Label(
            self,
            text="Ready to begin migration",
            font=("Segoe UI", 10, "bold"),
            foreground="white",
            bootstyle="inverse-dark"
        )

        # Progress bar
        self.progressbar = ttk.Progressbar(
            self,
            mode=DETERMINATE,
            bootstyle=SUCCESS,
            length=400
        )

        # Percent label
        self.percent_label = ttk.Label(
            self,
            text="0%",
            font=("Segoe UI", 10),
            foreground="white",
            bootstyle="inverse-dark"
        )

        # Detail message
        self.detail_label = ttk.Label(
            self,
            text="",
            font=("Segoe UI", 9),
            foreground="white",
            bootstyle="inverse-dark"
        )

    def _layout_widgets(self):
        """Layout widgets"""

        # Container for progress bar and percent
        progress_container = ttk.Frame(self, bootstyle=DARK)
        progress_container.pack(fill=X, pady=5)

        self.stage_label.pack(pady=(10, 5))

        self.progressbar.pack(side=LEFT, expand=YES, fill=X, padx=(20, 10))
        self.percent_label.pack(side=LEFT, padx=(0, 20))

        self.detail_label.pack(pady=(5, 10))

    def start(self):
        """Start progress"""
        self.progressbar.config(value=0, bootstyle=INFO)
        if self.mode == "cleanup":
            self.stage_label.config(text="Starting cleanup...")
        else:
            self.stage_label.config(text="Starting migration...")
        self.percent_label.config(text="0%")
        self.detail_label.config(text="")

    def update(self, stage, percent, message):
        """Update progress"""
        self.current_stage = stage
        self.current_percent = percent

        self.stage_label.config(text=stage)
        self.progressbar.config(value=percent, bootstyle=INFO)
        self.percent_label.config(text=f"{percent:.1f}%")
        self.detail_label.config(text=message)

    def complete(self):
        """Mark as complete"""
        self.progressbar.config(value=100, bootstyle=SUCCESS)
        if self.mode == "cleanup":
            self.stage_label.config(text="✓ Cleanup Complete", foreground="green")
            self.detail_label.config(text="SD card cleaned up successfully")
        else:
            self.stage_label.config(text="✓ Migration Complete", foreground="green")
            self.detail_label.config(text="All partitions migrated successfully")
        self.percent_label.config(text="100%")

    def error(self):
        """Mark as error"""
        self.progressbar.config(bootstyle=DANGER)
        if self.mode == "cleanup":
            self.stage_label.config(text="✗ Cleanup Failed", foreground="red")
            self.detail_label.config(text="An error occurred during cleanup")
        else:
            self.stage_label.config(text="✗ Migration Failed", foreground="red")
            self.detail_label.config(text="An error occurred during migration")

    def reset(self, mode="migration"):
        """Reset progress panel to initial state with specified mode"""
        self.mode = mode
        self.current_stage = ""
        self.current_percent = 0
        self.progressbar.config(value=0, bootstyle=SUCCESS)
        self.percent_label.config(text="0%")
        self.detail_label.config(text="")

        if mode == "cleanup":
            self.stage_label.config(text="Ready to begin cleanup", foreground="white")
        else:
            self.stage_label.config(text="Ready to begin migration", foreground="white")
