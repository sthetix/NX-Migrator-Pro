"""
Log Panel Widget - Toggleable log display with controls
"""

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox
import logging
from datetime import datetime
import threading


class LogPanel(ttk.Frame):
    """Toggleable log panel widget that displays application logs"""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)

        self.visible = False
        self.auto_scroll = True
        self.log_entries = []
        self.max_entries = 10000  # Limit to prevent memory issues

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        """Create log panel widgets"""

        # Control bar at the top
        self.control_frame = ttk.Frame(self, bootstyle=SECONDARY)

        # Title label
        self.title_label = ttk.Label(
            self.control_frame,
            text="📋 Application Log",
            font=("Segoe UI", 10, "bold"),
            bootstyle="inverse-secondary"
        )

        # Control buttons
        self.clear_btn = ttk.Button(
            self.control_frame,
            text="Clear",
            command=self._clear_log,
            bootstyle="secondary-outline",
            width=10
        )

        self.save_btn = ttk.Button(
            self.control_frame,
            text="Save Log",
            command=self._save_log,
            bootstyle="info-outline",
            width=10
        )

        # Auto-scroll checkbutton
        self.auto_scroll_var = ttk.BooleanVar(value=True)
        self.auto_scroll_check = ttk.Checkbutton(
            self.control_frame,
            text="Auto-scroll",
            variable=self.auto_scroll_var,
            command=self._toggle_auto_scroll,
            bootstyle="toolbutton"  # Toolbutton style works better on colored backgrounds
        )

        # Entry count label
        self.count_label = ttk.Label(
            self.control_frame,
            text="0 entries",
            font=("Segoe UI", 9),
            bootstyle="inverse-secondary"
        )

        # Log display area
        self.log_frame = ttk.Frame(self)

        # Scrollbar
        self.scrollbar = ttk.Scrollbar(self.log_frame)

        # Text widget for log display
        # Note: Text is not a ttk widget, so we configure colors manually
        self.log_text = ttk.Text(
            self.log_frame,
            wrap='word',
            yscrollcommand=self.scrollbar.set,
            font=("Consolas", 9),
            height=10,  # Default height
            state='disabled',
            background='#222222',  # Match the dark theme
            foreground='#FFFFFF',
            insertbackground='#FFFFFF',  # Cursor color
            relief='flat',
            borderwidth=0
        )

        self.scrollbar.config(command=self.log_text.yview)

        # Configure text tags for different log levels
        self.log_text.tag_config('DEBUG', foreground='#888888')
        self.log_text.tag_config('INFO', foreground='#FFFFFF')
        self.log_text.tag_config('WARNING', foreground='#FFA500')
        self.log_text.tag_config('ERROR', foreground='#FF4444')
        self.log_text.tag_config('CRITICAL', foreground='#FF0000', font=("Consolas", 9, "bold"))

    def _layout_widgets(self):
        """Layout widgets"""
        # Initially hidden
        pass

    def _layout_visible(self):
        """Layout widgets when visible"""
        # Control bar
        self.control_frame.pack(fill=X, pady=(5, 0))
        self.title_label.pack(side=LEFT, padx=10, pady=5)
        self.count_label.pack(side=LEFT, padx=10)
        self.auto_scroll_check.pack(side=RIGHT, padx=10, pady=5)
        self.save_btn.pack(side=RIGHT, padx=5, pady=5)
        self.clear_btn.pack(side=RIGHT, padx=5, pady=5)

        # Log display
        self.log_frame.pack(fill=BOTH, expand=True, pady=(0, 5))
        self.scrollbar.pack(side=RIGHT, fill=Y)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)

    def show(self):
        """Show the log panel"""
        if not self.visible:
            self.visible = True
            self._layout_visible()
            self.pack(fill=BOTH, expand=False, padx=8, pady=(5, 0))

    def hide(self):
        """Hide the log panel"""
        if self.visible:
            self.visible = False
            self.pack_forget()

    def toggle(self):
        """Toggle log panel visibility"""
        if self.visible:
            self.hide()
        else:
            self.show()

    def is_visible(self):
        """Check if log panel is visible"""
        return self.visible

    def append_log(self, level, message):
        """
        Append a log message to the display

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Log message text
        """
        timestamp = datetime.now().strftime('%H:%M:%S')

        # Store entry
        entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message
        }
        self.log_entries.append(entry)

        # Limit entries
        if len(self.log_entries) > self.max_entries:
            self.log_entries.pop(0)

        # Update display
        self.log_text.config(state='normal')

        # Format: [HH:MM:SS] LEVEL: message
        formatted_message = f"[{timestamp}] {level:8s}: {message}\n"

        # Insert with appropriate tag for coloring
        self.log_text.insert('end', formatted_message, level)

        # Auto-scroll if enabled
        if self.auto_scroll:
            self.log_text.see('end')

        self.log_text.config(state='disabled')

        # Update count
        self._update_count()

    def _clear_log(self):
        """Clear all log entries"""
        response = messagebox.askyesno(
            "Clear Log",
            "Are you sure you want to clear all log entries?\n\nThis cannot be undone.",
            icon='warning'
        )

        if response:
            self.log_entries.clear()
            self.log_text.config(state='normal')
            self.log_text.delete('1.0', 'end')
            self.log_text.config(state='disabled')
            self._update_count()

    def _save_log(self):
        """Save log to file"""
        if not self.log_entries:
            messagebox.showinfo("No Logs", "There are no log entries to save.")
            return

        # Ask for file location
        filename = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"nx_migrator_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(f"NX Migrator Pro - Log Export\n")
                    f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 80 + "\n\n")

                    for entry in self.log_entries:
                        f.write(f"[{entry['timestamp']}] {entry['level']:8s}: {entry['message']}\n")

                messagebox.showinfo("Log Saved", f"Log saved successfully to:\n{filename}")

            except Exception as e:
                messagebox.showerror("Save Failed", f"Failed to save log file:\n\n{str(e)}")

    def _toggle_auto_scroll(self):
        """Toggle auto-scroll feature"""
        self.auto_scroll = self.auto_scroll_var.get()

        # If enabled, scroll to bottom now
        if self.auto_scroll:
            self.log_text.see('end')

    def _update_count(self):
        """Update the entry count label"""
        count = len(self.log_entries)
        self.count_label.config(text=f"{count} {'entry' if count == 1 else 'entries'}")


class GUILogHandler(logging.Handler):
    """Custom logging handler that sends logs to the GUI log panel"""

    def __init__(self, log_panel):
        super().__init__()
        self.log_panel = log_panel

    def emit(self, record):
        """Emit a log record to the GUI"""
        try:
            # Format the message
            msg = self.format(record)

            # Remove timestamp and level from message (we add our own)
            # The message format from logging is: "timestamp [LEVEL] name: message"
            # We want to extract just the actual message part

            # Try to extract the actual message after the logger name
            if ':' in msg:
                # Find the last occurrence of ': ' which separates logger name from message
                parts = msg.split(': ', 1)
                if len(parts) > 1:
                    actual_message = parts[1]
                else:
                    actual_message = msg
            else:
                actual_message = msg

            # Send to GUI log panel (thread-safe)
            if self.log_panel:
                # Schedule GUI update in the main thread
                try:
                    # Try to use after() if available
                    self.log_panel.after(0, lambda: self.log_panel.append_log(
                        record.levelname,
                        actual_message
                    ))
                except:
                    # Fallback: direct call (might not be thread-safe but better than nothing)
                    self.log_panel.append_log(record.levelname, actual_message)

        except Exception:
            # Silently ignore errors in logging handler to prevent infinite loops
            pass
