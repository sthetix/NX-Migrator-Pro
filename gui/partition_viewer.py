"""
Partition Viewer Widget - Display partition layout visually
"""

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import Canvas
from ttkbootstrap.scrolled import ScrolledFrame

class PartitionViewerFrame(ttk.Frame):
    """Widget to display partition layout"""

    def __init__(self, parent, title="Partitions"):
        super().__init__(parent)

        self.title = title
        self.layout = None
        self.disk_info = None
        self._redraw_attempts = 0  # Track redraw attempts

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        """Create widgets"""

        # Create a scrolled frame to contain all content with fixed height
        self.scrolled_container = ScrolledFrame(self, autohide=True)

        # Content frame inside the scrolled container
        self.content_frame = self.scrolled_container

        # Title
        self.title_label = ttk.Label(
            self.content_frame,
            text=self.title,
            font=("Segoe UI", 12, "bold")
        )

        # Disk info
        self.disk_label = ttk.Label(
            self.content_frame,
            text="No disk selected",
            font=("Segoe UI", 9),
            bootstyle=SECONDARY
        )

        # Legend frame (horizontal layout for legend items)
        self.legend_frame = ttk.Frame(self.content_frame)

        # Visual partition bar
        self.partition_canvas = Canvas(
            self.content_frame,
            height=60,
            bg='#2b2b2b',
            highlightthickness=0
        )

        # Partition list
        self.partition_frame = ttk.Frame(self.content_frame)

        # Scrollable partition list
        self.partition_list = ttk.Treeview(
            self.partition_frame,
            columns=('name', 'type', 'start', 'size'),
            show='headings',
            height=6
        )

        self.partition_list.heading('name', text='Partition')
        self.partition_list.heading('type', text='Type')
        self.partition_list.heading('start', text='Start (MB)')
        self.partition_list.heading('size', text='Size (MB)')

        self.partition_list.column('name', width=120)
        self.partition_list.column('type', width=100)
        self.partition_list.column('start', width=100)
        self.partition_list.column('size', width=100)

        # Scrollbar
        self.scrollbar = ttk.Scrollbar(
            self.partition_frame,
            orient=VERTICAL,
            command=self.partition_list.yview
        )
        self.partition_list.configure(yscrollcommand=self.scrollbar.set)

        # Summary label
        self.summary_label = ttk.Label(
            self.content_frame,
            text="",
            font=("Segoe UI", 9),
            bootstyle=INFO
        )

    def _layout_widgets(self):
        """Layout widgets"""

        # Pack the scrolled container to fill the frame
        self.scrolled_container.pack(fill=BOTH, expand=YES)

        # Layout widgets inside the scrolled content frame
        self.title_label.pack(pady=(5, 2))
        self.disk_label.pack(pady=(0, 2))
        self.legend_frame.pack(pady=(2, 5))

        self.partition_canvas.pack(fill=X, padx=10, pady=(5, 10))

        self.partition_frame.pack(fill=BOTH, expand=YES, padx=10, pady=(0, 5))
        self.partition_list.pack(side=LEFT, fill=BOTH, expand=YES)
        self.scrollbar.pack(side=RIGHT, fill=Y)

        self.summary_label.pack(pady=(2, 5))

    def display_layout(self, layout, disk_info):
        """Display partition layout"""
        import logging
        logger = logging.getLogger(__name__)

        self.layout = layout
        self.disk_info = disk_info
        self._redraw_attempts = 0  # Reset redraw attempts

        logger.info(f"=== display_layout called for {self.title} ===")
        logger.info(f"Disk: {disk_info['name']} - {disk_info['size_gb']:.2f} GB")
        logger.info(f"Layout has {len(layout.partitions)} partitions")
        for p in layout.partitions:
            logger.info(f"  - {p.name} ({p.category}): {p.size_mb} MB")

        # Update disk label
        self.disk_label.config(text=f"{disk_info['name']} - {disk_info['size_gb']:.2f} GB")

        # Clear previous data
        for item in self.partition_list.get_children():
            self.partition_list.delete(item)

        # Add partitions to tree
        for partition in layout.partitions:
            self.partition_list.insert('', END, values=(
                partition.name,
                partition.type_name,
                f"{partition.start_sector * 512 // (1024 * 1024):,}",
                f"{partition.size_mb:,}"
            ))

        # Draw visual partition bar
        self._draw_partition_bar()

        # Update summary
        summary = layout.get_summary()
        self.summary_label.config(text=summary)
        logger.info(f"Summary: {summary}")

    def _draw_partition_bar(self):
        """Draw visual partition bar"""
        import logging
        logger = logging.getLogger(__name__)

        self.partition_canvas.delete('all')

        if not self.layout or not self.disk_info:
            return

        # Force canvas to update its geometry to get actual width
        self.partition_canvas.update_idletasks()

        canvas_width = self.partition_canvas.winfo_width()
        if canvas_width <= 1:
            # Still not rendered, schedule a redraw after idle (max 5 attempts)
            self._redraw_attempts += 1
            if self._redraw_attempts < 5:
                logger.warning(f"Canvas not yet rendered (width={canvas_width}), scheduling redraw (attempt {self._redraw_attempts}/5)...")
                self.partition_canvas.after(100, self._draw_partition_bar)
                return
            else:
                logger.warning(f"Canvas still not rendered after {self._redraw_attempts} attempts, giving up and using default width")
                canvas_width = 800  # Fallback to default

        canvas_height = 60

        logger.info(f"=== _draw_partition_bar for {self.title} ===")
        logger.info(f"Canvas width: {canvas_width}")
        logger.info(f"Total disk size: {self.disk_info['size_bytes']} bytes ({self.disk_info['size_bytes'] / (1024*1024*1024):.2f} GB)")

        # Color scheme
        colors = {
            'FAT32': '#4CAF50',      # Green
            'Linux': '#2196F3',       # Blue
            'Android': '#FF9800',     # Orange
            'emuMMC': '#9C27B0',      # Purple
            'Free': '#555555'         # Gray
        }

        total_size = self.disk_info['size_bytes']
        MIN_WIDTH = 2.0  # Minimum visible width in pixels

        # First pass: calculate natural widths and identify partitions needing minimum width
        partition_widths = []
        total_min_width = 0
        for partition in self.layout.partitions:
            partition_size = partition.size_mb * 1024 * 1024
            natural_width = (partition_size / total_size) * canvas_width

            if natural_width < MIN_WIDTH:
                partition_widths.append((partition, MIN_WIDTH, True))  # (partition, width, is_min)
                total_min_width += MIN_WIDTH
            else:
                partition_widths.append((partition, natural_width, False))

        # Second pass: scale down larger partitions to make room for minimum widths
        remaining_width = canvas_width - total_min_width
        scalable_total = sum(w for _, w, is_min in partition_widths if not is_min)

        if scalable_total > 0:
            scale_factor = remaining_width / scalable_total
        else:
            scale_factor = 1.0

        # Apply scaling to non-minimum partitions
        final_widths = []
        for partition, width, is_min in partition_widths:
            if is_min:
                final_widths.append((partition, width))
            else:
                final_widths.append((partition, width * scale_factor))

        # Draw partitions
        x_offset = 0
        for partition, width in final_widths:
            # Determine color
            color = colors.get(partition.category, '#666666')

            logger.info(f"Drawing partition {partition.name} ({partition.category}): size={partition.size_mb}MB, width={width:.2f}px at x={x_offset:.2f}, color={color}")

            # Draw rectangle
            self.partition_canvas.create_rectangle(
                x_offset, 5, x_offset + width, canvas_height - 5,
                fill=color,
                outline='white',
                width=1
            )

            # Add label if wide enough
            if width > 50:
                self.partition_canvas.create_text(
                    x_offset + width / 2, canvas_height / 2,
                    text=f"{partition.name}\n{partition.size_mb} MB",
                    fill='white',
                    font=("Segoe UI", 8, "bold")
                )

            x_offset += width

        logger.info(f"Final x_offset: {x_offset:.2f}px (should be ~{canvas_width}px)")

        # Update legend in separate frame
        self._update_legend(colors)

    def _update_legend(self, colors):
        """Update the legend frame with color indicators"""
        # Clear previous legend items
        for widget in self.legend_frame.winfo_children():
            widget.destroy()

        # Create legend items only for categories present in the layout
        for category, color in colors.items():
            if any(p.category == category for p in self.layout.partitions):
                # Create a frame for each legend item
                item_frame = ttk.Frame(self.legend_frame)
                item_frame.pack(side=LEFT, padx=5)

                # Color indicator (small canvas for colored square)
                color_canvas = Canvas(
                    item_frame,
                    width=12,
                    height=12,
                    bg='#2b2b2b',
                    highlightthickness=0
                )
                color_canvas.pack(side=LEFT, padx=(0, 3))
                color_canvas.create_rectangle(
                    0, 0, 12, 12,
                    fill=color,
                    outline='white'
                )

                # Category label
                label = ttk.Label(
                    item_frame,
                    text=category,
                    font=("Segoe UI", 8)
                )
                label.pack(side=LEFT)

    def update_title(self, new_title):
        """Update the title label"""
        self.title = new_title
        self.title_label.config(text=new_title)

    def clear(self):
        """Clear all partition data"""
        self.layout = None
        self.disk_info = None

        self.disk_label.config(text="No disk selected")
        self.partition_canvas.delete('all')

        # Clear legend
        for widget in self.legend_frame.winfo_children():
            widget.destroy()

        for item in self.partition_list.get_children():
            self.partition_list.delete(item)

        self.summary_label.config(text="")
