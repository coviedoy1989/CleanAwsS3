"""
Graphical interface for the CleanS3 application.
"""
import sys
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
    QLineEdit, QPushButton, QTextEdit, QLabel, QProgressBar,
    QMessageBox, QGroupBox, QFormLayout, QSpinBox, QCompleter
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QStringListModel
from PyQt6.QtGui import QFont
from s3_cleaner import S3Cleaner


class S3OperationThread(QThread):
    """Thread to execute S3 operations without blocking the UI."""
    
    progress = pyqtSignal(str)
    finished = pyqtSignal(bool, str)
    
    def __init__(self, operation_type: str, access_key: str, secret_key: str,
                 region: str = None, max_workers: int = 10, **kwargs):
        super().__init__()
        self.operation_type = operation_type
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.max_workers = max_workers
        self.kwargs = kwargs
        self._cancelled = False
        self._paused = False
        self.cleaner = None
    
    def cancel(self):
        """Cancel the operation."""
        self._cancelled = True
    
    def pause(self):
        """Pause the operation."""
        self._paused = True
    
    def resume(self):
        """Resume the operation."""
        self._paused = False
    
    def is_paused(self):
        """Return whether it is paused."""
        return self._paused
    
    def run(self):
        """Execute the S3 operation."""
        try:
            self.cleaner = S3Cleaner(self.access_key, self.secret_key, self.region)
            
            if self.operation_type == 'clean':
                bucket_name = self.kwargs.get('bucket_name')
                success = self.cleaner.clean_bucket(
                    bucket_name, 
                    self._progress_callback, 
                    max_workers=self.max_workers,
                    cancel_flag=lambda: self._cancelled,
                    pause_flag=lambda: self._paused
                )
                if self._cancelled:
                    message = "Operation cancelled by user"
                    success = False
                else:
                    message = "Cleanup completed successfully" if success else "Error during cleanup"
                self.finished.emit(success, message)
            
            elif self.operation_type == 'copy':
                success = self.cleaner.copy_objects(
                    self.kwargs.get('source_bucket'),
                    self.kwargs.get('source_prefix', ''),
                    self.kwargs.get('dest_bucket'),
                    self.kwargs.get('dest_prefix', ''),
                    self._progress_callback,
                    max_workers=self.max_workers,
                    cancel_flag=lambda: self._cancelled,
                    pause_flag=lambda: self._paused
                )
                if self._cancelled:
                    message = "Operation cancelled by user"
                    success = False
                else:
                    message = "Copy completed successfully" if success else "Error during copy"
                self.finished.emit(success, message)
        
        except Exception as e:
            self.progress.emit(f"ERROR: {str(e)}")
            self.finished.emit(False, f"Error: {str(e)}")
    
    def _progress_callback(self, message: str):
        """Callback to report progress."""
        self.progress.emit(message)


class MainWindow(QMainWindow):
    """Main window of the application."""
    
    def __init__(self):
        super().__init__()
        self.operation_thread = None
        # Variables to store credentials
        self.access_key = ""
        self.secret_key = ""
        self.region = None
        self.max_workers = 10  # Default concurrency value
        self.buckets_list = []  # List of available buckets
        self.init_ui()
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("CleanS3 - S3 Bucket Management")
        self.setGeometry(100, 100, 900, 700)
        
        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principal
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Tabs
        tabs = QTabWidget()
        main_layout.addWidget(tabs)
        
        # Configuration tab (first)
        config_tab = self.create_config_tab()
        tabs.addTab(config_tab, "Configuration")
        
        # Clean tab
        clean_tab = self.create_clean_tab()
        tabs.addTab(clean_tab, "Clean Bucket")
        
        # Copy tab
        copy_tab = self.create_copy_tab()
        tabs.addTab(copy_tab, "Copy Objects")
        
        # Shared logs area
        logs_group = QGroupBox("Logs and Progress")
        logs_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier", 9))
        logs_layout.addWidget(self.log_text)
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        logs_layout.addWidget(self.progress_bar)
        
        # Control buttons
        control_layout = QHBoxLayout()
        self.stop_button = QPushButton("⏹ Stop")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.on_stop_clicked)
        control_layout.addWidget(self.stop_button)
        
        self.pause_button = QPushButton("⏸ Pause")
        self.pause_button.setEnabled(False)
        self.pause_button.clicked.connect(self.on_pause_clicked)
        control_layout.addWidget(self.pause_button)
        
        control_layout.addStretch()
        logs_layout.addLayout(control_layout)
        
        logs_group.setLayout(logs_layout)
        main_layout.addWidget(logs_group)
    
    def create_config_tab(self) -> QWidget:
        """Create the configuration tab with credentials."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Credentials group
        creds_group = QGroupBox("AWS Credentials")
        creds_layout = QFormLayout()
        
        self.config_access_key = QLineEdit()
        self.config_access_key.setPlaceholderText("Enter Access Key ID")
        creds_layout.addRow("Access Key ID:", self.config_access_key)
        
        self.config_secret_key = QLineEdit()
        self.config_secret_key.setEchoMode(QLineEdit.EchoMode.Password)
        self.config_secret_key.setPlaceholderText("Enter Secret Access Key")
        creds_layout.addRow("Secret Access Key:", self.config_secret_key)
        
        self.config_region = QLineEdit()
        self.config_region.setPlaceholderText("us-east-1 (optional)")
        creds_layout.addRow("Region:", self.config_region)
        
        creds_group.setLayout(creds_layout)
        layout.addWidget(creds_group)
        
        # Performance configuration group
        perf_group = QGroupBox("Performance Configuration")
        perf_layout = QFormLayout()
        
        self.config_max_workers = QSpinBox()
        self.config_max_workers.setMinimum(1)
        self.config_max_workers.setMaximum(100)
        self.config_max_workers.setValue(10)
        self.config_max_workers.setToolTip("Number of simultaneous operations (1-100). Higher value = faster but more load on AWS.")
        self.config_max_workers.valueChanged.connect(self.on_concurrency_changed)
        perf_layout.addRow("Concurrency (Workers):", self.config_max_workers)
        
        perf_group.setLayout(perf_layout)
        layout.addWidget(perf_group)
        
        # Button to save/validate credentials
        self.test_button = QPushButton("Validate Credentials")
        self.test_button.clicked.connect(self.on_test_credentials)
        layout.addWidget(self.test_button)
        
        # Status label
        self.credentials_status = QLabel("Credentials not configured")
        self.credentials_status.setStyleSheet("color: orange;")
        layout.addWidget(self.credentials_status)
        
        layout.addStretch()
        
        return widget
    
    def create_clean_tab(self) -> QWidget:
        """Create the clean tab."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Informative message
        info_label = QLabel("Note: Credentials must be configured in the 'Configuration' tab")
        info_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(info_label)
        
        # Bucket group
        bucket_group = QGroupBox("Bucket Information")
        bucket_layout = QFormLayout()
        
        # Search field with autocomplete
        bucket_search_layout = QHBoxLayout()
        self.clean_bucket = QLineEdit()
        self.clean_bucket.setPlaceholderText("Search or enter bucket name")
        
        # Autocomplete
        self.clean_bucket_completer = QCompleter()
        self.clean_bucket_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.clean_bucket_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.clean_bucket.setCompleter(self.clean_bucket_completer)
        
        bucket_search_layout.addWidget(self.clean_bucket)
        
        # Button to refresh bucket list
        refresh_buckets_btn = QPushButton("🔄")
        refresh_buckets_btn.setToolTip("Refresh bucket list")
        refresh_buckets_btn.setMaximumWidth(40)
        refresh_buckets_btn.clicked.connect(lambda: self.load_buckets_list('clean'))
        bucket_search_layout.addWidget(refresh_buckets_btn)
        
        bucket_layout.addRow("Bucket:", bucket_search_layout)
        
        bucket_group.setLayout(bucket_layout)
        layout.addWidget(bucket_group)
        
        # Action button
        self.clean_button = QPushButton("Clean Bucket")
        self.clean_button.clicked.connect(self.on_clean_clicked)
        layout.addWidget(self.clean_button)
        
        layout.addStretch()
        
        return widget
    
    def create_copy_tab(self) -> QWidget:
        """Create the copy tab."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Informative message
        info_label = QLabel("Note: Credentials must be configured in the 'Configuration' tab")
        info_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(info_label)
        
        # Source group
        source_group = QGroupBox("Source")
        source_layout = QFormLayout()
        
        # Source bucket field with autocomplete
        source_bucket_layout = QHBoxLayout()
        self.source_bucket = QLineEdit()
        self.source_bucket.setPlaceholderText("Search or enter source bucket name")
        
        # Autocomplete
        self.source_bucket_completer = QCompleter()
        self.source_bucket_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.source_bucket_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.source_bucket.setCompleter(self.source_bucket_completer)
        
        source_bucket_layout.addWidget(self.source_bucket)
        
        refresh_source_btn = QPushButton("🔄")
        refresh_source_btn.setToolTip("Refresh bucket list")
        refresh_source_btn.setMaximumWidth(40)
        refresh_source_btn.clicked.connect(lambda: self.load_buckets_list('source'))
        source_bucket_layout.addWidget(refresh_source_btn)
        
        source_layout.addRow("Source Bucket:", source_bucket_layout)
        
        self.source_prefix = QLineEdit()
        self.source_prefix.setPlaceholderText("source/path/ (optional)")
        source_layout.addRow("Source Path:", self.source_prefix)
        
        source_group.setLayout(source_layout)
        layout.addWidget(source_group)
        
        # Destination group
        dest_group = QGroupBox("Destination")
        dest_layout = QFormLayout()
        
        # Destination bucket field with autocomplete
        dest_bucket_layout = QHBoxLayout()
        self.dest_bucket = QLineEdit()
        self.dest_bucket.setPlaceholderText("Search or enter destination bucket name")
        
        # Autocomplete
        self.dest_bucket_completer = QCompleter()
        self.dest_bucket_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.dest_bucket_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.dest_bucket.setCompleter(self.dest_bucket_completer)
        
        dest_bucket_layout.addWidget(self.dest_bucket)
        
        refresh_dest_btn = QPushButton("🔄")
        refresh_dest_btn.setToolTip("Refresh bucket list")
        refresh_dest_btn.setMaximumWidth(40)
        refresh_dest_btn.clicked.connect(lambda: self.load_buckets_list('dest'))
        dest_bucket_layout.addWidget(refresh_dest_btn)
        
        dest_layout.addRow("Destination Bucket:", dest_bucket_layout)
        
        self.dest_prefix = QLineEdit()
        self.dest_prefix.setPlaceholderText("destination/path/ (optional)")
        dest_layout.addRow("Destination Path:", self.dest_prefix)
        
        dest_group.setLayout(dest_layout)
        layout.addWidget(dest_group)
        
        # Action button
        self.copy_button = QPushButton("Copy Objects")
        self.copy_button.clicked.connect(self.on_copy_clicked)
        layout.addWidget(self.copy_button)
        
        layout.addStretch()
        
        return widget
    
    def log(self, message: str):
        """Add a message to the logs area."""
        self.log_text.append(message)
        # Auto-scroll to end
        cursor = self.log_text.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_text.setTextCursor(cursor)
    
    def get_credentials(self) -> tuple:
        """Get the configured credentials."""
        return (self.access_key, self.secret_key, self.region)
    
    def check_credentials_configured(self) -> bool:
        """Check if credentials are configured."""
        if not self.access_key or not self.secret_key:
            QMessageBox.warning(
                self, 
                "Credentials not configured",
                "Please configure AWS credentials in the 'Configuration' tab before continuing."
            )
            return False
        return True
    
    def on_concurrency_changed(self, value):
        """Update concurrency when value changes."""
        self.max_workers = value
        if self.credentials_status.text().startswith("✓"):
            # Update message if credentials are already saved
            self.credentials_status.setText(f"✓ Valid credentials saved (Concurrency: {value})")
    
    def on_test_credentials(self):
        """Validate and save credentials and configuration."""
        access_key = self.config_access_key.text().strip()
        secret_key = self.config_secret_key.text().strip()
        region = self.config_region.text().strip() or None
        max_workers = self.config_max_workers.value()
        
        if not access_key or not secret_key:
            QMessageBox.warning(self, "Error", "Please enter Access Key ID and Secret Access Key")
            self.credentials_status.setText("Credentials not configured")
            self.credentials_status.setStyleSheet("color: orange;")
            return
        
        try:
            cleaner = S3Cleaner(access_key, secret_key, region)
            if cleaner.test_connection():
                # Save credentials and configuration
                self.access_key = access_key
                self.secret_key = secret_key
                self.region = region
                self.max_workers = max_workers
                self.credentials_status.setText(f"✓ Valid credentials saved (Concurrency: {max_workers})")
                self.credentials_status.setStyleSheet("color: green;")
                
                # Automatically load bucket list
                self.load_buckets_list()
                
                QMessageBox.information(self, "Success", f"Credentials validated and saved successfully.\nConcurrency configured: {max_workers} workers")
            else:
                self.credentials_status.setText("✗ Invalid credentials")
                self.credentials_status.setStyleSheet("color: red;")
                QMessageBox.warning(self, "Error", "Could not connect to AWS. Please verify credentials.")
        except Exception as e:
            self.credentials_status.setText("✗ Connection error")
            self.credentials_status.setStyleSheet("color: red;")
            QMessageBox.warning(self, "Error", f"Connection error: {str(e)}")
    
    def load_buckets_list(self, target: str = 'all'):
        """Load bucket list from AWS and update autocompletes."""
        if not self.check_credentials_configured():
            return
        
        try:
            access_key, secret_key, region = self.get_credentials()
            cleaner = S3Cleaner(access_key, secret_key, region)
            buckets = cleaner.list_buckets()
            
            if buckets:
                self.buckets_list = buckets
                model = QStringListModel(buckets)
                
                # Update autocompletes according to target
                if target == 'all' or target == 'clean':
                    self.clean_bucket_completer.setModel(model)
                if target == 'all' or target == 'source':
                    self.source_bucket_completer.setModel(model)
                if target == 'all' or target == 'dest':
                    self.dest_bucket_completer.setModel(model)
                
                self.log(f"✓ {len(buckets)} buckets loaded")
            else:
                self.log("⚠ No buckets found or no permissions to list them")
                
        except Exception as e:
            self.log(f"ERROR loading buckets: {str(e)}")
            QMessageBox.warning(self, "Error", f"Error loading bucket list: {str(e)}")
    
    def on_clean_clicked(self):
        """Handle the clean button click."""
        if not self.check_credentials_configured():
            return
        
        access_key, secret_key, region = self.get_credentials()
        bucket_name = self.clean_bucket.text().strip()
        
        if not bucket_name:
            QMessageBox.warning(self, "Error", "Please enter the bucket name")
            return
        
        # Verify that the bucket exists (quick check)
        try:
            cleaner = S3Cleaner(access_key, secret_key, region)
            if not cleaner.bucket_exists(bucket_name):
                QMessageBox.warning(self, "Error", f"The bucket '{bucket_name}' does not exist or is not accessible")
                return
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error: {str(e)}")
            return
        
        # Confirmation dialog (without count to avoid delays)
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Warning)
        msg.setWindowTitle("Confirm Cleanup")
        msg.setText(f"Are you sure you want to delete ALL objects from the bucket '{bucket_name}'?")
        msg.setInformativeText(
            f"This operation is IRREVERSIBLE.\n\n"
            f"ALL objects and versions in the bucket will be deleted.\n"
            f"This action cannot be undone."
        )
        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        msg.setDefaultButton(QMessageBox.StandardButton.No)
        
        if msg.exec() != QMessageBox.StandardButton.Yes:
            return
        
        # Execute cleanup in thread
        self.start_clean_operation(access_key, secret_key, region, bucket_name)
    
    def on_copy_clicked(self):
        """Handle the copy button click."""
        if not self.check_credentials_configured():
            return
        
        access_key, secret_key, region = self.get_credentials()
        source_bucket = self.source_bucket.text().strip()
        source_prefix = self.source_prefix.text().strip()
        dest_bucket = self.dest_bucket.text().strip()
        dest_prefix = self.dest_prefix.text().strip()
        
        if not source_bucket or not dest_bucket:
            QMessageBox.warning(self, "Error", "Please enter the bucket names")
            return
        
        # Verify that buckets exist (quick check)
        try:
            cleaner = S3Cleaner(access_key, secret_key, region)
            
            if not cleaner.bucket_exists(source_bucket):
                QMessageBox.warning(self, "Error", f"The source bucket '{source_bucket}' does not exist or is not accessible")
                return
            
            if not cleaner.bucket_exists(dest_bucket):
                QMessageBox.warning(self, "Error", f"The destination bucket '{dest_bucket}' does not exist or is not accessible")
                return
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error: {str(e)}")
            return
        
        # Confirmation dialog (without count to avoid delays)
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Question)
        msg.setWindowTitle("Confirm Copy")
        msg.setText(f"Do you want to copy objects from '{source_bucket}' to '{dest_bucket}'?")
        msg.setInformativeText(
            f"Source: s3://{source_bucket}/{source_prefix or ''}\n"
            f"Destination: s3://{dest_bucket}/{dest_prefix or ''}\n\n"
            f"All objects from the source path will be copied to the destination path."
        )
        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        msg.setDefaultButton(QMessageBox.StandardButton.Yes)
        
        if msg.exec() != QMessageBox.StandardButton.Yes:
            return
        
        # Execute copy in thread
        self.start_copy_operation(
            access_key, secret_key, region,
            source_bucket, source_prefix,
            dest_bucket, dest_prefix
        )
    
    def start_clean_operation(self, access_key: str, secret_key: str, region: str, bucket_name: str):
        """Start the cleanup operation in a thread."""
        if self.operation_thread and self.operation_thread.isRunning():
            QMessageBox.warning(self, "Warning", "An operation is already in progress")
            return
        
        self.log_text.clear()
        self.log(f"Starting cleanup of bucket '{bucket_name}'...")
        self.log(f"Concurrency configured: {self.max_workers} workers")
        
        self.clean_button.setEnabled(False)
        self.copy_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.stop_button.setEnabled(True)
        self.pause_button.setEnabled(True)
        self.pause_button.setText("⏸ Pause")
        
        # Update max_workers from SpinBox if visible
        if hasattr(self, 'config_max_workers'):
            self.max_workers = self.config_max_workers.value()
        
        self.operation_thread = S3OperationThread(
            'clean', access_key, secret_key, region,
            max_workers=self.max_workers,
            bucket_name=bucket_name
        )
        self.operation_thread.progress.connect(self.log)
        self.operation_thread.finished.connect(self.on_operation_finished)
        self.operation_thread.start()
    
    def start_copy_operation(self, access_key: str, secret_key: str, region: str,
                            source_bucket: str, source_prefix: str,
                            dest_bucket: str, dest_prefix: str):
        """Start the copy operation in a thread."""
        if self.operation_thread and self.operation_thread.isRunning():
            QMessageBox.warning(self, "Warning", "An operation is already in progress")
            return
        
        self.log_text.clear()
        self.log(f"Starting copy from '{source_bucket}' to '{dest_bucket}'...")
        self.log(f"Concurrency configured: {self.max_workers} workers")
        
        self.clean_button.setEnabled(False)
        self.copy_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.stop_button.setEnabled(True)
        self.pause_button.setEnabled(True)
        self.pause_button.setText("⏸ Pause")
        
        # Update max_workers from SpinBox if visible
        if hasattr(self, 'config_max_workers'):
            self.max_workers = self.config_max_workers.value()
        
        self.operation_thread = S3OperationThread(
            'copy', access_key, secret_key, region,
            max_workers=self.max_workers,
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            dest_bucket=dest_bucket,
            dest_prefix=dest_prefix
        )
        self.operation_thread.progress.connect(self.log)
        self.operation_thread.finished.connect(self.on_operation_finished)
        self.operation_thread.start()
    
    def on_operation_finished(self, success: bool, message: str):
        """Handle the completion of an operation."""
        self.clean_button.setEnabled(True)
        self.copy_button.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.stop_button.setEnabled(False)
        self.pause_button.setEnabled(False)
        
        if success:
            self.log(f"✓ {message}")
            QMessageBox.information(self, "Success", message)
        else:
            self.log(f"✗ {message}")
            if "cancelled" not in message.lower():
                QMessageBox.warning(self, "Error", message)
    
    def on_stop_clicked(self):
        """Handle the Stop button click."""
        if self.operation_thread and self.operation_thread.isRunning():
            reply = QMessageBox.question(
                self, 
                "Confirm Stop",
                "Are you sure you want to stop the operation in progress?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.operation_thread.cancel()
                self.log("⏹ Operation cancelled by user...")
                self.stop_button.setEnabled(False)
                self.pause_button.setEnabled(False)
    
    def on_pause_clicked(self):
        """Handle the Pause/Resume button click."""
        if self.operation_thread and self.operation_thread.isRunning():
            if self.operation_thread.is_paused():
                self.operation_thread.resume()
                self.pause_button.setText("⏸ Pause")
                self.log("▶ Operation resumed")
            else:
                self.operation_thread.pause()
                self.pause_button.setText("▶ Resume")
                self.log("⏸ Operation paused...")

