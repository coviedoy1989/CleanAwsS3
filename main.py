"""
Entry point for the CleanS3 application.
"""
import sys
from PyQt6.QtWidgets import QApplication
from gui import MainWindow


def main():
    """Main function that initializes and runs the application."""
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    # Run event loop
    sys.exit(app.exec())


if __name__ == '__main__':
    main()




