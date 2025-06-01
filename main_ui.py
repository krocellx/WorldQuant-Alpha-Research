# launch_streamlit.py
import subprocess
import os

from src.utilities.parameters import BASE_DIR

# Path to your Streamlit app file
app_path = "alpha_tracker_editing_ui.py"
app_path = os.path.join(BASE_DIR, 'src', 'ui', app_path)


# Run Streamlit
subprocess.run([
    "poetry", "run", "streamlit", "run", app_path,
    "--server.port=8501",
    "--server.address=0.0.0.0"  # This allows connections from outside the container
])