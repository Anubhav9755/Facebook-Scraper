import os
import subprocess
import sys

port = os.environ.get("PORT", "8080")
print(f"Starting on port {port}", flush=True)

cmd = [
    "gunicorn", "app:app",
    "--bind", f"0.0.0.0:{port}",
    "--workers", "2",
    "--timeout", "300",
]
subprocess.run(cmd)
