import subprocess
import os

# Step into the InCognitive directory
os.chdir("fcm_incognitive/data/InCognitive")

# install requirements
#subprocess.call(["pip", "install", "-r", "requirements.txt"])

# Run the main GUI application
#subprocess.call(["python", "InCognitiveApp/main.py"])

# ABOVE DID NOT WORK - need to 
# 1) navigate to the Incognitive folder via the command line (cd fcm_incognitive\data\Incognitive) and 
# 2) enter: bokeh serve IncognitiveApp --show