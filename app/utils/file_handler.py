import os
import shutil

def save_uploaded_file(uploaded_file, destination_path):
    with open(destination_path, "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file, buffer)
