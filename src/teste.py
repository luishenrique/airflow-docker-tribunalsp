from pathlib import Path
import os 

def create_parent_folder(file_path):
    (Path(file_path).parent).mkdir(parents=True, exist_ok=True)  
    print (Path(file_path).parent)
    print (os.getcwd())
    #os.mkdir("Teste")


create_parent_folder(os.getcwd())
