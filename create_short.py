from moviepy.editor import *
from pathlib import Path
from dataclasses import dataclass
from random import randint

import json


@dataclass
class accountConfig:
    """Generador de videos cortos para plataformas sociales"""
    
    def __init__(self, account):
        """Inicializa el generador con la configuración proporcionada"""
        self.language = account["language"]
        self.audio_links_path = Path("./Links") / f"audio_{account['language']}.csv"
        self.videos_links_path = Path("./Links") / f'{account["edition"]["type"]}_{account["edition"]["content"]}.csv'
        self.audio_db_path = Path("./DB") / f"audio_{account['language']}.csv"
        self.videos_db_path = Path("./DB") / f'{account["edition"]["type"]}_{account["edition"]["content"]}.csv'
        self.audio_folder_path = Path("./Media") / account["type"] / "Audios" / account["language"]
        self.video_folder_path = Path("./Media") / account["type"] / account["edition"]["type"] / account["edition"]["content"]
        self.caption_folder_path = Path("./Media") / account["type"] / "Captions" / account["language"]

"""
self.language
self.audio_links_path
self.videos_links_path
self.audio_db_path
self.videos_db_path
self.audio_folder_path
self.video_folder_path
self.caption_folder_path
"""



archivo = "./Config/config.json"

try:
    print(f"[Config] Cargando configuración para '{archivo}'")

    with open(archivo, 'r', encoding='utf-8') as config_file:
        accounts_config = json.load(config_file)
except Exception as e:
    raise RuntimeError(f"Error leyendo configuración: {e}")

def get_concatenation_clips(files):
    print("")
    
    clips_list = []
    for file in files:
        start_time = randint(2,10)
        duration = randint(5,10)
        clip = VideoFileClip(file).subclip(start_time, start_time + duration)
        clips_list.append(clip)
        print(f" --> Selecting '{file}' '{duration}'")
    
    print("")
    return concatenate_videoclips(clips_list, method="compose").without_audio()

for user_config in accounts_config:
    account = accountConfig(user_config)

    files = [str(f) for f in account.video_folder_path.iterdir() if f.is_file()]
    if not files: continue

    concatenation = get_concatenation_clips(files)
    concatenation.write_videofile(
        "output/output_file_path.mp4", 
        codec="libx264", 
        audio_codec="aac", 
        fps=concatenation.fps, 
        threads=4, 
        preset="ultrafast",
        remove_temp=True
    ) 
