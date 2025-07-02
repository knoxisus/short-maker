from moviepy.editor import VideoFileClip, CompositeVideoClip, vfx
from pathlib import Path
from dataclasses import dataclass

import os
import json
import pickle

import whisper_timestamped
WM = whisper_timestamped.load_model("small", device = "cpu")

RESOLUTION = 9/16

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

def _create_folder(path: Path):
    """Crea un directorio si no existe."""
    if not path.exists():
        print(f"[Config] Creando la ruta '{path}'.")
        path.mkdir(parents=True, exist_ok=True)

def _create_file(path: Path):
    """Crea un archivo si no existe y le agrega el título 'video_id' si es CSV."""
    if not path.exists():
        print(f"[Config] Creando el archivo '{path}'.")
        path.touch()
        if path.suffix == ".csv":
            with path.open("w", encoding="utf-8") as f:
                f.write("video_id\n")

def write_folders(accounts):
    _create_folder(Path("./output"))
    _create_folder(Path("./Media"))
    _create_folder(Path("./Links"))

    db_path = Path("./DB")
    _create_folder(db_path)

    for account in accounts:
        _create_file(db_path / f"{account['name']}.csv")
        
        _create_file(db_path / f"audio_{account['language']}.csv")
        _create_file(db_path / f'{account["edition"]["type"]}_{account["edition"]["content"]}.csv')

        _create_file(Path("./Links") / f"audio_{account['language']}.csv")
        _create_file(Path("./Links") / f'{account["edition"]["type"]}_{account["edition"]["content"]}.csv')


        media_path = Path("./Media") / account["type"]
        _create_folder(media_path)
        _create_folder(media_path / "Audios" / account["language"])
        _create_folder(media_path / account["edition"]["type"] / account["edition"]["content"])
        _create_folder(media_path / "Captions" / account["language"])

def download_media(accounts):  
    for account in accounts:
        config = accountConfig(account)
        missing_audio = []
        missing_videos = []

        if config.audio_links_path.exists() and config.audio_db_path.exists():
            with config.audio_links_path.open("r", encoding="utf-8") as f_links, \
                 config.audio_db_path.open("r", encoding="utf-8") as f_db:
                links = set(line.strip() for line in f_links if line.strip() and line.strip() != "video_id")
                db = set(line.strip() for line in f_db if line.strip() and line.strip() != "video_id")
                missing_audio = links - db

        if config.videos_links_path.exists() and config.videos_db_path.exists():
            with config.videos_links_path.open("r", encoding="utf-8") as f_links, \
                 config.videos_db_path.open("r", encoding="utf-8") as f_db:
                links = set(line.strip() for line in f_links if line.strip() and line.strip() != "video_id")
                db = set(line.strip() for line in f_db if line.strip() and line.strip() != "video_id")
                missing_videos = links - db


        if missing_audio:
            print(f"[Downloadig Audio] Los siguientes audios no están en el DB: {missing_audio}")
            with open("aux.csv", "w", encoding="utf-8") as aux_file:
                for audio_id in missing_audio:
                    aux_file.write(f"{audio_id}\n")
            
            cmd = f'yt-dlp -x --audio-format mp3 -o "{config.audio_folder_path}/%(id)s.%(ext)s" --batch-file aux.csv'
            os.system(cmd)

            os.remove("aux.csv")
            os.remove(config.audio_links_path)
            _create_file(Path("./Links") / f"audio_{account['language']}.csv")

            with config.audio_db_path.open("a", encoding="utf-8") as db_file:
                for audio_id in missing_audio:
                    db_file.write(f"{audio_id}\n")

        if missing_videos:
            print(f"[Downloadig Vidio] Los siguientes videos no están en el DB: {missing_videos}")
            with open("aux.csv", "w", encoding="utf-8") as aux_file:
                for vidio_id in missing_videos:
                    aux_file.write(f"{vidio_id}\n")
            
            cmd = f'yt-dlp --merge-output-format mp4 -f "bv+ba/b" -o "{config.video_folder_path}/%(id)s.%(ext)s" --batch-file aux.csv'
            os.system(cmd)

            os.remove("aux.csv")
            os.remove(config.videos_links_path)
            _create_file(Path("./Links") / f'{account["edition"]["type"]}_{account["edition"]["content"]}.csv')

            with config.videos_db_path.open("a", encoding="utf-8") as db_file:
                for vidio_id in missing_videos:
                    db_file.write(f"{vidio_id}\n")
    
    for account in accounts:
        config = accountConfig(account)
        audio_files = [str(f) for f in config.audio_folder_path.iterdir() if f.is_file()]
        for audio_file in audio_files:
            if audio_file.endswith("mp3"):
                cmd = f"ffmpeg -i {audio_file} {audio_file.replace('.mp3', '.wav')}"
                os.system(cmd)
                os.remove(audio_file)

def audios_to_pickle(accounts, whisper_model=WM, txt_format="segments"):
    # Leer todos los archivos en audio_path y crear una lista

    for account in accounts:
        config = accountConfig(account)

        audio_files = [str(f) for f in config.audio_folder_path.iterdir() if f.is_file()]
        caption_files = [str(f) for f in config.caption_folder_path.iterdir() if f.is_file()]

        id_names = [f.split('/')[-1] for f in audio_files]

        # Verificar que todos los id_names tengan su archivo de caption correspondiente
        for audio_id in id_names:
            caption_file = config.caption_folder_path / audio_id.replace(".wav", ".pickle")
            if str(caption_file) not in caption_files:
                whisper_audio = whisper_timestamped.load_audio(str(config.audio_folder_path / audio_id))
                whisper_results = whisper_timestamped.transcribe(whisper_model, whisper_audio, language=config.language)
                whisper_transcribed_text = whisper_results[txt_format]

                print(f"[Caption] Guardando el archivo '{caption_file}'.")
                with (caption_file).open("wb") as f:
                    pickle.dump(whisper_transcribed_text, f)
        
        # Verificar que todos los archivos de caption tengan su archivo de audio correspondiente
        for caption_file in config.caption_folder_path.iterdir():
            if caption_file.suffix == ".pickle":
                audio_file = config.audio_folder_path / caption_file.stem.replace(".wav", "")  # stem sin extensión
                audio_file = audio_file.with_suffix(".wav")
                if not audio_file.exists():
                    print(f"[Caption] '{caption_file}' no tiene su archivo de audio correspondiente.")
                    os.remove(caption_file)

def resize_video(accounts_config):
    for account in accounts_config:
        config = accountConfig(account)
        videos_files = [str(f) for f in config.video_folder_path.iterdir() if f.is_file()]
        for video_file in videos_files:
            video_file_clip = VideoFileClip(video_file).without_audio()
            resolution = video_file_clip.w / video_file_clip.h
            if resolution != RESOLUTION or str(video_file_clip.w) != '1080':
                print(f"[Vidio] Resizing video '{video_file}'.")
                output_path = str(Path(video_file).with_stem(Path(video_file).stem + "_resized"))
                video_resized = video_file_clip.fx(vfx.resize, (1080, 1920))
                video_resized.write_videofile(output_path, codec="libx264", audio=False)
                os.remove(video_file)
                os.rename(output_path, video_file)


def clean_db(archivo):
    try:
        print(f"[Config] Cargando configuración para '{archivo}'")

        with open(archivo, 'r', encoding='utf-8') as config_file:
            accounts_config = json.load(config_file)
    except Exception as e:
        raise RuntimeError(f"Error leyendo configuración: {e}")

    write_folders(accounts_config)
    download_media(accounts_config)
    audios_to_pickle(accounts_config)
    resize_video(accounts_config)


config_file = "./Config/config.json"
clean_db(config_file)