
from dataclasses import dataclass
from pathlib import Path
from random import choice, randint, sample
from typing import List
from datetime import datetime
from moviepy.editor import *

import gc
import csv
import json
import pickle
import functools
import threading

VIDEO_FPS = 60
SAMPLE_CLIPS_COUNT = 20
EXPECTED_VIDEO_WIDTH = 1080

USERNAMES = ["KnoxReadsTM"]

PLATAFORMAS = ["TikTok", "YouTube"]
TEXT_COLOR_LIST = ["#a4c7c0", "#beb6b1"]

txt_file_type = "txt"
wav_file_type = "wav"
video_file_type = "mp4"
pickle_file_type = "pickle"

@dataclass
class VideoConfig:
    """Configuración para la generación de videos"""
    account_id: str
    language: str
    audio_type: str
    clip_type: str

@dataclass
class VideoClipCache:
    """Cache para clips de video reutilizables"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._cache = {}
        return cls._instance
    
    def get_clip(self, path: str):
        if path not in self._cache:
            self._cache[path] = VideoFileClip(str(path)).without_audio()
        return self._cache[path]
    
    def clear(self):
        for clip in self._cache.values():
            clip.close()
        self._cache.clear()


@dataclass
class ShortVideoGenerator:
    """Generador de videos cortos para plataformas sociales"""
    
    def __init__(self, config: VideoConfig):
        """Inicializa el generador con la configuración proporcionada"""
        self.config = config
        self.clips_folder_path = Path(f"Media/Videos/{config.clip_type}/")
        self.script_pickle_folder_path = Path(f"Media/Scripts/pickle/{config.language}/")
        self.audio_folder_path = Path(f"Media/Audios/{config.audio_type}/{config.language}/")
        self.db_file_path = Path(f"DB/{config.account_id}.csv")
        self.temp_files = []  # Para tracking de archivos temporales
        self._used_scripts_cache = None  # Cache para scripts usados
        self.clip_cache = VideoClipCache()

    def _get_used_scripts(self) -> set:
        """Cache de scripts usados para evitar leer el CSV múltiples veces"""
        # Usar cache de instancia en lugar de lru_cache
        if hasattr(self, '_used_scripts_cache') and self._used_scripts_cache is not None:
            return self._used_scripts_cache
            
        if not self.db_file_path.exists():
            self._used_scripts_cache = set()
            return self._used_scripts_cache
        
        used_scripts = set()
        try:
            with open(self.db_file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader, None)  # Saltar cabecera si existe
                for row in csv_reader:
                    if row and len(row) > 0:
                        used_scripts.add(row[0])  # Corregido índice
        except Exception as e:
            print(f"Error leyendo DB: {e}")
        
        self._used_scripts_cache = used_scripts
        return used_scripts

    def get_unique_script(self) -> str:
        """Obtiene un script único que no se haya usado antes - Optimizado"""
        script_files = list(self.script_pickle_folder_path.glob(f"*.{pickle_file_type}"))
        if not script_files:
            raise FileNotFoundError(f"No se encontraron scripts en {self.script_pickle_folder_path}")

        used_scripts = self._get_used_scripts()
        available_scripts = [script.stem for script in script_files if script.stem not in used_scripts]

        if not available_scripts:
            print("Todos los scripts han sido utilizados. Se reutilizará uno aleatorio.")
            available_scripts = [script.stem for script in script_files]

        self.script_file_name = choice(available_scripts)
        print(f"[Script] {self.script_file_name}")

        self.script_file_path = f"{self.script_pickle_folder_path}/{self.script_file_name}.{pickle_file_type}"
        self.audio_file_path = f"{self.audio_folder_path}/{self.script_file_name}.{wav_file_type}"

        # Validar existencia de los archivos de script y audio
        if not Path(self.script_file_path).exists():
            raise FileNotFoundError(f"No se encontró el archivo de script: {self.script_file_path}")
        if not Path(self.audio_file_path).exists():
            raise FileNotFoundError(f"No se encontró el archivo de audio: {self.audio_file_path}")
    
    def get_caption_list(self) -> List[TextClip]:
        """Genera la lista de subtítulos - Optimizado con batch processing"""
        try:
            with open(self.script_file_path, 'rb') as fp:
                whisper_transcribed_text = pickle.load(fp)
        except Exception as e:
            raise FileNotFoundError(f"Error al cargar el script: {e}")

        color_choice = choice(TEXT_COLOR_LIST)
        caption_list = []

        # Procesar en batch para mejor rendimiento
        for segment in whisper_transcribed_text:
            segment_captions = []
            
            for word in segment["words"]:
                text = word.get("text", "").strip()
                if not text:
                    continue
                    
                textColor = color_choice if len(text) >= 5 and randint(1,3) >= 2 else "white"
                
                try:
                    caption = TextClip(text,
                        fontsize=80,
                        color=textColor,
                        font="Font/KOMIKAX_.ttf",
                        stroke_width=4,
                        stroke_color="black",
                        size=(820, None),
                        method='caption',
                        align="center",
                    )
                    
                    start_time = word.get("start", 0)
                    end_time = word.get("end", start_time + 0.5)  # Fallback duration
                    
                    caption = caption.set_start(start_time).set_end(end_time)
                    caption = caption.set_position(("center", "center"), relative=True)
                    segment_captions.append(caption)
                    
                except Exception as e:
                    print(f"Error al crear subtítulo para '{text}': {e}")
                    continue
            
            caption_list.extend(segment_captions)

        self.caption_list = caption_list
        print(f"[Caption] {len(caption_list)} subtítulos generados")
    
    def load_audio(self) -> AudioFileClip:
        """Carga Audio - Optimizado con batch processing"""
        self.audio_file = AudioFileClip(self.audio_file_path)
        print(f"[Audio] Cargando audio {self.audio_file.duration:.2f}s")
    
    def get_video_clip_list(self) -> List[VideoFileClip]:
        """Selecciona clips de video - Optimizado con validación previa y cache"""

        try:
            # Obtener archivos válidos de una vez
            clip_filenames = [f for f in self.clips_folder_path.glob("*") 
                            if f.is_file() and f.stat().st_size > 0]
            
            if not clip_filenames:
                raise FileNotFoundError(f"No se encontraron clips válidos en {self.clips_folder_path}")

            # Muestra más pequeña para reducir tiempo de procesamiento
            sample_size = min(SAMPLE_CLIPS_COUNT, len(clip_filenames))
            selected_files = sample(clip_filenames, sample_size)

            duration = 0
            clip_list = []
            target_duration = self.audio_file.duration

            for clip_file in selected_files:
                if duration >= target_duration:
                    break

                try:
                    # Usar cache para clips
                    clip = self.clip_cache.get_clip(clip_file)
                    
                    # Validación rápida
                    if not hasattr(clip, 'size') or clip.size[0] != EXPECTED_VIDEO_WIDTH:
                        continue

                    clip_duration = clip.duration
                    if clip_duration <= 0:
                        continue

                    clip_list.append(clip)
                    duration += clip_duration
                    
                    print(f" --> {round(duration, 2)}s - {clip_file.name} - {clip.size} - {round(clip_duration, 2)}s")

                except Exception as e:
                    print(f"Error al procesar clip '{clip_file}': {e}")
                    continue

            if duration < target_duration:
                print(f"Advertencia: Duración de clips ({duration:.2f}s) menor que audio ({target_duration:.2f}s)")
                # En lugar de fallar, repetir clips si es necesario
                if clip_list:
                    while duration < target_duration:
                        clip_to_repeat = choice(clip_list)
                        clip_list.append(clip_to_repeat)
                        duration += clip_to_repeat.duration
                        if len(clip_list) > 50:  # Evitar bucle infinito
                            break

            return clip_list

        except Exception as e:
            raise RuntimeError(f"Error al cargar los clips de video: {e}")

    def get_video_composition(self, video_clip_list: List[VideoFileClip], plataforma: str, username: str) -> None:
        """Compone el video final - Optimizado para mejor rendimiento"""
        if not video_clip_list:
            raise ValueError("Lista de clips de video vacía")

        try:
            # Optimizar concatenación
            concatenation = concatenate_videoclips(video_clip_list, method="compose")
            
            # Crear composición final
            composite = CompositeVideoClip([concatenation] + self.caption_list)
            composite = composite.set_duration(self.audio_file.duration)
            composite.audio = self.audio_file

            output_file_path = f"{self.script_file_name}.{video_file_type}"
            print(f"[Video] Escribiendo video '{output_file_path}'")
            
            # Configuración optimizada para velocidad
            composite.write_videofile(
                output_file_path, 
                codec="libx264", 
                audio_codec="aac", 
                fps=VIDEO_FPS, 
                threads=4, 
                preset="ultrafast",
                remove_temp=True
            ) 
                # logger=None,
            
            # Liberar memoria
            composite.close()
            concatenation.close()
            gc.collect()
            os.rename(output_file_path, f"Media/Videos/output/{username}_{plataforma}_{output_file_path}")

        except Exception as e:
            raise RuntimeError(f"Error creando video para {plataforma}: {e}")
        
    def update_db(self) -> None:
        """Actualiza la base de datos - Optimizado con mejor manejo de errores"""
        try:
            # Crear directorio si no existe
            self.db_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Escribir de forma atómica
            with open(self.db_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([self.script_file_name])
            
            print(f"[DB] Información añadida a '{self.db_file_path}'")
            
            # Limpiar cache después de actualizar
            self._used_scripts_cache = None
            
        except Exception as e:
            print(f"Error al actualizar la base de datos: {e}")

    def cleanup_temp_files(self) -> None:
        """Limpia archivos temporales"""        
        # Limpiar cache de clips
        self.clip_cache.clear()
        
        # Cerrar clips de audio
        if hasattr(self, 'audio_clip'):
            self.audio_clip.close()

    def __del__(self):
        """Destructor para limpieza automática"""
        self.cleanup_temp_files()


@functools.lru_cache(maxsize=10)
def load_config(account_name: str, config_type: str) -> VideoConfig:
    """Carga configuración con cache para evitar múltiples lecturas"""
    config_file_path = Path(f"./Config/{account_name}.json")

    print(f"[Config] Cargando configuración para '{account_name}'")
    if not config_file_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración para la cuenta: {account_name}")

    try:
        with open(config_file_path, 'r', encoding='utf-8') as config_file:
            account_config = json.load(config_file)
    except Exception as e:
        raise RuntimeError(f"Error leyendo configuración: {e}")

    if config_type == "video":
        config = VideoConfig(
            account_id=account_config.get("name"),
            language=account_config.get("language"),
            audio_type=account_config.get("audio_type"),
            clip_type=account_config.get("clip_type")
        )
    elif config_type == "audio":
        config = AudioConfig(
            account_id=account_config.get("name"),
            language=account_config.get("language"),
            audio_type=account_config.get("audio_type"),
            gemini_api=account_config.get("gemini_api")
        )
    else:
        raise ValueError(f"Tipo de configuración no soportado: {config_type}")

    return config

def create_short():
    start_time = datetime.now()

    for username in USERNAMES:
        config = load_config(username, "video")
        generator = ShortVideoGenerator(config)

        generator.get_unique_script()
        generator.get_caption_list()

        for plataforma in PLATAFORMAS:
            print(f"\n[{plataforma}] {generator.script_file_name}")
            generator.load_audio()
            video_clip_list = generator.get_video_clip_list()
            generator.get_video_composition(video_clip_list, plataforma, username)

        generator.update_db()
        generator.cleanup_temp_files()

    end_time = datetime.now()
    print(f"\nTiempo de ejecución: {end_time - start_time}")


@dataclass
class AudioConfig:
    """Configuración para la generación de audios"""
    account_id: str
    language: str
    audio_type: str
    gemini_api: str

@dataclass
class ShortAudioGenerator:
    """Generador de audios cortos para plataformas sociales"""

    def __init__(self, config: AudioConfig):
        """Inicializa el generador con la configuración proporcionada"""
        self.config = config
        self.script_pickle_folder_path = Path(f"Media/Scripts/pickle/{config.language}/")
        self.script_txt_folder_path = Path(f"Media/Scripts/txt/{config.language}/")
        self.audio_folder_path = Path(f"Media/Audios/{config.audio_type}/{config.language}/")
        self.client = genai.Client(api_key=config.gemini_api)
        self._used_scripts_cache = None  # Cache para scripts usados

    def get_unique_script(self) -> str:
        """Obtiene un script único que no se haya usado antes - Optimizado"""
        script_files = list(self.script_txt_folder_path.glob("*.txt"))
        if not script_files:
            raise FileNotFoundError(f"No se encontraron scripts en {self.script_txt_folder_path}")

        used_scripts = list(self.audio_folder_path.glob(f"*.{wav_file_type}"))
        available_scripts = [script.stem for script in script_files if script.stem not in used_scripts]

        if not available_scripts:
            print("Todos los scripts han sido utilizados. Se reutilizará uno aleatorio.")
            available_scripts = [script.stem for script in script_files]

        self.script_file_name = choice(available_scripts)
        print(f"[Script] Script seleccionado: {self.script_file_name}")

    def txt_to_audio(self) -> str:
        """Convierte un archivo de texto a audio - Optimizado con manejo de memoria"""
        voice_name = choice(VOICES)
        audio_output_filename = f"{self.script_file_name}.{wav_file_type}"

        script_file_path = f"{self.script_txt_folder_path}/{self.script_file_name}.{txt_file_type}"
        
        # Leer archivo de forma más eficiente
        try:
            with open(script_file_path, 'r', encoding='utf-8') as f:
                script_txt = f.read().strip()
        except Exception as e:
            raise FileNotFoundError(f"Error leyendo script: {e}")
        
        try:
            response = self.client.models.generate_content(
                model="gemini-2.5-flash-preview-tts",
                contents=script_txt,
                config=types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(
                            prebuilt_voice_config=types.PrebuiltVoiceConfig(
                            voice_name=voice_name,
                            )
                        )
                    ),
                )
            )
            
            data = response.candidates[0].content.parts[0].inline_data.data
            self.wave_file(audio_output_filename, data)
            print(f"[Audio] Generado y guardado como: {self.script_file_name}")

            os.rename(
                audio_output_filename, 
                f"{self.audio_folder_path}/{audio_output_filename}"
            )
            os.remove(script_file_path)  # Eliminar archivo de texto original
            
        except Exception as e:
            raise RuntimeError(f"Error generando audio: {e}")
    
    def wave_file(self, filename, pcm, channels=1, rate=24000, sample_width=2):
        """Optimizado para escribir archivos wave más eficientemente"""
        self.temp_files.append(filename)  # Track para cleanup
        with wave.open(filename, "wb") as wf:
            wf.setnchannels(channels)
            wf.setsampwidth(sample_width)
            wf.setframerate(rate)
            wf.writeframes(pcm)

from google import genai
from google.genai import types
import wave
gemini_api_key = "key"  # Reemplaza con tu clave de API de Gemini
VOICES = ["Kore", "Charon", "Fenrir", "Leda", "Rasalgethi"]
def create_audio():
    """Función para crear audio, si es necesario"""
    for username in USERNAMES:
        config = load_config(username, "audio")
        generator = ShortAudioGenerator(config)

        for audios_count in range(1, 2):
            print(f"\n[{username}] Creando audio {audios_count}")
            generator.get_unique_script()
            generator.txt_to_audio()
    

if __name__ == "__main__":
    create_audio()
