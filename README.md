# short-maker
makes short videos 9:16 format

## Download canvas or audio
- canva : yt-dlp --merge-output-format mp4 -f "bv+ba/b" -o "output/%(id)s.%(ext)s" --batch-file <FILE>
- audio : yt-dlp -x --audio-format mp3 -o "output/%(id)s.%(ext)s" --batch-file <FILE>