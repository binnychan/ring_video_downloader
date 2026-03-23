# ring_video_downloader
Using python-ring-doorbell to download Ring video

docker volume create ring-config

docker run -d --name ring-fetcher -v ring-config:/app -v /hostpath/ring_video:/tmp/ring_videos --restart always python:3 \
bash -c "pip install --upgrade pip && pip install git+https://github.com/python-ring-doorbell/python-ring-doorbell.git schedule && python -u /app/ring_vd.py"