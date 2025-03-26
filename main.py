import asyncio
import os
import subprocess
import shutil
from pathlib import Path
from collections import deque
from time import time
import yt_dlp
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from urllib.parse import urlparse

# List of allowed domains
ALLOWED_DOMAINS = ['youtube.com', 'youtu.be', 'tiktok.com', 'instagram.com', 'x.com', 'twitter.com']

# Base directory for temporary downloads
BASE_DOWNLOAD_DIR = Path("downloads")

# Ensure base download directory exists
BASE_DOWNLOAD_DIR.mkdir(exist_ok=True)

# Rate limiting configuration
MAX_REQUESTS = 10  # Maximum concurrent requests
TIME_WINDOW = 10  # Time window in seconds
request_timestamps = deque()  # Tracks timestamps of requests
pending_queue = asyncio.Queue()  # Queue for pending requests
active_tasks = set()  # Tracks currently active tasks

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command and send instructions."""
    await update.message.reply_text("Привет! Отправь мне, пожалуйста, ссылку на свое видео.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages and process URLs from allowed platforms."""
    message = update.message
    url = message.text

    # Basic URL validation
    if not url.startswith("http"):
        return

    # Check if the URL is from an allowed domain
    domain = urlparse(url).netloc
    if not any(allowed in domain for allowed in ALLOWED_DOMAINS):
        return

    # Send a temporary "Processing video..." message
    status_message = await context.bot.send_message(
        chat_id=message.chat_id,
        text="Обрабатываю твой запрос...",
        reply_to_message_id=message.message_id
    )

    # Create a unique directory for this specific request
    request_dir = BASE_DOWNLOAD_DIR / f"{message.chat_id}_{message.message_id}"
    request_dir.mkdir(exist_ok=True)

    # Add request to the queue
    await queue_request(
        url, message.chat_id, message.message_id, status_message.message_id, context, request_dir
    )

async def queue_request(url, chat_id, message_id, status_message_id, context, request_dir):
    """Handle rate limiting and queuing of video processing requests."""
    current_time = time()

    # Clean up old timestamps outside the time window
    while request_timestamps and current_time - request_timestamps[0] > TIME_WINDOW:
        request_timestamps.popleft()

    # Check if we can process immediately or need to queue
    if len(request_timestamps) < MAX_REQUESTS:
        request_timestamps.append(current_time)
        task = asyncio.create_task(
            process_video(url, chat_id, message_id, status_message_id, context, request_dir)
        )
        active_tasks.add(task)
        task.add_done_callback(lambda t: active_tasks.remove(t))
    else:
        # Queue the request
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message_id,
            text="Ваш запрос в очереди, ожидайте..."
        )
        await pending_queue.put((url, chat_id, message_id, status_message_id, context, request_dir))
        asyncio.create_task(process_queue())

async def process_queue():
    """Process requests from the queue when slots become available."""
    while not pending_queue.empty() and len(request_timestamps) < MAX_REQUESTS:
        current_time = time()
        while request_timestamps and current_time - request_timestamps[0] > TIME_WINDOW:
            request_timestamps.popleft()

        if len(request_timestamps) < MAX_REQUESTS:
            url, chat_id, message_id, status_message_id, context, request_dir = await pending_queue.get()
            request_timestamps.append(current_time)
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text="Обрабатываю твой запрос..."
            )
            task = asyncio.create_task(
                process_video(url, chat_id, message_id, status_message_id, context, request_dir)
            )
            active_tasks.add(task)
            task.add_done_callback(lambda t: active_tasks.remove(t))
        else:
            await asyncio.sleep(1)  # Wait a bit before checking again

async def process_video(url, chat_id, message_id, status_message_id, context, request_dir, retries=3):
    """Download, compress (if needed), and send the video with retry logic."""
    loop = asyncio.get_event_loop()
    video_file = None
    
    try:
        for attempt in range(retries):
            try:
                # Step 1: Get video metadata without downloading
                ydl_opts = {
                    'quiet': True,
                    'no_warnings': True,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=False))
                    width = info.get("width")
                    height = info.get("height")
                    if width and height:
                        aspect_ratio = f"{width}:{height}"
                    else:
                        aspect_ratio = "16:9"

                # Step 2: Download video with yt-dlp
                video_file = request_dir / 'video.mp4'
                ydl_opts = {
                    'format': 'best',
                    'outtmpl': str(video_file),
                    'merge_output_format': 'mp4',
                    'postprocessors': [{
                        'key': 'FFmpegVideoConvertor',
                        'preferedformat': 'mp4',
                    }],
                    'quiet': True,
                    'no_warnings': True,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    await loop.run_in_executor(None, lambda: ydl.download([url]))

                # Verify the file exists and has content
                if not video_file.exists() or video_file.stat().st_size == 0:
                    continue

                # Step 3: Check file size and compress if necessary
                file_size = video_file.stat().st_size
                if file_size < 50 * 1024 * 1024:  # Under 50MB
                    with video_file.open('rb') as f:
                        await context.bot.send_video(
                            chat_id,
                            f,
                            supports_streaming=True,
                            width=width,
                            height=height,
                            reply_to_message_id=message_id
                        )
                else:
                    # File exceeds 50MB, compress it
                    result = await loop.run_in_executor(
                        None,
                        lambda: subprocess.run(
                            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                             '-of', 'default=noprint_wrappers=1:nokey=1', str(video_file)],
                            stdout=subprocess.PIPE, text=True
                        )
                    )
                    duration = float(result.stdout.strip()) if result.stdout.strip() else 0
                    if duration <= 0:
                        continue

                    target_total_bitrate = (40 * 1024 * 1024 * 8) / duration
                    video_bitrate = target_total_bitrate - 128000

                    if video_bitrate < 100000:
                        continue

                    output_file = request_dir / 'compressed_video.mp4'
                    ffmpeg_cmd = [
                        'ffmpeg',
                        '-i', str(video_file),
                        '-c:v', 'libx264',
                        '-profile:v', 'baseline',
                        '-b:v', f'{int(video_bitrate)}',
                        '-vf', 'scale=iw:ih',
                        '-aspect', aspect_ratio,
                        '-pix_fmt', 'yuv420p',
                        '-c:a', 'aac',
                        '-b:a', '128k',
                        '-movflags', '+faststart',
                        '-preset', 'ultrafast',
                        '-y',
                        str(output_file)
                    ]
                    await loop.run_in_executor(None, lambda: subprocess.run(ffmpeg_cmd, check=True))

                    if not output_file.exists():
                        continue

                    with output_file.open('rb') as f:
                        await context.bot.send_video(
                            chat_id,
                            f,
                            supports_streaming=True,
                            width=width,
                            height=height,
                            reply_to_message_id=message_id
                        )

                # Delete status message after success
                await context.bot.delete_message(chat_id=chat_id, message_id=status_message_id)
                break

            except Exception as e:
                if attempt == retries - 1:
                    await context.bot.delete_message(chat_id=chat_id, message_id=status_message_id)
                continue

    finally:
        # Clean up request-specific directory after processing
        if request_dir.exists():
            shutil.rmtree(request_dir, ignore_errors=True)
        # Trigger queue processing after a task completes
        asyncio.create_task(process_queue())

def main():
    """Set up and run the Telegram bot."""
    application = Application.builder().token("***").build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()