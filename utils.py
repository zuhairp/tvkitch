import sys
import time
import math
from typing import io, Callable

import gevent
from gevent.queue import Queue, Full as QueueFullException
from rarfile import RarFile

ProgressCallback = Callable[[int], None]

def copy_stream(src: io.IO[bytes], dst: io.IO[bytes], progress_cb: ProgressCallback):
    total_copied = 0
    chunk_size = 4096

    while True:
        gevent.sleep(0)
        chunk = src.read(chunk_size)
        if not chunk:
            break
        dst.write(chunk) 
        total_copied += len(chunk)

        progress_cb(total_copied)


if __name__ == "__main__":
    rf = RarFile('/media/zuhair/Shuttle/Legion.S02E01.1080p.WEB.H264-DEFLATE/legion.s02e01.1080p.web.h264-deflate.rar')
    info = rf.infolist()[0]
    src = rf.open(info, 'r')
    dst = open('/tmp/legion.mkv', mode='wb')

    progress_queue = Queue(maxsize=1)

    def on_progress(copied):
        try:
            progress_queue.put_nowait(copied)
        except QueueFullException:
            # Swallow the exception and go on
            pass
    
    def printer():
        start_timestamp = time.time()
        loop_timestamp = start_timestamp
        loop_progress = 0
        try:
            while True:
                # Wait for something to come in
                progress = progress_queue.peek(block=True, timeout=1)
                ratio = progress/info.file_size
                percentage = math.ceil(100 * ratio)

                current_timestamp = time.time()
                elapsed = current_timestamp - start_timestamp

                curr_elapsed = current_timestamp - loop_timestamp
                curr_progress = progress - loop_progress

                # If x% is done in y seconds, then (100-x)% will be done in...
                # x(total) = y -> total = y/x
                # (1-x)(total) = (1-x)(y/x)
                eta =  math.ceil((1 - ratio) * (elapsed / ratio))

                avg_thrpt = (progress / elapsed) / (1024 * 1024)
                curr_thrpt = ((curr_progress) / (curr_elapsed)) / (1024 * 1024)

                #output = '[' + '=' * percentage + ' ' * (100-percentage) + ']'
                stat_string = f"{percentage:>2}% ETA:{eta:>3}s Avg: {avg_thrpt:05.02f}MBps Curr: {curr_thrpt:05.02f}MBps"
                stat_string = f"{stat_string:<100}\r"
                sys.stdout.write(stat_string)
                sys.stdout.flush()

                if (percentage == 100):
                    return

                loop_timestamp = current_timestamp
                loop_progress = progress
                # Sleep for a bit
                gevent.sleep(0.032)

                # Remove the item so we can get a fresh new one on the next iteration
                progress_queue.get_nowait()

        except Exception as e:
            print(e)
            return

    printer_event = gevent.spawn(printer)
    copy_stream(src, dst, on_progress)
    printer_event.join()
    print()