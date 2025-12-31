import multiprocessing
import os

CPU = multiprocessing.cpu_count()

workers = (CPU * 2) + 1
threads = int(os.getenv("GUNICORN_THREADS", 4))

worker_class = "gthread"
bind = "0.0.0.0:8000"

timeout = 120
keepalive = 5
