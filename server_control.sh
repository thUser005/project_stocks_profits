#!/bin/bash

APP_NAME="stock_profits.py"
LOG_FILE="stock_profits.log"
PID_FILE="server.pid"

start() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "⚠️  $APP_NAME is already running with PID $(cat $PID_FILE)"
        exit 1
    fi

    echo "🚀 Starting $APP_NAME ..."
    nohup python3 "$APP_NAME" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "✅ Started $APP_NAME with PID $(cat $PID_FILE)"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "⚠️  No PID file found. Is $APP_NAME running?"
        exit 1
    fi

    PID=$(cat "$PID_FILE")
    if kill -0 $PID 2>/dev/null; then
        echo "🛑 Stopping $APP_NAME (PID $PID)..."
        kill $PID
        rm -f "$PID_FILE"
        echo "✅ Stopped $APP_NAME"
    else
        echo "⚠️  Process not found. Removing stale PID file."
        rm -f "$PID_FILE"
    fi
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "✅ $APP_NAME is running (PID $PID)"
            exit 0
        else
            echo "⚠️  PID file exists but process not found"
            exit 1
        fi
    else
        echo "❌ $APP_NAME is not running"
    fi
}

restart() {
    echo "🔁 Restarting $APP_NAME..."
    stop
    sleep 2
    start
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
