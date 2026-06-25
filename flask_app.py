from flask import Flask, request, Response, jsonify
import time
import threading
import queue
import json
import os
from pathlib import Path

app = Flask(__name__)

# Папка для хранения облачных конфигов
CONFIGS_ROOT = Path("configs")
CONFIGS_ROOT.mkdir(exist_ok=True)

class MessengerServer:
def __init__(self):
self.clients = {} # {client_id: {'queue': queue, 'username': username, 'last_seen': time}}
self.client_id_counter = 0
self.lock = threading.Lock()
self.messages = [] # храним последние 50 сообщений

def add_client(self, username):
with self.lock:
self.client_id_counter += 1
client_id = self.client_id_counter
self.clients[client_id] = {
'queue': queue.Queue(),
'username': username,
'last_seen': time.time()
}
return client_id

def remove_client(self, client_id):
with self.lock:
return self.clients.pop(client_id, None)

def broadcast(self, message):
self.messages.append(message)
if len(self.messages) > 50:
self.messages.pop(0)

disconnected = []
with self.lock:
clients_copy = dict(self.clients)

for client_id, client_data in clients_copy.items():
try:
client_data['queue'].put_nowait(message)
client_data['last_seen'] = time.time()
except:
disconnected.append(client_id)

with self.lock:
for client_id in disconnected:
self.clients.pop(client_id, None)

def cleanup_old_clients(self):
current_time = time.time()
disconnected = []
with self.lock:
for client_id, client_data in list(self.clients.items()):
if current_time - client_data['last_seen'] > 30:
disconnected.append(client_id)

for client_id in disconnected:
with self.lock:
if client_id in self.clients:
username = self.clients[client_id]['username']
self.clients.pop(client_id)
self.broadcast(f"SYSTEM:{username} покинул сеть (таймаут)")


server = MessengerServer()

# Поток очистки неактивных клиентов
def cleanup_thread():
while True:
time.sleep(10)
server.cleanup_old_clients()


threading.Thread(target=cleanup_thread, daemon=True).start()


@app.route('/')
def index():
return '''
<!DOCTYPE html>
<html>
<head>
<title>Chat Server</title>
<style>
body { font-family: Arial; margin: 20px; background: #111; color: #eee; }
#messages { height: 400px; overflow-y: scroll; border: 1px solid #444; padding: 10px; background: #1a1a1a; }
#input { margin-top: 10px; }
button { padding: 8px 16px; background: #0066ff; border: none; color: white; cursor: pointer; }
</style>
</head>
<body>
<h1>Chat Server Running</h1>
<p>API endpoints:</p>
<ul>
<li>GET /api/history — история сообщений</li>
<li>POST /api/send/&lt;username&gt; — отправить сообщение</li>
<li>GET /api/stream/&lt;username&gt; — SSE поток сообщений</li>
<li>GET /api/configs/&lt;username&gt; — список конфигов</li>
<li>GET/POST/DELETE /api/config/&lt;username&gt;/&lt;config_name&gt; — работа с конфигами</li>
</ul>

<h2>Тестовый чат</h2>
<div id="messages"></div>
<div id="input">
<input type="text" id="message" placeholder="Сообщение..." style="width:300px; padding:8px;">
<button onclick="sendMessage()">Отправить</button>
</div>

<script>
const username = 'web_test_' + Math.random().toString(36).substr(2, 5);
const eventSource = new EventSource(`/api/stream/${username}`);
eventSource.onmessage = function(event) {
const messages = document.getElementById('messages');
messages.innerHTML += '<div>' + event.data + '</div>';
messages.scrollTop = messages.scrollHeight;
};

function sendMessage() {
const msg = document.getElementById('message').value.trim();
if (msg) {
fetch(`/api/send/${username}`, {
method: 'POST',
headers: {'Content-Type': 'application/json'},
body: JSON.stringify({message: msg})
});
document.getElementById('message').value = '';
}
}

document.getElementById('message').addEventListener('keypress', e => {
if (e.key === 'Enter') sendMessage();
});
</script>
</body>
</html>
'''


@app.route('/api/history')
def get_history():
return jsonify(server.messages)


@app.route('/api/send/<username>', methods=['POST'])
def send_message(username):
data = request.get_json(silent=True)
if not data or 'message' not in data:
return jsonify({'error': 'Invalid or missing message'}), 400

message = data['message'].strip()
if not message:
return jsonify({'error': 'Empty message'}), 400

server.broadcast(f"{username}:{message}")
return jsonify({'status': 'ok'})


@app.route('/api/stream/<username>')
def stream_messages(username):
client_id = server.add_client(username)

# Отправляем последние 20 сообщений при подключении
for msg in server.messages[-20:]:
if msg:
yield f"data: {msg}\n\n"

# Системное сообщение о входе
server.broadcast(f"SYSTEM:{username} вошел в сеть")

def generate():
queue_obj = server.clients.get(client_id, {}).get('queue')
if not queue_obj:
return

try:
while True:
try:
message = queue_obj.get(timeout=25)
yield f"data: {message}\n\n"
except queue.Empty:
yield ": heartbeat\n\n" # keep-alive
except GeneratorExit:
left = server.remove_client(client_id)
if left:
server.broadcast(f"SYSTEM:{left['username']} покинул сеть")

return Response(
generate(),
mimetype='text/event-stream',
headers={
'Cache-Control': 'no-cache',
'X-Accel-Buffering': 'no',
'Access-Control-Allow-Origin': '*'
}
)


@app.route('/api/status')
def status():
with server.lock:
return jsonify({
'clients': len(server.clients),
'messages': len(server.messages),
'clients_list': [c['username'] for c in server.clients.values()]
})


# ────────────────────────────────────────────────
# Эндпоинты для облачных конфигов
# ────────────────────────────────────────────────

@app.route('/api/configs/<username>', methods=['GET'])
def list_configs(username):
user_dir = CONFIGS_ROOT / username
if not user_dir.exists():
return jsonify([])

configs = [
f.stem for f in user_dir.iterdir()
if f.is_file() and f.suffix == '.json'
]
return jsonify(configs)


@app.route('/api/config/<username>/<config_name>', methods=['GET'])
def get_config(username, config_name):
path = CONFIGS_ROOT / username / f"{config_name}.json"
if not path.exists():
return jsonify({"error": "Config not found"}), 404

with open(path, 'r', encoding='utf-8') as f:
try:
data = json.load(f)
return jsonify(data)
except json.JSONDecodeError:
return jsonify({"error": "Invalid JSON"}), 400


@app.route('/api/config/<username>/<config_name>', methods=['POST'])
def save_config(username, config_name):
path = CONFIGS_ROOT / username / f"{config_name}.json"
path.parent.mkdir(parents=True, exist_ok=True)

data = request.get_json(silent=True)
if data is None:
return jsonify({"error": "Invalid JSON"}), 400

try:
with open(path, 'w', encoding='utf-8') as f:
json.dump(data, f, indent=2, ensure_ascii=False)
return jsonify({"status": "saved"})
except Exception as e:
return jsonify({"error": str(e)}), 500


@app.route('/api/config/<username>/<config_name>', methods=['DELETE'])
def delete_config(username, config_name):
path = CONFIGS_ROOT / username / f"{config_name}.json"
if path.exists():
path.unlink()
return jsonify({"status": "deleted"})
return jsonify({"error": "Config not found"}), 404


if __name__ == '__main__':
app.run(host='0.0.0.0', port=8000, debug=True)
