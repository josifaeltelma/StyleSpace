#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stylespace - servidor da lousa compartilhada
============================================

Como usar
---------
1. Deixe este arquivo na MESMA PASTA do index.html.
2. No terminal, rode:

       python servidor.py            (ou: python servidor.py 9000   para usar outra porta)

3. Abra no navegador um dos enderecos que aparecerem no terminal, por exemplo
   http://192.168.0.12:8765 . Quem estiver na mesma rede Wi-Fi e abrir esse
   endereco desenha na MESMA lousa, ao vivo (como o Stylespace de verdade:
   rede local, sockets e Python).

No terminal voce tambem pode digitar:
    limpar   -> apaga a lousa de todo mundo
    quem     -> mostra quem esta conectado

So usa a biblioteca padrao do Python 3.8 ou mais novo. Nada para instalar.
"""
import asyncio
import base64
import gzip
import hashlib
import json
import os
import random
import re
import socket
import struct
import sys
import threading
import time

try:  # evita erro de acentuacao em consoles antigos do Windows
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PORT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 8765
HERE = os.path.dirname(os.path.abspath(__file__))
INDEX = os.path.join(HERE, "index.html")
GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# ---- limites (protegem o servidor de abuso e de lousas gigantes) ----
MAX_CLIENTS = 150
MAX_STROKES = 1500
MAX_TOTAL_POINTS = 300000
MAX_POINTS_PER_STROKE = 3000
MAX_POINTS_PER_MSG = 64
MAX_FRAME = 1 << 20          # 1 MB por mensagem recebida
MAX_BUFFER = 4 << 20         # cliente que acumula mais que isso e desconectado
IDLE_TIMEOUT = 60            # o site manda um "ping" a cada poucos segundos

NAMES = ["Tucano", "Capivara", "Onça", "Arara", "Lobo-guará", "Sabiá", "Tamanduá", "Boto",
         "Mico-leão", "Jaguatirica", "Quero-quero", "Preguiça", "Tatu", "Gambá",
         "Beija-flor", "Jabuti", "Seriema", "Tapir", "Carcará", "Bem-te-vi"]
COLORS = ["#ff6a2c", "#8fd6ff", "#a8e6a3", "#ffd479", "#d6a8ff", "#ff8fb1", "#6ee7d8", "#f3ede4"]
HEX = re.compile(r"^#[0-9a-fA-F]{6}$")
SID = re.compile(r"^[A-Za-z0-9_-]{1,24}$")


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# ======================================================================
#  WebSocket (RFC 6455) - o minimo necessario, sem dependencias
# ======================================================================
class ProtocolError(Exception):
    pass


def ws_frame(payload, opcode=1):
    n = len(payload)
    if n < 126:
        head = bytes([0x80 | opcode, n])
    elif n < 65536:
        head = bytes([0x80 | opcode, 126]) + struct.pack(">H", n)
    else:
        head = bytes([0x80 | opcode, 127]) + struct.pack(">Q", n)
    return head + payload


def unmask(data, mask):
    n = len(data)
    if n == 0:
        return b""
    m = (mask * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(m, "big")).to_bytes(n, "big")


class WSReader:
    """Le mensagens WebSocket (com suporte a mensagens fragmentadas)."""

    def __init__(self, reader):
        self.r = reader
        self.buf = bytearray()
        self.op = None

    async def read(self):
        while True:
            h = await self.r.readexactly(2)
            fin, rsv, op = h[0] & 0x80, h[0] & 0x70, h[0] & 0x0F
            masked, n = h[1] & 0x80, h[1] & 0x7F
            if rsv or not masked:
                raise ProtocolError("quadro invalido")
            if n == 126:
                n = struct.unpack(">H", await self.r.readexactly(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", await self.r.readexactly(8))[0]
            if n > MAX_FRAME or len(self.buf) + n > MAX_FRAME:
                raise ProtocolError("mensagem grande demais")
            mask = await self.r.readexactly(4)
            data = unmask(await self.r.readexactly(n), mask)
            if op >= 8:                       # controle: close / ping / pong
                if not fin or n > 125:
                    raise ProtocolError("controle invalido")
                return op, data
            if op == 0:
                if self.op is None:
                    raise ProtocolError("continuacao sem inicio")
            else:
                if self.op is not None:
                    raise ProtocolError("inicio no meio de outra mensagem")
                self.op = op
            self.buf += data
            if fin:
                out = (self.op, bytes(self.buf))
                self.buf = bytearray()
                self.op = None
                return out


# ======================================================================
#  A lousa compartilhada
# ======================================================================
def num01(v):
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v != v:
        raise ValueError("numero invalido")
    return round(min(1.0, max(0.0, float(v))), 4)


def pt(p):
    if not isinstance(p, list) or len(p) != 2:
        raise ValueError("ponto invalido")
    return [num01(p[0]), num01(p[1])]


class Client:
    def __init__(self, cid, name, color, writer):
        self.id = cid
        self.name = name
        self.color = color
        self.writer = writer
        self.x = None
        self.y = None
        self.last_cur = 0.0
        self.tokens = 300.0
        self.t_tokens = time.monotonic()
        self.closed = False

    def send_raw(self, frame):
        if self.closed:
            return
        tr = self.writer.transport
        if tr.is_closing():
            self.closed = True
            return
        if tr.get_write_buffer_size() > MAX_BUFFER:   # cliente lento demais
            self.closed = True
            tr.abort()
            return
        self.writer.write(frame)

    def send(self, obj):
        self.send_raw(ws_frame(dumps(obj)))


def dumps(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


class Hub:
    def __init__(self):
        self.clients = {}
        self.strokes = {}      # id -> traco (dicionario mantem a ordem de criacao)
        self.seq = 0
        self.points = 0
        self.next_id = 1

    # ---------- presenca ----------
    def add(self, writer):
        cid = "u%d" % self.next_id
        self.next_id += 1
        used_names = set(c.name for c in self.clients.values())
        used_colors = set(c.color for c in self.clients.values())
        pool = [n for n in NAMES if n not in used_names]
        random.shuffle(pool)
        if pool:
            name = pool[0]
        else:
            k = 2
            while True:
                name = "%s %d" % (random.choice(NAMES), k)
                if name not in used_names:
                    break
                k += 1
        free = [c for c in COLORS if c not in used_colors]
        color = free[0] if free else random.choice(COLORS)
        c = Client(cid, name, color, writer)
        self.clients[cid] = c
        return c

    def users(self):
        return [{"id": c.id, "name": c.name, "color": c.color, "x": c.x, "y": c.y}
                for c in self.clients.values()]

    def welcome(self, c):
        c.send({"t": "welcome", "id": c.id, "name": c.name, "color": c.color,
                "users": self.users(), "strokes": [self.pub(s) for s in self.strokes.values()]})
        self.broadcast({"t": "users", "users": self.users()}, skip=c)
        log("%s entrou (%d conectado%s)" % (c.name, len(self.clients), "" if len(self.clients) == 1 else "s"))

    def remove(self, c):
        if self.clients.pop(c.id, None) is None:
            return
        for s in self.strokes.values():
            if s["u"] == c.id:
                s["open"] = False
        self.broadcast({"t": "users", "users": self.users()})
        log("%s saiu (%d conectado%s)" % (c.name, len(self.clients), "" if len(self.clients) == 1 else "s"))

    # ---------- envio ----------
    def broadcast(self, obj, skip=None):
        frame = ws_frame(dumps(obj))
        for c in list(self.clients.values()):
            if c is not skip:
                c.send_raw(frame)

    @staticmethod
    def pub(s):
        return {"id": s["id"], "u": s["u"], "c": s["c"], "w": s["w"], "e": s["e"],
                "p": s["p"], "seq": s["seq"]}

    # ---------- tracos ----------
    def _add_stroke(self, c, msg, pts, is_open):
        sid, col = msg.get("id"), msg.get("c")
        if not isinstance(sid, str) or not SID.match(sid) or sid in self.strokes:
            return None
        if not isinstance(col, str) or not HEX.match(col):
            return None
        w = msg.get("w")
        if isinstance(w, bool) or not isinstance(w, (int, float)) or w != w:
            return None
        self.seq += 1
        s = {"id": sid, "u": c.id, "c": col.lower(), "w": round(min(40.0, max(0.5, float(w))), 2),
             "e": bool(msg.get("e")), "p": pts, "seq": self.seq, "open": is_open}
        self.strokes[sid] = s
        self.points += len(pts)
        c.send({"t": "ack", "id": sid, "seq": s["seq"]})
        return s

    def _trim(self):
        gone = []
        while self.strokes and (len(self.strokes) > MAX_STROKES or self.points > MAX_TOTAL_POINTS):
            sid = next(iter(self.strokes))
            self.points -= len(self.strokes.pop(sid)["p"])
            gone.append(sid)
        if gone:
            self.broadcast({"t": "del", "ids": gone})

    def clear_all(self):
        self.strokes.clear()
        self.points = 0
        self.broadcast({"t": "clear"})

    def on_text(self, c, data):
        now = time.monotonic()                     # limite de mensagens por segundo
        c.tokens = min(300.0, c.tokens + (now - c.t_tokens) * 150)
        c.t_tokens = now
        if c.tokens < 1:
            return
        c.tokens -= 1
        try:
            msg = json.loads(data.decode("utf-8"))
            if not isinstance(msg, dict):
                return
            t = msg.get("t")
            if t == "s+":
                raw = msg.get("p")
                if not isinstance(raw, list) or not raw:
                    return
                pts = [pt(q) for q in raw[:MAX_POINTS_PER_MSG]]
                s = self._add_stroke(c, msg, pts, True)
                if s:
                    self.broadcast({"t": "s+", **self.pub(s)}, skip=c)
                    self._trim()
            elif t == "stroke":
                raw = msg.get("p")
                if not isinstance(raw, list) or not raw:
                    return
                pts = [pt(q) for q in raw[:MAX_POINTS_PER_STROKE]]
                s = self._add_stroke(c, msg, pts, False)
                if s:
                    self.broadcast({"t": "stroke", **self.pub(s)}, skip=c)
                    self._trim()
            elif t == "s.":
                s = self.strokes.get(msg.get("id"))
                raw = msg.get("p")
                if not s or s["u"] != c.id or not s["open"] or not isinstance(raw, list):
                    return
                room = MAX_POINTS_PER_STROKE - len(s["p"])
                pts = [pt(q) for q in raw[:min(MAX_POINTS_PER_MSG, room)]]
                if pts:
                    s["p"].extend(pts)
                    self.points += len(pts)
                    self.broadcast({"t": "s.", "id": s["id"], "p": pts}, skip=c)
                    self._trim()
            elif t == "s-":
                s = self.strokes.get(msg.get("id"))
                if s and s["u"] == c.id and s["open"]:
                    s["open"] = False
                    self.broadcast({"t": "s-", "id": s["id"]}, skip=c)
            elif t == "del":
                s = self.strokes.get(msg.get("id"))
                if s and s["u"] == c.id:
                    del self.strokes[s["id"]]
                    self.points -= len(s["p"])
                    self.broadcast({"t": "del", "ids": [s["id"]]}, skip=c)
            elif t == "clr":                       # "Limpar" apaga so os tracos de quem clicou
                ids = [sid for sid, s in self.strokes.items() if s["u"] == c.id]
                for sid in ids:
                    self.points -= len(self.strokes.pop(sid)["p"])
                if ids:
                    self.broadcast({"t": "del", "ids": ids}, skip=c)
            elif t == "cur":
                if msg.get("x") is None or msg.get("y") is None:
                    c.x = c.y = None               # "saiu da lousa": nunca e segurado pelo limite
                else:
                    if now - c.last_cur < 0.02:
                        return
                    c.last_cur = now
                    c.x, c.y = num01(msg.get("x")), num01(msg.get("y"))
                self.broadcast({"t": "cur", "u": c.id, "x": c.x, "y": c.y}, skip=c)
            elif t == "ping":
                n = msg.get("n")
                c.send({"t": "pong", "n": n if isinstance(n, (int, float)) and not isinstance(n, bool) else 0})
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeDecodeError):
            return                                 # mensagem malformada: ignora


hub = Hub()


# ======================================================================
#  HTTP: entrega o index.html  |  WebSocket em /ws
# ======================================================================
_cache = {"mtime": None, "raw": b"", "gz": b""}


def load_index():
    st = os.stat(INDEX)
    if _cache["mtime"] != st.st_mtime_ns:
        with open(INDEX, "rb") as f:
            raw = f.read()
        _cache.update(mtime=st.st_mtime_ns, raw=raw, gz=gzip.compress(raw, 6))
    return _cache


async def http_reply(writer, status, body=b"", ctype="text/plain; charset=utf-8", extra="", head_only=False):
    head = ("HTTP/1.1 %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nCache-Control: no-store\r\n"
            "X-Content-Type-Options: nosniff\r\nConnection: close\r\n%s\r\n"
            % (status, ctype, len(body), extra))
    writer.write(head.encode("latin-1"))
    if body and not head_only:
        writer.write(body)
    await writer.drain()


async def serve_index(writer, method, headers):
    try:
        cache = load_index()
    except OSError:
        await http_reply(writer, "404 Not Found",
                         "Nao achei o index.html. Deixe o servidor.py na mesma pasta dele.".encode("utf-8"))
        return
    if "gzip" in headers.get("accept-encoding", ""):
        await http_reply(writer, "200 OK", cache["gz"], "text/html; charset=utf-8",
                         "Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n", method == "HEAD")
    else:
        await http_reply(writer, "200 OK", cache["raw"], "text/html; charset=utf-8",
                         "Vary: Accept-Encoding\r\n", method == "HEAD")


async def serve_ws(reader, writer, headers):
    key = headers.get("sec-websocket-key")
    if not key or headers.get("sec-websocket-version") != "13":
        await http_reply(writer, "400 Bad Request", b"WebSocket invalido")
        return
    if len(hub.clients) >= MAX_CLIENTS:
        await http_reply(writer, "503 Service Unavailable", b"Lousa lotada")
        return
    accept = base64.b64encode(hashlib.sha1(key.encode("latin-1") + GUID).digest()).decode()
    writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode("latin-1"))
    await writer.drain()
    c = hub.add(writer)
    ws = WSReader(reader)
    try:
        hub.welcome(c)
        while True:
            op, data = await asyncio.wait_for(ws.read(), IDLE_TIMEOUT)
            if op == 8:
                c.send_raw(ws_frame(data[:2], 8))
                break
            if op == 9:
                c.send_raw(ws_frame(data, 10))
            elif op == 1:
                hub.on_text(c, data)
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, OSError, ProtocolError):
        pass
    finally:
        hub.remove(c)


async def handle(reader, writer):
    try:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            lines = head.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError,
                ConnectionError, ValueError, UnicodeDecodeError):
            return
        path = target.split("?", 1)[0]
        if headers.get("upgrade", "").lower() == "websocket" and path == "/ws":
            await serve_ws(reader, writer, headers)
        elif method in ("GET", "HEAD") and path in ("/", "/index.html"):
            await serve_index(writer, method, headers)
        elif path == "/favicon.ico":
            await http_reply(writer, "204 No Content")
        else:
            await http_reply(writer, "404 Not Found", b"Nao encontrado")
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


# ======================================================================
#  Terminal e inicializacao
# ======================================================================
def lan_ips():
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))           # nao envia nada; so descobre a placa de rede em uso
        ips.append(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if not ip.startswith("127.") and ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    return ips


def on_command(cmd):
    if cmd in ("limpar", "clear"):
        hub.clear_all()
        log("Lousa limpa para todo mundo.")
    elif cmd in ("quem", "status"):
        log("%d conectado(s): %s | %d tracos na lousa" % (
            len(hub.clients), ", ".join(c.name for c in hub.clients.values()) or "-", len(hub.strokes)))
    elif cmd:
        print("Comandos: limpar  |  quem", flush=True)


def start_console(loop):
    def run():
        try:
            for line in sys.stdin:
                loop.call_soon_threadsafe(on_command, line.strip().lower())
        except Exception:
            pass
    threading.Thread(target=run, daemon=True).start()


async def main():
    try:
        server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    except OSError as e:
        print("Nao consegui abrir a porta %d (%s)." % (PORT, e))
        print("Provavelmente o servidor ja esta rodando em outra janela. Tente: python servidor.py %d" % (PORT + 1))
        return
    if not os.path.exists(INDEX):
        print("ATENCAO: nao achei o index.html em %s" % HERE)
    print("=" * 62)
    print(" Stylespace - lousa compartilhada no ar")
    print("=" * 62)
    print(" Abra no navegador (todos na mesma rede Wi-Fi):")
    for ip in lan_ips():
        print("     http://%s:%d" % (ip, PORT))
    print("     http://localhost:%d   (neste computador)" % PORT)
    print()
    print(" Digite 'limpar' + Enter para apagar a lousa de todo mundo.")
    print(" Ctrl+C para encerrar.")
    print("=" * 62, flush=True)
    start_console(asyncio.get_running_loop())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServidor encerrado.")
