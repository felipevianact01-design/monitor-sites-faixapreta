import os
import json
import time
import hashlib
import asyncio
import logging
import base64
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Monitor de Sites")

URLS_FILE = Path("urls.json")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "")
GITHUB_FILE  = "urls.json"

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# Cache em memória
_url_cache: list = []
_results_cache: list = []
_checking: bool = False
_last_check: str = ""
_check_id: int = 0


@app.on_event("startup")
async def startup():
    global _url_cache
    _url_cache = await _fetch_from_github()
    logger.info(f"Iniciado com {len(_url_cache)} URLs carregadas")


async def _fetch_from_github() -> list:
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
                    headers=GH_HEADERS,
                )
            if r.status_code == 200:
                content = base64.b64decode(r.json()["content"]).decode("utf-8")
                return json.loads(content)
        except Exception as e:
            logger.error(f"Erro ao ler GitHub: {e}")
    if URLS_FILE.exists():
        return json.loads(URLS_FILE.read_text(encoding="utf-8"))
    return []


def load_urls() -> list:
    return _url_cache


async def run_all_checks():
    global _results_cache, _checking, _last_check
    if _checking:
        return
    _checking = True
    logger.info("Iniciando verificação...")
    try:
        urls = load_urls()
        if not urls:
            return
        semaphore = asyncio.Semaphore(5)

        async def guarded(u):
            async with semaphore:
                return await check_url(u, client)

        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
        ) as client:
            raw = await asyncio.gather(*[guarded(u) for u in urls], return_exceptions=True)

        results = [r for r in raw if isinstance(r, dict)]
        order = {"red": 0, "yellow": 1, "green": 2}
        _results_cache = sorted(results, key=lambda x: order.get(x["color"], 9))
        _last_check = datetime.now().strftime("%d/%m %H:%M:%S")
        logger.info(f"Verificação concluída: {len(results)} sites")
    except Exception as e:
        logger.error(f"Erro na verificação: {e}")
    finally:
        _checking = False


async def _push_to_github(urls: list) -> None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
                headers=GH_HEADERS,
            )
            sha = r.json().get("sha") if r.status_code == 200 else None
            encoded = base64.b64encode(
                json.dumps(urls, ensure_ascii=False, indent=2).encode("utf-8")
            ).decode("utf-8")
            body = {"message": "atualiza urls.json via monitor", "content": encoded}
            if sha:
                body["sha"] = sha
            pr = await client.put(
                f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
                headers=GH_HEADERS,
                json=body,
            )
        if pr.status_code not in (200, 201):
            logger.error(f"Erro GitHub: {pr.text}")
        else:
            logger.info("urls.json salvo no GitHub")
    except Exception as e:
        logger.error(f"Erro ao salvar no GitHub: {e}")


async def save_urls(urls: list) -> None:
    global _url_cache
    _url_cache = urls
    if GITHUB_TOKEN and GITHUB_REPO:
        asyncio.create_task(_push_to_github(urls))
    else:
        URLS_FILE.write_text(json.dumps(urls, ensure_ascii=False, indent=2), encoding="utf-8")


class UrlEntry(BaseModel):
    name: str
    url: str


async def check_url(entry: dict, client: httpx.AsyncClient) -> dict:
    url = entry["url"]
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    start = time.time()
    code = None
    error = None

    try:
        response = await client.get(url)
        elapsed = round((time.time() - start) * 1000)
        code = response.status_code

        if 200 <= code < 300:
            status, color = ("Lento", "yellow") if elapsed > 5000 else ("Online", "green")
        elif 300 <= code < 400:
            status, color = "Redirecionando", "yellow"
        else:
            status, color = f"Erro {code}", "red"

    except httpx.ConnectTimeout:
        elapsed = round((time.time() - start) * 1000)
        status, color, error = "Timeout", "red", "Servidor não respondeu"
    except httpx.ConnectError:
        elapsed = round((time.time() - start) * 1000)
        status, color, error = "Fora do ar", "red", "Conexão recusada"
    except httpx.SSLError:
        elapsed = round((time.time() - start) * 1000)
        status, color, error = "Erro SSL", "yellow", "Certificado SSL inválido"
    except Exception as e:
        elapsed = round((time.time() - start) * 1000)
        status, color, error = "Erro", "red", str(e)[:120]

    return {
        **entry,
        "status": status,
        "color": color,
        "code": code,
        "elapsed_ms": elapsed,
        "error": error,
        "checked_at": datetime.now().strftime("%d/%m %H:%M:%S"),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(HTML)


@app.post("/api/check/start")
async def start_check():
    global _check_id
    _check_id += 1
    current_id = _check_id
    asyncio.create_task(run_all_checks())
    return {"started": True, "check_id": current_id}


@app.get("/api/check/result")
async def get_result():
    return {
        "checking": _checking,
        "results": _results_cache,
        "last_check": _last_check,
        "check_id": _check_id,
        "total": len(load_urls()),
    }


@app.get("/api/urls")
async def get_urls():
    return load_urls()


@app.post("/api/urls")
async def add_url(entry: UrlEntry):
    urls = load_urls().copy()
    new_entry = {
        "id": hashlib.md5(f"{entry.name}{entry.url}{time.time()}".encode()).hexdigest()[:8],
        "name": entry.name.strip(),
        "url": entry.url.strip(),
    }
    urls.append(new_entry)
    await save_urls(urls)
    logger.info(f"URL adicionada: {entry.name} — {entry.url}")
    return new_entry


@app.delete("/api/urls/{url_id}")
async def delete_url(url_id: str):
    urls = [u for u in load_urls() if u["id"] != url_id]
    await save_urls(urls)
    return {"ok": True}


HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Monitor de Sites</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    padding: 24px;
  }

  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 28px;
  }

  h1 { font-size: 1.5rem; font-weight: 700; color: #f8fafc; }
  h1 span { color: #6366f1; }

  .header-right {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }

  .summary { display: flex; gap: 10px; font-size: 0.85rem; }
  .badge {
    padding: 4px 12px;
    border-radius: 999px;
    font-weight: 600;
    font-size: 0.78rem;
  }
  .badge-green  { background: #14532d; color: #4ade80; }
  .badge-yellow { background: #422006; color: #fbbf24; }
  .badge-red    { background: #450a0a; color: #f87171; }

  .btn {
    padding: 8px 18px;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-size: 0.85rem;
    font-weight: 600;
    transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn-primary   { background: #6366f1; color: #fff; }
  .btn-secondary { background: #1e293b; color: #94a3b8; border: 1px solid #334155; }

  .add-form {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 24px;
    display: none;
  }
  .add-form.open { display: block; }
  .add-form h2 { font-size: 1rem; margin-bottom: 14px; color: #f1f5f9; }
  .form-row { display: flex; gap: 10px; flex-wrap: wrap; }
  .form-row input {
    flex: 1;
    min-width: 160px;
    padding: 9px 14px;
    background: #0f1117;
    border: 1px solid #334155;
    border-radius: 8px;
    color: #e2e8f0;
    font-size: 0.9rem;
    outline: none;
  }
  .form-row input:focus { border-color: #6366f1; }
  .form-row input::placeholder { color: #475569; }

  #grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 16px;
  }

  .card {
    background: #1e293b;
    border-radius: 12px;
    padding: 18px;
    border-left: 4px solid transparent;
    transition: transform 0.15s;
  }
  .card:hover { transform: translateY(-2px); }
  .card.green  { border-left-color: #22c55e; }
  .card.yellow { border-left-color: #f59e0b; }
  .card.red    { border-left-color: #ef4444; }
  .card.loading { border-left-color: #475569; opacity: 0.7; }

  .card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 10px;
  }

  .dot { width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; margin-top: 4px; }
  .dot.green   { background: #22c55e; box-shadow: 0 0 8px #22c55e88; }
  .dot.yellow  { background: #f59e0b; box-shadow: 0 0 8px #f59e0b88; }
  .dot.red     { background: #ef4444; box-shadow: 0 0 8px #ef444488; animation: pulse 1.5s infinite; }
  .dot.loading { background: #475569; }

  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }

  .card-name { font-weight: 700; font-size: 1rem; color: #f1f5f9; flex: 1; word-break: break-word; }

  .card-delete {
    background: none; border: none; color: #475569; cursor: pointer;
    font-size: 1.1rem; line-height: 1; padding: 2px 4px; border-radius: 4px;
    transition: color 0.15s; flex-shrink: 0;
  }
  .card-delete:hover { color: #ef4444; }

  .card-url { font-size: 0.78rem; color: #64748b; margin-bottom: 12px; word-break: break-all; }

  .card-status { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }

  .status-pill {
    padding: 3px 10px; border-radius: 999px;
    font-size: 0.78rem; font-weight: 700; letter-spacing: 0.02em;
  }
  .green  .status-pill { background: #14532d; color: #4ade80; }
  .yellow .status-pill { background: #422006; color: #fbbf24; }
  .red    .status-pill { background: #450a0a; color: #f87171; }
  .loading .status-pill { background: #1e293b; color: #94a3b8; }

  .card-meta { font-size: 0.75rem; color: #64748b; display: flex; gap: 12px; flex-wrap: wrap; }

  .card-error {
    margin-top: 8px; font-size: 0.75rem; color: #f87171;
    background: #450a0a44; border-radius: 6px; padding: 6px 10px;
  }

  #empty { text-align: center; padding: 60px 20px; color: #475569; display: none; }
  #empty.show { display: block; }
  #empty h3 { font-size: 1.1rem; margin-bottom: 8px; }

  .spinner {
    width: 16px; height: 16px; border: 2px solid #334155; border-top-color: #6366f1;
    border-radius: 50%; animation: spin 0.6s linear infinite;
    display: inline-block; margin-right: 6px; vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  #last-check { font-size: 0.78rem; color: #475569; }

  .refresh-bar { height: 3px; background: #1e293b; border-radius: 999px; margin-bottom: 20px; overflow: hidden; }
  .refresh-bar-fill { height: 100%; background: #6366f1; border-radius: 999px; transition: width 1s linear; }
</style>
</head>
<body>

<header>
  <h1>Monitor de <span>Sites</span></h1>
  <div class="header-right">
    <div class="summary">
      <span class="badge badge-green"  id="count-green">— online</span>
      <span class="badge badge-yellow" id="count-yellow">— alerta</span>
      <span class="badge badge-red"    id="count-red">— fora do ar</span>
    </div>
    <button class="btn btn-secondary" onclick="toggleForm()">+ Adicionar site</button>
    <button class="btn btn-primary" onclick="refresh()" id="btn-refresh">Verificar agora</button>
  </div>
</header>

<div class="add-form" id="add-form">
  <h2>Novo site</h2>
  <div class="form-row">
    <input type="text" id="new-name" placeholder="Nome do cliente (ex: Marmoraria Silva)" />
    <input type="url"  id="new-url"  placeholder="URL do site (ex: https://cliente.com.br)" />
    <button class="btn btn-primary" onclick="addUrl()">Salvar</button>
    <button class="btn btn-secondary" onclick="toggleForm()">Cancelar</button>
  </div>
</div>

<div class="refresh-bar"><div class="refresh-bar-fill" id="bar" style="width:0%"></div></div>
<span id="last-check"></span>

<div id="grid"></div>
<div id="empty">
  <h3>Nenhum site cadastrado ainda</h3>
  <p>Clique em "+ Adicionar site" para monitorar o primeiro cliente.</p>
</div>

<script>
const REFRESH_INTERVAL = 5 * 60;
let countdown = REFRESH_INTERVAL;
let timerInterval = null;
let pollInterval = null;
let load_urls_cache = [];

async function refresh() {
  clearInterval(pollInterval);
  countdown = REFRESH_INTERVAL;

  const btn = document.getElementById('btn-refresh');
  btn.innerHTML = '<span class="spinner"></span>Verificando…';
  btn.disabled = true;

  await showLoadingCards();

  // Dispara verificação no servidor (retorna imediatamente)
  const startRes = await fetch('/api/check/start', { method: 'POST' });
  const startData = await startRes.json();
  const expectedId = startData.check_id;

  // Fica consultando o resultado a cada 2 segundos
  pollInterval = setInterval(async () => {
    try {
      const res = await fetch('/api/check/result');
      const data = await res.json();

      // Para quando a verificação terminar (checking=false após o nosso start)
      if (!data.checking && data.check_id >= expectedId) {
        clearInterval(pollInterval);
        if (data.results && data.results.length > 0) {
          renderCards(data.results);
        }
        btn.innerHTML = 'Verificar agora';
        btn.disabled = false;
        document.getElementById('last-check').textContent =
          data.last_check ? 'Última verificação: ' + data.last_check : '';
        startTimer();
      }
    } catch (e) { console.error(e); }
  }, 2000);
}

async function showLoadingCards() {
  const res = await fetch('/api/urls');
  const urls = await res.json();
  load_urls_cache = urls;
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');

  if (urls.length === 0) {
    grid.innerHTML = '';
    empty.classList.add('show');
    return;
  }
  empty.classList.remove('show');
  grid.innerHTML = urls.map(u => `
    <div class="card loading" id="card-${u.id}">
      <div class="card-header">
        <span class="dot loading"></span>
        <span class="card-name">${esc(u.name)}</span>
        <button class="card-delete" onclick="deleteUrl('${u.id}')" title="Remover">✕</button>
      </div>
      <div class="card-url">${esc(u.url)}</div>
      <div class="card-status"><span class="status-pill"><span class="spinner"></span>Verificando…</span></div>
    </div>`).join('');
}

function renderCards(data) {
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');
  if (data.length === 0) {
    grid.innerHTML = '';
    empty.classList.add('show');
    updateCounts([], [], []);
    return;
  }
  empty.classList.remove('show');
  grid.innerHTML = data.map(s => {
    const meta = [
      s.code ? `HTTP ${s.code}` : '',
      s.elapsed_ms != null ? `${s.elapsed_ms} ms` : '',
      s.checked_at ? `às ${s.checked_at}` : '',
    ].filter(Boolean).join(' · ');
    return `
      <div class="card ${s.color}" id="card-${s.id}">
        <div class="card-header">
          <span class="dot ${s.color}"></span>
          <span class="card-name">${esc(s.name)}</span>
          <button class="card-delete" onclick="deleteUrl('${s.id}')" title="Remover">✕</button>
        </div>
        <div class="card-url"><a href="${esc(s.url)}" target="_blank" style="color:inherit">${esc(s.url)}</a></div>
        <div class="card-status"><span class="status-pill">${esc(s.status)}</span></div>
        <div class="card-meta">${esc(meta)}</div>
        ${s.error ? `<div class="card-error">${esc(s.error)}</div>` : ''}
      </div>`;
  }).join('');
  updateCounts(
    data.filter(s => s.color === 'green'),
    data.filter(s => s.color === 'yellow'),
    data.filter(s => s.color === 'red')
  );
}

function updateCounts(g, y, r) {
  document.getElementById('count-green').textContent  = `${g.length} online`;
  document.getElementById('count-yellow').textContent = `${y.length} alerta`;
  document.getElementById('count-red').textContent    = `${r.length} fora do ar`;
}

function esc(str) {
  if (!str) return '';
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function toggleForm() {
  const form = document.getElementById('add-form');
  form.classList.toggle('open');
  if (form.classList.contains('open')) document.getElementById('new-name').focus();
}

async function addUrl() {
  const name = document.getElementById('new-name').value.trim();
  const url  = document.getElementById('new-url').value.trim();
  if (!name || !url) { alert('Preencha o nome e a URL.'); return; }
  await fetch('/api/urls', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, url }),
  });
  document.getElementById('new-name').value = '';
  document.getElementById('new-url').value  = '';
  toggleForm();
  refresh();
}

async function deleteUrl(id) {
  if (!confirm('Remover este site do monitor?')) return;
  await fetch(`/api/urls/${id}`, { method: 'DELETE' });
  refresh();
}

function startTimer() {
  clearInterval(timerInterval);
  countdown = REFRESH_INTERVAL;
  updateBar();
  timerInterval = setInterval(() => {
    countdown--;
    updateBar();
    if (countdown <= 0) refresh();
  }, 1000);
}

function updateBar() {
  document.getElementById('bar').style.width =
    ((REFRESH_INTERVAL - countdown) / REFRESH_INTERVAL * 100) + '%';
}

document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.getElementById('add-form').classList.contains('open')) addUrl();
});

refresh().then(() => startTimer());
</script>
</body>
</html>
"""
