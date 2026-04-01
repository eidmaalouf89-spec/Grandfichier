# launcher.py
"""
JANSA GrandFichier Updater — Launcher
Point d'entrée PyInstaller. Lance le backend FastAPI et ouvre le navigateur.
"""
import sys
import os
import time
import threading
import webbrowser
import urllib.request
import urllib.error

# ─── Résolution des chemins en mode PyInstaller ──────────────────────────────
def get_base_path():
    """Retourne le chemin de base des ressources (frozen ou dev)."""
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

BASE_PATH = get_base_path()

# Ajouter BASE_PATH au sys.path pour que les imports fonctionnent
if BASE_PATH not in sys.path:
    sys.path.insert(0, BASE_PATH)

# Changer le CWD vers BASE_PATH pour que les chemins relatifs (data/, input/, output/) fonctionnent
os.chdir(BASE_PATH)

# ─── Constantes ──────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 8000
URL = f"http://{HOST}:{PORT}"
MAX_WAIT_SECONDS = 30
POLL_INTERVAL = 0.5

# ─── Démarrage du serveur ─────────────────────────────────────────────────────
def start_server():
    """Lance uvicorn dans un thread daemon."""
    import uvicorn
    # Import de l'app FastAPI
    from api_server import app
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")

server_thread = threading.Thread(target=start_server, daemon=True)
server_thread.start()

# ─── Attente que le serveur soit prêt ────────────────────────────────────────
print("JANSA GrandFichier Updater — Démarrage...")
waited = 0
ready = False
while waited < MAX_WAIT_SECONDS:
    try:
        urllib.request.urlopen(f"{URL}/api/runs", timeout=1)
        ready = True
        break
    except Exception:
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL

if not ready:
    print(f"ERREUR : Le serveur n'a pas démarré après {MAX_WAIT_SECONDS}s.")
    print("Vérifiez qu'aucune autre application n'utilise le port 8000.")
    input("Appuyez sur Entrée pour fermer...")
    sys.exit(1)

# ─── Ouverture du navigateur ──────────────────────────────────────────────────
print(f"Serveur prêt. Ouverture de {URL} dans votre navigateur...")
webbrowser.open(URL)

# ─── Maintien de l'application active ────────────────────────────────────────
print("L'application est en cours d'exécution.")
print("Fermez cette fenêtre pour arrêter l'application.")
print("(Ne fermez pas cette fenêtre pendant un traitement en cours)")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nArrêt de l'application.")
    sys.exit(0)
