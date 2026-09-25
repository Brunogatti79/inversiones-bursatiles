"""
scripts/backfill_signals_archive.py

Reconstruye el archivo permanente de señales (data/signals_archive_YYYY-MM.jsonl
+ data/signals_archive_runs.jsonl) a partir del HISTORIAL DE GIT de
data/signals_history.json. Plan semana 1, 25/09/2026 (sesión con Claude).

Queda en el repo como registro de auditoría: documenta exactamente cómo se
generaron las filas con origen="backfill_git". NO lo corre Railway.

Uso (desde un clon con historia; alcanza con --filter=blob:none):
    python scripts/backfill_signals_archive.py <ruta_repo> <dir_salida>

Criterios:
  - Para cada día calendario se toma el último commit de signals_history.json;
    cada fecha de señal sale del snapshot de su propio día (misma versión
    final que habría archivado el pipeline). Verificado el 25/09/2026: en
    6.321 comparaciones contra snapshots posteriores no hubo ni una fila
    mutada, así que la elección de snapshot no altera el resultado.
  - Se excluyen fines de semana.
  - code_sha es INFERIDO: último commit que tocó código (src/, *.py raíz,
    requirements.txt, Dockerfile) antes del snapshot. Railway tarda unos
    minutos en redeployar, así que en días con deploy muy cercano a una
    corrida puede corresponder al commit anterior. Por eso se marca
    code_sha_origen="inferido_git" (el pipeline escribe "railway_env").
  - pesos_optimizados: data/optimized_weights.json vigente en git a esa hora.
"""
import json
import subprocess
import sys
import os
from collections import OrderedDict
from datetime import date

CODE_PATHS = ["src", "main.py", "start_server.py", "requirements.txt", "Dockerfile"]


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True).stdout


def main(repo, out_dir):
    log = git(repo, "log", "--format=%H %cI", "--", "data/signals_history.json").split("\n")
    byday = OrderedDict()
    for line in log:  # más nuevo primero
        if not line.strip():
            continue
        h, ts = line.split()
        byday.setdefault(ts[:10], (h, ts))

    filas, runs, vistas = [], [], set()
    for day in sorted(byday):
        h, ts = byday[day]
        snap = json.loads(git(repo, "show", f"{h}:data/signals_history.json") or "{}")
        rows = snap.get(day)
        if not rows or date.fromisoformat(day).weekday() >= 5 or day in vistas:
            continue
        vistas.add(day)
        code_sha = git(repo, "rev-list", "-1", f"--before={ts}", "HEAD", "--", *CODE_PATHS).strip() or None
        w_commit = git(repo, "rev-list", "-1", f"--before={ts}", "HEAD", "--",
                       "data/optimized_weights.json").strip()
        try:
            pesos = json.loads(git(repo, "show", f"{w_commit}:data/optimized_weights.json")) if w_commit else None
        except Exception:
            pesos = None
        for r in rows:
            f = {"fecha": day}
            f.update(r)
            f.update({"code_sha": code_sha, "code_sha_origen": "inferido_git",
                      "origen": "backfill_git", "snapshot_commit": h, "archivado_en": ts})
            filas.append(f)
        runs.append({"fecha": day, "code_sha": code_sha, "code_sha_origen": "inferido_git",
                     "model_version": rows[0].get("model_version"), "n_filas": len(rows),
                     "pesos_optimizados": pesos, "origen": "backfill_git",
                     "snapshot_commit": h, "archivado_en": ts})

    os.makedirs(out_dir, exist_ok=True)
    por_mes = OrderedDict()
    for f in filas:
        por_mes.setdefault(f["fecha"][:7], []).append(f)
    for mes, fs in por_mes.items():
        with open(f"{out_dir}/signals_archive_{mes}.jsonl", "w", encoding="utf-8") as fh:
            fh.writelines(json.dumps(x, ensure_ascii=False) + "\n" for x in fs)
    with open(f"{out_dir}/signals_archive_runs.jsonl", "w", encoding="utf-8") as fh:
        fh.writelines(json.dumps(x, ensure_ascii=False) + "\n" for x in runs)
    print(f"{len(runs)} fechas hábiles, {len(filas)} filas, meses: {list(por_mes)}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
