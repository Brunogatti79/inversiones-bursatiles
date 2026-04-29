# 📊 Inversiones Bursátiles — Pipeline Automático

Pipeline de análisis financiero con descarga automática desde Yahoo Finance,  
generación de dashboard HTML, fichas Excel y notificaciones completas a Telegram.  
Deploy en **GitHub → Railway** con ejecución diaria al cierre de mercado.

---

## Arquitectura

```
Yahoo Finance (yfinance)
       ↓  descarga automática
   Railway (Python worker)
       ├── APScheduler (cron diario 21:30 UTC = 18:30 ART)
       ├── Motor de análisis (señales + rankings)
       ├── Generador HTML + Excel
       └── Telegram Bot
              ├── Informe diario (señales + link)
              ├── Alertas de cambio de señal
              └── Comandos: /run /status /señales
```

---

## Paso 1 — Crear el Bot de Telegram

1. Abrí Telegram y buscá **@BotFather**
2. Enviá `/newbot` y seguí las instrucciones
3. Guardá el **token** que te da (formato `7xxx:AAA...`)
4. Para obtener tu **Chat ID**:
   - Buscá **@userinfobot** y enviá cualquier mensaje
   - Te devuelve tu ID numérico (ej. `123456789`)
   - Si usás un grupo: agregá el bot al grupo, enviá un mensaje,  
     luego visitá `https://api.telegram.org/bot<TOKEN>/getUpdates`  
     y buscá el `chat.id` (empieza con `-100...`)

---

## Paso 2 — Crear el repositorio en GitHub

```bash
# Clonar / inicializar el proyecto
git init inversiones-bursatiles
cd inversiones-bursatiles

# Copiar todos los archivos de este proyecto aquí
# Luego:
git add .
git commit -m "feat: pipeline inversiones bursátiles inicial"

# Crear repo en GitHub (podés hacerlo desde la web o con gh CLI)
gh repo create inversiones-bursatiles --private --source=. --push
# O sin gh CLI:
# Creá el repo en github.com, luego:
# git remote add origin https://github.com/TU_USUARIO/inversiones-bursatiles.git
# git push -u origin main
```

> ⚠️ Verificá que `.gitignore` está correctamente configurado  
> y que el archivo `.env` **NUNCA** se sube al repo.

---

## Paso 3 — Deploy en Railway

### 3.1 Crear el proyecto

1. Ir a [railway.app](https://railway.app) → **New Project**
2. Seleccionar **Deploy from GitHub repo**
3. Buscar y seleccionar `inversiones-bursatiles`
4. Railway detectará el `Dockerfile` automáticamente

### 3.2 Configurar variables de entorno

En Railway → tu proyecto → **Variables**, agregar:

| Variable | Valor | Descripción |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `7xxx:AAA...` | Token del bot |
| `TELEGRAM_CHAT_ID` | `-100xxx` o `123456` | ID del chat/grupo |
| `RUN_TIME_UTC` | `21:30` | Hora de ejecución en UTC |
| `TIMEZONE` | `America/Argentina/Buenos_Aires` | Tu zona horaria |
| `DASHBOARD_BASE_URL` | `https://tu-app.up.railway.app` | URL pública (ver paso 3.3) |
| `SEND_EXCEL` | `true` | Enviar Excel por Telegram |
| `SEND_ALERT_ON_CHANGE` | `true` | Alertas de cambio de señal |
| `DEBUG_MODE` | `false` | Logs detallados |

### 3.3 Obtener la URL pública

1. En Railway → tu servicio → **Settings** → **Networking**
2. Hacer clic en **Generate Domain**
3. Copiar la URL generada (ej. `inversiones-abc123.up.railway.app`)
4. Pegarlo en la variable `DASHBOARD_BASE_URL`

### 3.4 Primer deploy

Railway hace el build automáticamente al pushear a `main`.  
Para verificar: ir a **Deployments** → ver logs en tiempo real.

Deberías ver:
```
═══ Inversiones Bursátiles — Iniciando ═══
Scheduler activo — ejecución diaria a las 21:30 UTC
Bot de Telegram iniciado — modo polling
```

Y en Telegram recibirás el mensaje de inicio con los comandos disponibles.

---

## Paso 4 — Ejecutar el primer análisis

### Opción A — Desde Telegram (recomendado)
Enviá `/run` al bot. El pipeline completo tarda ~3-5 minutos.

### Opción B — Desde Railway
En Railway → tu servicio → **Settings** → **Start Command**:
```
python main.py --run-now
```
Cambiar temporalmente, deploy, luego volver a `python main.py`.

### Opción C — En local (para testing)
```bash
# Instalar dependencias
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Copiar y configurar el .env
cp .env.example .env
# Editar .env con tus valores reales

# Ejecutar una vez
python -c "from src.pipeline import run_pipeline; run_pipeline()"
```

---

## Comandos del Bot

| Comando | Descripción |
|---|---|
| `/run` | Ejecuta el análisis completo ahora |
| `/status` | Estado: última ejecución, próxima, errores |
| `/señales` | Lista señales activas del modelo |
| `/help` | Ayuda y lista de comandos |

---

## Qué recibís en Telegram cada día

**1. Si hubo cambios de señal** (llega primero, son urgentes):
```
🚨 Cambios de señal detectados

🇦🇷 CEPU.BA  Central Puerto
   🟡 NEUTRAL/ESPERAR → 🟢 COMPRA

🇺🇸 XOM  ExxonMobil
   🟡 NEUTRAL/ESPERAR → 🟠 VENTA PARCIAL
```

**2. Informe diario:**
```
📊 Inversiones Bursátiles — 29/04/2026 18:32

Índices (12 meses)
🇦🇷 MERVAL 2,869,560  +32.92% 12m  |  Vol 44.4%
🇧🇷 BOVESPA 188,619  +39.62% 12m  |  Vol 16.1%
🇺🇸 S&P 500 7,139  +28.38% 12m  |  Vol 12.5%

Señales activas del modelo

🇦🇷 MERVAL
  Compras:
  ⭐ COMPRA FUERTE TRAN.BA Transener — Score 71 | Sem +1.2%
  🟢 COMPRA CEPU.BA Central Puerto — Score 63 | Sem -1.2%

🏆 Top 3 global
  1. ⭐ TRAN.BA — Score 71 | Sem +1.2% | Anual +92.4%
  ...

🔗 Ver dashboard completo

⏱ Próxima actualización: mañana al cierre
```

**3. Archivo Excel** con las 4 hojas (fichas de inversión).

---

## Estructura del proyecto

```
inversiones-bursatiles/
├── main.py                  # Entry point: scheduler + bot
├── requirements.txt
├── Dockerfile
├── railway.toml
├── .env.example             # Template de variables
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── downloader.py        # Yahoo Finance → DataFrames
│   ├── analyzer.py          # Señales + rankings + detección cambios
│   ├── generator.py         # HTML dashboard + Excel
│   ├── notifier.py          # Mensajes Telegram
│   ├── bot.py               # Comandos /run /status /señales
│   └── pipeline.py          # Orquestador del flujo completo
├── data/                    # CSVs descargados + estado (gitignored)
└── outputs/                 # Dashboards HTML + Excel (gitignored)
```

---

## Horarios de ejecución

| Mercado | Cierre local | UTC | RUN_TIME_UTC sugerido |
|---|---|---|---|
| MERVAL (Argentina) | 18:30 ART | 21:30 UTC | `21:30` |
| BOVESPA (Brasil) | 17:55 BRT | 20:55 UTC | `21:15` |
| S&P 500 (EE.UU.) | 16:00 EST | 21:00 UTC | `21:30` |

El valor `21:30 UTC` es el ideal para capturar los tres cierres.

---

## Actualizar señales manuales del modelo

Si querés actualizar los scores macro (normalmente semanales):  
Editar `src/analyzer.py` → `MACRO_SCORES` y hacer push.  
Railway re-deploya automáticamente.

---

## Troubleshooting

**El bot no responde:**  
Verificar `TELEGRAM_BOT_TOKEN` en Railway → Variables.

**Errores de descarga yfinance:**  
Yahoo Finance puede tener rate limits. El pipeline reintenta automáticamente.  
Si el error persiste: revisar logs en Railway → Deployments.

**El dashboard no carga:**  
Verificar que `DASHBOARD_BASE_URL` coincide con la URL de Railway.  
El HTML se sirve como static file — Railway necesita tener un static server  
o podés usar GitHub Pages (ver sección avanzada abajo).

---

## (Avanzado) Servir el HTML via GitHub Pages

Si Railway no sirve statics, podés pushear el HTML a una rama `gh-pages`:

```python
# Agregar al final de src/pipeline.py después de generate_dashboard():
import subprocess
subprocess.run([
    "git", "add", f"outputs/{dashboard_name}",
    "&&", "git", "commit", "-m", f"dashboard {run_date}",
    "&&", "git", "push", "origin", "gh-pages"
], check=True)
```

Y configurar `DASHBOARD_BASE_URL=https://TU_USUARIO.github.io/inversiones-bursatiles/outputs`.
