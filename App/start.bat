@echo off
cd /d "%~dp0"
echo Iniciando Sintesis Inversor...

if not exist venv (
    echo Creando entorno virtual...
    python -m venv venv
)

call venv\Scripts\activate

echo Instalando dependencias...
pip install -r requirements.txt --quiet

echo Iniciando servidor en http://localhost:8000
python main.py
pause
