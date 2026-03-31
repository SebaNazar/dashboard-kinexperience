import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pandas as pd
import unicodedata
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── CONFIGURACIÓN ──────────────────────────────────────────────
FICHA_CENTRAL_ID = "1GvaHOXN916kzJa4SMJjceinGkUB4Ma7tdRMUf5xeUe8"
REGISTRO_ID      = "1kQgC5koSq-tgsP7W2Bxah7ilLUZrEgp6tY12XNByN-s"
TOKEN_PATH       = os.getenv("TOKEN_PATH")
CLIENT_ID        = os.getenv("CLIENT_ID")
CLIENT_SECRET    = os.getenv("CLIENT_SECRET")

PESTAÑA_FICHA    = "Ficha Central"
PESTAÑA_REGISTRO = "Respuestas de formulario 1"
PESTAÑA_OUTPUT   = "Dashboard Pack"

# ── NORMALIZACIÓN DE NOMBRES ────────────────────────────────────
def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    texto = texto.upper().strip()
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    while '  ' in texto:
        texto = texto.replace('  ', ' ')
    return texto

# ── CONEXIÓN A GOOGLE SHEETS ────────────────────────────────────
def conectar():
    # Modo GitHub Actions: construir token en memoria desde variables de entorno
    refresh_token_env = os.getenv("GOOGLE_REFRESH_TOKEN")
    if refresh_token_env:
        client_id     = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        creds = Credentials(
            token=None,
            refresh_token=refresh_token_env,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        # Modo local: leer token desde archivo
        with open(TOKEN_PATH) as f:
            token_data = json.load(f)
        creds = Credentials(
            token=token_data.get("access_token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )

    if not creds.valid or creds.expired:
        creds.refresh(Request())

    return gspread.authorize(creds)

# ── LEER SHEETS ─────────────────────────────────────────────────
def leer_sheet(cliente, sheet_id, pestaña):
    sheet = cliente.open_by_key(sheet_id)
    ws = sheet.worksheet(pestaña)
    datos = ws.get_all_records()
    return pd.DataFrame(datos)

# ── LÓGICA PRINCIPAL ────────────────────────────────────────────
def calcular_dashboard(ficha, registro):
    # Filtrar solo pacientes pack activos o pausados
    pack_df = ficha[
        (~ficha['extension'].str.lower().str.contains('permanente', na=True)) &
        (ficha['estado'].isin(['Activo', 'Pausado']))
    ].copy()

    # Normalizar nombres y kines en ficha
    pack_df['nombre_norm'] = pack_df['nombre_paciente'].apply(normalizar)
    pack_df['kine_norm']   = pack_df['kine'].apply(normalizar)

    # Parsear inicio_pack
    pack_df['inicio_pack'] = pd.to_datetime(
        pack_df['inicio_pack'], dayfirst=True, errors='coerce'
    )

    # Normalizar nombres y kines en registro
    registro['nombre_norm'] = registro['Nombre del Paciente'].apply(normalizar)
    registro['kine_norm']   = registro['Nombre del Kinesiólogo '].apply(normalizar)
    registro['fecha']       = pd.to_datetime(
        registro['Fecha de la sesión realizada'], dayfirst=True, errors='coerce'
    )

    # Estados que cuentan como sesión consumida
    estados_consumidos = ['Realizada', 'Recuperada', 'Evaluación de ingreso']
    registro_valido = registro[registro['Estado de la sesión'].isin(estados_consumidos)]

    # ── CRUCE Y CONTEO ──────────────────────────────────────────
    resultados = []

    for _, paciente in pack_df.iterrows():
        nombre_p  = paciente['nombre_norm']
        kine_p    = paciente['kine_norm']
        inicio    = paciente['inicio_pack']
        cantidad  = paciente['cantidad_sesiones']

        # Buscar coincidencias por nombre (parcial)
        def coincide_nombre(nombre_reg):
            palabras = nombre_reg.split()
            return all(p in nombre_p for p in palabras)

        candidatos = registro_valido[
            registro_valido['nombre_norm'].apply(coincide_nombre)
        ]

        # Si hay ambigüedad, desempatar con kine
        if len(candidatos['nombre_norm'].unique()) > 1:
            candidatos_kine = candidatos[candidatos['kine_norm'] == kine_p]
            if len(candidatos_kine) > 0:
                candidatos = candidatos_kine
            else:
                # Marcar para revisión manual
                resultados.append({
                    'Paciente':            paciente['nombre_paciente'],
                    'Kine':                paciente['kine'],
                    'Pack':                paciente['extension'],
                    'Estado':              paciente['estado'],
                    'Inicio Pack':         str(paciente['inicio_pack'].date()) if pd.notna(inicio) else '?',
                    'Sesiones Contratadas': cantidad,
                    'Sesiones Consumidas': '?',
                    'Sesiones Restantes':  '?',
                    'Alerta':              '🚨 REVISAR MANUALMENTE'
                })
                continue

        # Filtrar desde inicio_pack
        if pd.notna(inicio):
            candidatos = candidatos[candidatos['fecha'] >= inicio]

        sesiones_consumidas = len(candidatos)

        try:
            contratadas = int(cantidad)
        except:
            contratadas = 0

        restantes = contratadas - sesiones_consumidas

        # Definir alerta
        if restantes > 2:
            alerta = '✅ OK'
        elif restantes == 2:
            alerta = '🟡 Quedan 2 sesiones'
        elif restantes == 1:
            alerta = '🟠 Queda 1 sesión'
        elif restantes == 0:
            alerta = '🔴 Pack terminado'
        else:
            alerta = f'🚨 CRÍTICO: {abs(restantes)} sesión(es) sin cobrar'

        resultados.append({
            'Paciente':             paciente['nombre_paciente'],
            'Kine':                 paciente['kine'],
            'Pack':                 paciente['extension'],
            'Estado':               paciente['estado'],
            'Inicio Pack':          str(inicio.date()) if pd.notna(inicio) else '?',
            'Sesiones Contratadas': contratadas,
            'Sesiones Consumidas':  sesiones_consumidas,
            'Sesiones Restantes':   restantes,
            'Alerta':               alerta
        })

    return pd.DataFrame(resultados).sort_values('Sesiones Restantes')

# ── ESCRIBIR OUTPUT EN SHEETS ───────────────────────────────────
def escribir_dashboard(cliente, df):
    sheet  = cliente.open_by_key(FICHA_CENTRAL_ID)

    # Crear pestaña si no existe
    try:
        ws = sheet.worksheet(PESTAÑA_OUTPUT)
        ws.clear()
    except:
        ws = sheet.add_worksheet(title=PESTAÑA_OUTPUT, rows=200, cols=10)

    # Escribir encabezados y datos
    encabezados = list(df.columns)
    filas = [encabezados] + df.values.tolist()
    ws.update(filas, 'A1')

    print(f"✅ Dashboard escrito en pestaña '{PESTAÑA_OUTPUT}'")
    print(f"   {len(df)} pacientes pack procesados")

# ── GENERAR HTML ────────────────────────────────────────────────
def alerta_clase(alerta):
    alerta_str = str(alerta)
    if 'CRÍTICO' in alerta_str or 'REVISAR' in alerta_str:
        return 'critico'
    elif 'terminado' in alerta_str:
        return 'rojo'
    elif 'Queda 1' in alerta_str:
        return 'naranja'
    elif 'Quedan 2' in alerta_str:
        return 'amarillo'
    else:
        return 'verde'

def generar_html(df, output_path="index.html"):
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M")

    conteos = {
        'critico': 0, 'rojo': 0, 'naranja': 0, 'amarillo': 0, 'verde': 0
    }
    for _, row in df.iterrows():
        cls = alerta_clase(row['Alerta'])
        conteos[cls] += 1

    filas_html = ""
    for _, row in df.iterrows():
        cls = alerta_clase(row['Alerta'])
        consumidas = row['Sesiones Consumidas']
        contratadas = row['Sesiones Contratadas']
        restantes = row['Sesiones Restantes']

        try:
            pct = int(int(consumidas) / int(contratadas) * 100)
        except:
            pct = 0

        filas_html += f"""
        <div class="card {cls}">
          <div class="card-top">
            <div class="paciente">{row['Paciente']}</div>
            <div class="alerta-badge badge-{cls}">{row['Alerta']}</div>
          </div>
          <div class="card-info">
            <span><strong>Kine:</strong> {row['Kine']}</span>
            <span><strong>Pack:</strong> {row['Pack']}</span>
            <span><strong>Inicio:</strong> {row['Inicio Pack']}</span>
            <span><strong>Estado:</strong> {row['Estado']}</span>
          </div>
          <div class="progreso-label">
            {consumidas} de {contratadas} sesiones consumidas
            &nbsp;·&nbsp; <strong>{restantes} restantes</strong>
          </div>
          <div class="barra-fondo">
            <div class="barra-fill barra-{cls}" style="width:{min(pct,100)}%"></div>
          </div>
        </div>"""

    resumen_html = f"""
      <div class="resumen">
        <div class="res-item res-critico"><span class="res-num">{conteos['critico'] + conteos['rojo']}</span><span class="res-label">Urgente</span></div>
        <div class="res-item res-naranja"><span class="res-num">{conteos['naranja']}</span><span class="res-label">1 sesión</span></div>
        <div class="res-item res-amarillo"><span class="res-num">{conteos['amarillo']}</span><span class="res-label">2 sesiones</span></div>
        <div class="res-item res-verde"><span class="res-num">{conteos['verde']}</span><span class="res-label">OK</span></div>
      </div>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dashboard Packs Kinexperience</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #f0f2f5;
      color: #1a1a2e;
      min-height: 100vh;
    }}

    header {{
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
      color: white;
      padding: 20px 16px 16px;
      position: sticky;
      top: 0;
      z-index: 10;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }}

    header h1 {{
      font-size: 1.25rem;
      font-weight: 700;
      letter-spacing: 0.3px;
    }}

    .actualizacion {{
      font-size: 0.75rem;
      color: #a0aec0;
      margin-top: 4px;
    }}

    .resumen {{
      display: flex;
      gap: 8px;
      padding: 12px 16px;
      background: white;
      border-bottom: 1px solid #e2e8f0;
      overflow-x: auto;
    }}

    .res-item {{
      flex: 1;
      min-width: 60px;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 8px 4px;
      border-radius: 8px;
      gap: 2px;
    }}

    .res-num {{
      font-size: 1.5rem;
      font-weight: 700;
    }}

    .res-label {{
      font-size: 0.65rem;
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      opacity: 0.8;
    }}

    .res-critico {{ background: #fff1f0; color: #c0392b; }}
    .res-naranja {{ background: #fff7f0; color: #e67e22; }}
    .res-amarillo {{ background: #fffbf0; color: #d4ac0d; }}
    .res-verde {{ background: #f0fff4; color: #27ae60; }}

    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 12px;
      padding: 16px;
      max-width: 1200px;
      margin: 0 auto;
    }}

    .card {{
      background: white;
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.08);
      border-left: 4px solid #cbd5e0;
      transition: box-shadow 0.2s;
    }}

    .card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.12); }}

    .card.critico  {{ border-left-color: #c0392b; background: #fffafa; }}
    .card.rojo     {{ border-left-color: #e74c3c; background: #fffafa; }}
    .card.naranja  {{ border-left-color: #e67e22; background: #fffdf9; }}
    .card.amarillo {{ border-left-color: #f1c40f; background: #fffef9; }}
    .card.verde    {{ border-left-color: #27ae60; background: #fafffc; }}

    .card-top {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
      margin-bottom: 10px;
    }}

    .paciente {{
      font-size: 0.95rem;
      font-weight: 700;
      line-height: 1.3;
      flex: 1;
    }}

    .alerta-badge {{
      font-size: 0.7rem;
      font-weight: 600;
      padding: 3px 8px;
      border-radius: 20px;
      white-space: nowrap;
      flex-shrink: 0;
    }}

    .badge-critico  {{ background: #fde8e8; color: #c0392b; }}
    .badge-rojo     {{ background: #fde8e8; color: #e74c3c; }}
    .badge-naranja  {{ background: #fef0e6; color: #e67e22; }}
    .badge-amarillo {{ background: #fef9e7; color: #b7950b; }}
    .badge-verde    {{ background: #e8f8f0; color: #27ae60; }}

    .card-info {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 3px 12px;
      font-size: 0.78rem;
      color: #555;
      margin-bottom: 10px;
    }}

    .progreso-label {{
      font-size: 0.75rem;
      color: #666;
      margin-bottom: 5px;
    }}

    .barra-fondo {{
      background: #edf2f7;
      border-radius: 4px;
      height: 6px;
      overflow: hidden;
    }}

    .barra-fill {{
      height: 100%;
      border-radius: 4px;
      transition: width 0.3s;
    }}

    .barra-critico, .barra-rojo     {{ background: #e74c3c; }}
    .barra-naranja                  {{ background: #e67e22; }}
    .barra-amarillo                 {{ background: #f1c40f; }}
    .barra-verde                    {{ background: #27ae60; }}

    @media (max-width: 400px) {{
      .cards {{ padding: 10px; gap: 10px; }}
      .card-info {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Dashboard Packs Kinexperience</h1>
    <p class="actualizacion">Última actualización: {ahora}</p>
  </header>
  {resumen_html}
  <div class="cards">
    {filas_html}
  </div>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTML generado: {os.path.abspath(output_path)}")

# ── MAIN ────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Conectando a Google Sheets...")
    cliente = conectar()

    print("Leyendo Ficha Central...")
    ficha = leer_sheet(cliente, FICHA_CENTRAL_ID, PESTAÑA_FICHA)

    print("Leyendo Registro de Sesiones...")
    registro = leer_sheet(cliente, REGISTRO_ID, PESTAÑA_REGISTRO)

    print("Calculando dashboard...")
    dashboard = calcular_dashboard(ficha, registro)

    print(dashboard[['Paciente', 'Sesiones Restantes', 'Alerta']].to_string())

    print("\nEscribiendo en Drive...")
    escribir_dashboard(cliente, dashboard)

    print("\nGenerando HTML...")
    generar_html(dashboard, output_path="index.html")
