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

    # Lista de kines únicos para el dropdown
    kines_unicos = sorted(df['Kine'].dropna().unique().tolist())
    opciones_kine = '<option value="todos">Todos</option>\n'
    for k in kines_unicos:
        opciones_kine += f'        <option value="{k}">{k}</option>\n'

    filas_html = ""
    for _, row in df.iterrows():
        cls = alerta_clase(row['Alerta'])
        consumidas = row['Sesiones Consumidas']
        contratadas = row['Sesiones Contratadas']
        restantes = row['Sesiones Restantes']
        kine_val = str(row['Kine']).replace('"', '&quot;')

        # Nivel de alerta para filtros JS
        if cls in ('critico', 'rojo'):
            nivel = 'urgente'
        elif cls in ('naranja', 'amarillo'):
            nivel = 'pocas'
        else:
            nivel = 'ok'

        try:
            pct = int(int(consumidas) / int(contratadas) * 100)
        except:
            pct = 0

        filas_html += f"""
        <div class="card {cls}" data-kine="{kine_val}" data-nivel="{nivel}">
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
      padding: 14px 16px 12px;
      position: sticky;
      top: 0;
      z-index: 10;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }}

    .header-inner {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}

    .header-logo {{
      height: 36px;
      width: auto;
      flex-shrink: 0;
      object-fit: contain;
    }}

    .header-text h1 {{
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: 0.3px;
      line-height: 1.2;
    }}

    .actualizacion {{
      font-size: 0.72rem;
      color: #a0aec0;
      margin-top: 2px;
    }}

    /* ── VIDEO FLOTANTE ── */
    .video-flotante {{
      position: fixed;
      bottom: 20px;
      left: 20px;
      width: 150px;
      height: 150px;
      border-radius: 50%;
      overflow: hidden;
      z-index: 100;
      box-shadow: 0 4px 16px rgba(0,0,0,0.3);
      pointer-events: none;
    }}

    .video-flotante video {{
      width: 100%;
      height: 100%;
      object-fit: cover;
      display: block;
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

    /* ── TOOLBAR DE FILTROS ── */
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
      padding: 10px 16px;
      background: #f8fafc;
      border-bottom: 1px solid #e2e8f0;
    }}

    .toolbar-group {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}

    .toolbar label {{
      font-size: 0.75rem;
      font-weight: 600;
      color: #4a5568;
      white-space: nowrap;
    }}

    #filtro-kine {{
      font-size: 0.8rem;
      padding: 5px 28px 5px 10px;
      border: 1px solid #cbd5e0;
      border-radius: 6px;
      background: white;
      color: #2d3748;
      cursor: pointer;
      appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%23718096'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 9px center;
    }}

    #filtro-kine:focus {{
      outline: none;
      border-color: #667eea;
      box-shadow: 0 0 0 2px rgba(102,126,234,0.2);
    }}

    .btn-alerta {{
      font-size: 0.75rem;
      font-weight: 600;
      padding: 5px 12px;
      border-radius: 6px;
      border: 1px solid #cbd5e0;
      background: white;
      color: #4a5568;
      cursor: pointer;
      transition: all 0.15s;
      white-space: nowrap;
    }}

    .btn-alerta:hover {{
      border-color: #a0aec0;
      background: #f7fafc;
    }}

    .btn-alerta.activo {{
      border-color: transparent;
    }}

    .btn-alerta[data-nivel="todos"].activo    {{ background: #1a1a2e; color: white; }}
    .btn-alerta[data-nivel="urgente"].activo  {{ background: #c0392b; color: white; border-color: #c0392b; }}
    .btn-alerta[data-nivel="pocas"].activo    {{ background: #e67e22; color: white; border-color: #e67e22; }}
    .btn-alerta[data-nivel="ok"].activo       {{ background: #27ae60; color: white; border-color: #27ae60; }}

    .btn-actualizar {{
      margin-left: auto;
      font-size: 0.75rem;
      font-weight: 600;
      padding: 5px 12px;
      border-radius: 6px;
      border: 1px solid #cbd5e0;
      background: white;
      color: #4a5568;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: all 0.15s;
      white-space: nowrap;
    }}

    .btn-actualizar:hover {{
      border-color: #667eea;
      color: #667eea;
      background: #f0f4ff;
    }}

    .sin-resultados {{
      display: none;
      grid-column: 1 / -1;
      text-align: center;
      padding: 40px 16px;
      color: #718096;
      font-size: 0.9rem;
    }}

    /* ── CARDS ── */
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 12px;
      padding: 16px;
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
      white-space: normal;
      word-break: break-word;
      flex-shrink: 0;
      max-width: 55%;
      text-align: center;
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

    .cards {{
      max-width: 1200px;
      margin: 0 auto;
    }}

    @media (max-width: 400px) {{
      .cards {{ padding: 10px; gap: 10px; }}
      .card-info {{ grid-template-columns: 1fr; }}
      .video-flotante {{ width: 100px; height: 100px; bottom: 12px; left: 12px; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <img
        class="header-logo"
        src="https://www.kinexperience.cl/_next/image?url=%2F_next%2Fstatic%2Fmedia%2FLOGO-FONDO-OSCURO.81c302e1.png&w=128&q=75"
        alt="Kinexperience"
      >
      <div class="header-text">
        <h1>Dashboard Packs Kinexperience</h1>
        <p class="actualizacion">Última actualización: {ahora}</p>
      </div>
    </div>
  </header>
  {resumen_html}
  <div class="toolbar">
    <div class="toolbar-group">
      <label for="filtro-kine">Filtrar por Kine:</label>
      <select id="filtro-kine">
        {opciones_kine}
      </select>
    </div>
    <div class="toolbar-group">
      <label>Alerta:</label>
      <button class="btn-alerta activo" data-nivel="todos">Todos</button>
      <button class="btn-alerta" data-nivel="urgente">Urgente</button>
      <button class="btn-alerta" data-nivel="pocas">Pocas sesiones</button>
      <button class="btn-alerta" data-nivel="ok">OK</button>
    </div>
    <a class="btn-actualizar" href="https://github.com/SebaNazar/dashboard-kinexperience/actions" target="_blank" rel="noopener noreferrer">&#x21BB; Actualizar datos</a>
  </div>
  <div class="cards" id="grid-cards">
    {filas_html}
    <div class="sin-resultados" id="sin-resultados">No hay pacientes que coincidan con los filtros seleccionados.</div>
  </div>

  <div class="video-flotante">
    <video autoplay loop muted playsinline>
      <source src="Paso001.mp4" type="video/mp4">
    </video>
  </div>

  <script>
    (function () {{
      var selectKine   = document.getElementById('filtro-kine');
      var btnAlertas   = document.querySelectorAll('.btn-alerta');
      var cards        = document.querySelectorAll('#grid-cards .card');
      var sinResultados = document.getElementById('sin-resultados');
      var nivelActivo  = 'todos';

      function aplicarFiltros() {{
        var kineSeleccionado = selectKine.value;
        var visibles = 0;

        cards.forEach(function (card) {{
          var matchKine  = kineSeleccionado === 'todos' || card.dataset.kine === kineSeleccionado;
          var matchNivel = nivelActivo === 'todos'   || card.dataset.nivel === nivelActivo;
          var mostrar    = matchKine && matchNivel;
          card.style.display = mostrar ? '' : 'none';
          if (mostrar) visibles++;
        }});

        sinResultados.style.display = visibles === 0 ? 'block' : 'none';
      }}

      selectKine.addEventListener('change', aplicarFiltros);

      btnAlertas.forEach(function (btn) {{
        btn.addEventListener('click', function () {{
          btnAlertas.forEach(function (b) {{ b.classList.remove('activo'); }});
          btn.classList.add('activo');
          nivelActivo = btn.dataset.nivel;
          aplicarFiltros();
        }});
      }});
    }})();
  </script>
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
