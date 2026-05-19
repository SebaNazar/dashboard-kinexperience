import argparse
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pandas as pd
import unicodedata
import json
import os
from pathlib import Path
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
    pack_df = ficha[
        (~ficha['extension'].str.lower().str.contains('permanente', na=True)) &
        (ficha['estado'].isin(['Activo', 'Pausado']))
    ].copy()

    pack_df['nombre_norm'] = pack_df['nombre_paciente'].apply(normalizar)
    pack_df['kine_norm']   = pack_df['kine'].apply(normalizar)
    pack_df['inicio_pack'] = pd.to_datetime(
        pack_df['inicio_pack'], dayfirst=True, errors='coerce'
    )

    registro['nombre_norm'] = registro['Nombre del Paciente'].apply(normalizar)
    registro['kine_norm']   = registro['Nombre del Kinesiólogo '].apply(normalizar)
    registro['fecha']       = pd.to_datetime(
        registro['Fecha de la sesión realizada'], dayfirst=True, errors='coerce'
    )

    estados_consumidos = ['Realizada', 'Recuperada', 'Evaluación de ingreso']
    estados_modal      = ['Realizada', 'Recuperada', 'Evaluación de ingreso', 'Suspendida']

    registro_valido = registro[registro['Estado de la sesión'].isin(estados_consumidos)]
    registro_modal  = registro[registro['Estado de la sesión'].isin(estados_modal)]

    resultados = []
    sesiones_por_paciente = {}

    for _, paciente in pack_df.iterrows():
        nombre_p  = paciente['nombre_norm']
        kine_p    = paciente['kine_norm']
        inicio    = paciente['inicio_pack']
        cantidad  = paciente['cantidad_sesiones']

        def coincide_nombre(nombre_reg):
            palabras = nombre_reg.split()
            return all(p in nombre_p for p in palabras)

        candidatos = registro_valido[
            registro_valido['nombre_norm'].apply(coincide_nombre)
        ]
        candidatos_modal = registro_modal[
            registro_modal['nombre_norm'].apply(coincide_nombre)
        ]

        if len(candidatos['nombre_norm'].unique()) > 1:
            candidatos_kine = candidatos[candidatos['kine_norm'] == kine_p]
            if len(candidatos_kine) > 0:
                candidatos = candidatos_kine
                candidatos_modal = candidatos_modal[candidatos_modal['kine_norm'] == kine_p]
            else:
                resultados.append({
                    'Paciente':             paciente['nombre_paciente'],
                    'Kine':                 paciente['kine'],
                    'Pack':                 paciente['extension'],
                    'Estado':               paciente['estado'],
                    'Inicio Pack':          str(paciente['inicio_pack'].date()) if pd.notna(inicio) else '?',
                    'Sesiones Contratadas': cantidad,
                    'Sesiones Consumidas':  '?',
                    'Sesiones Restantes':   '?',
                    'Alerta':               '🚨 REVISAR MANUALMENTE'
                })
                sesiones_por_paciente[paciente['nombre_paciente']] = []
                continue

        if pd.notna(inicio):
            candidatos       = candidatos[candidatos['fecha'] >= inicio]
            candidatos_modal = candidatos_modal[candidatos_modal['fecha'] >= inicio]

        sesiones_consumidas = len(candidatos)

        try:
            contratadas = int(cantidad)
        except Exception:
            contratadas = 0

        restantes = contratadas - sesiones_consumidas

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

        # Construir detalle de sesiones para el modal, ordenadas por fecha
        detalle = []
        sesiones_ordenadas = candidatos_modal.sort_values('fecha')
        for idx, (_, s) in enumerate(sesiones_ordenadas.iterrows(), start=1):
            fecha_str = s['fecha'].strftime('%d/%m/%Y') if pd.notna(s['fecha']) else '?'
            detalle.append({
                'n':          idx,
                'Fecha':      fecha_str,
                'Estado':     s['Estado de la sesión'],
                'fuera_pack': idx > contratadas
            })

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
        sesiones_por_paciente[paciente['nombre_paciente']] = detalle

    df = pd.DataFrame(resultados).sort_values('Sesiones Restantes')
    return df, sesiones_por_paciente

# ── ESCRIBIR OUTPUT EN SHEETS ───────────────────────────────────
def escribir_dashboard(cliente, df):
    sheet = cliente.open_by_key(FICHA_CENTRAL_ID)

    try:
        ws = sheet.worksheet(PESTAÑA_OUTPUT)
        ws.clear()
    except Exception:
        ws = sheet.add_worksheet(title=PESTAÑA_OUTPUT, rows=200, cols=10)

    encabezados = list(df.columns)
    filas = [encabezados] + df.values.tolist()
    ws.update(filas, 'A1')

    print(f"✅ Dashboard escrito en pestaña '{PESTAÑA_OUTPUT}'")
    print(f"   {len(df)} pacientes pack procesados")

# ── ALERTAS WHATSAPP ────────────────────────────────────────────
def enviar_alertas_whatsapp(df, dry_run=False):
    script_dir  = Path(__file__).parent
    config_path = script_dir / "config.json"
    state_path  = script_dir / ".whatsapp_alertas_state.json"

    if not config_path.exists():
        print("⚠️  config.json no encontrado — saltando alertas WhatsApp")
        return

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    fijos = config["whatsapp"]["fijos"]
    # Normalizar claves del config para comparación robusta
    kines_cfg = {normalizar(k): v for k, v in config["whatsapp"]["kines"].items()}

    estado_previo = {}
    if state_path.exists():
        with open(state_path, encoding="utf-8") as f:
            estado_previo = json.load(f)

    sid   = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_ = "whatsapp:+14155238886"

    if not dry_run:
        if not sid or not token:
            print("⚠️  TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN no configurados — saltando alertas")
            return
        from twilio.rest import Client as TwilioClient
        twilio_client = TwilioClient(sid, token)

    estado_nuevo = dict(estado_previo)
    alertas_enviadas = 0

    for _, row in df.iterrows():
        restantes = row['Sesiones Restantes']

        # Solo filas numéricas con restantes <= 1
        try:
            restantes_int = int(restantes)
        except (ValueError, TypeError):
            continue

        if restantes_int > 1:
            continue

        paciente = str(row['Paciente'])
        kine     = str(row['Kine'])

        if restantes_int == 1:
            estado_actual = "1"
        elif restantes_int == 0:
            estado_actual = "0"
        else:
            estado_actual = str(restantes_int)  # negativo, e.g. "-2"

        if estado_previo.get(paciente) == estado_actual:
            print(f"   ⏭️  Sin cambio para {paciente} (restantes={restantes_int}) — no se reenvía")
            continue

        # Determinar destinatarios
        modo_prueba = config.get("modo_prueba", False)
        if modo_prueba:
            destinatarios = [fijos["seba"]]
        else:
            destinatarios = [fijos["seba"], fijos["mauricio"]]
            kine_norm = normalizar(kine)
            if kine_norm in kines_cfg:
                numero_kine = kines_cfg[kine_norm]
                if numero_kine not in destinatarios:
                    destinatarios.append(numero_kine)
            else:
                print(f"   ⚠️  Kine '{kine}' no está en config.json — se omite su número")

        if restantes_int < 0:
            detalle_restantes = f"⚠️ {abs(restantes_int)} sesión(es) SIN COBRAR"
        elif restantes_int == 0:
            detalle_restantes = "Pack terminado (0 restantes)"
        else:
            detalle_restantes = f"Queda 1 sesión"

        mensaje = (
            f"⚠️ *Alerta Pack Kinexperience*\n"
            f"Paciente: {paciente}\n"
            f"Kine: {kine}\n"
            f"Sesiones restantes: {restantes_int} — {detalle_restantes}"
        )

        for numero in destinatarios:
            if dry_run:
                print(f"   [DRY-RUN] → whatsapp:{numero}")
                print(f"   {mensaje}")
                print()
            else:
                try:
                    msg = twilio_client.messages.create(
                        from_=from_,
                        to=f"whatsapp:{numero}",
                        body=mensaje
                    )
                    print(f"   ✅ {numero} | SID: {msg.sid} | status: {msg.status}")
                except Exception as e:
                    print(f"   ❌ Error enviando a {numero}: {e}")

        estado_nuevo[paciente] = estado_actual
        alertas_enviadas += 1

    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(estado_nuevo, f, ensure_ascii=False, indent=2)

    modo = "[DRY-RUN] " if dry_run else ""
    print(f"✅ {modo}Alertas WhatsApp procesadas: {alertas_enviadas} paciente(s) con cambio de estado")

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

def generar_html(df, sesiones_por_paciente, output_path="index.html"):
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M")

    conteos = {'critico': 0, 'rojo': 0, 'naranja': 0, 'amarillo': 0, 'verde': 0}
    for _, row in df.iterrows():
        cls = alerta_clase(row['Alerta'])
        conteos[cls] += 1

    kines_unicos = sorted(df['Kine'].dropna().unique().tolist())
    opciones_kine = '<option value="todos">Todos</option>\n'
    for k in kines_unicos:
        opciones_kine += f'        <option value="{k}">{k}</option>\n'

    filas_html = ""
    for _, row in df.iterrows():
        cls         = alerta_clase(row['Alerta'])
        consumidas  = row['Sesiones Consumidas']
        contratadas = row['Sesiones Contratadas']
        restantes   = row['Sesiones Restantes']
        kine_val    = str(row['Kine']).replace('"', '&quot;')
        paciente    = str(row['Paciente'])
        inicio_pack = str(row['Inicio Pack'])

        if cls in ('critico', 'rojo'):
            nivel = 'urgente'
        elif cls in ('naranja', 'amarillo'):
            nivel = 'pocas'
        else:
            nivel = 'ok'

        try:
            pct = int(int(consumidas) / int(contratadas) * 100)
        except Exception:
            pct = 0

        lista_sesiones = sesiones_por_paciente.get(paciente, [])
        sesiones_json = json.dumps(lista_sesiones, ensure_ascii=False).replace("'", "&#39;")

        paciente_escaped = paciente.replace('"', '&quot;').replace("'", "&#39;")
        kine_escaped     = str(row['Kine']).replace('"', '&quot;').replace("'", "&#39;")

        filas_html += f"""
        <div class="card {cls}"
             data-kine="{kine_val}"
             data-nivel="{nivel}"
             data-paciente="{paciente_escaped}"
             data-kine-display="{kine_escaped}"
             data-inicio="{inicio_pack}"
             data-contratadas="{contratadas}"
             data-consumidas="{consumidas}"
             data-restantes="{restantes}"
             data-alerta-cls="{cls}"
             data-sesiones='{sesiones_json}'>
          <div class="card-top">
            <div class="paciente">{paciente}</div>
            <div class="alerta-badge badge-{cls}">{row['Alerta']}</div>
          </div>
          <div class="card-info">
            <span><strong>Kine:</strong> {row['Kine']}</span>
            <span><strong>Pack:</strong> {row['Pack']}</span>
            <span><strong>Inicio:</strong> {inicio_pack}</span>
            <span><strong>Estado:</strong> {row['Estado']}</span>
          </div>
          <div class="progreso-label">
            {consumidas} de {contratadas} sesiones consumidas
            &nbsp;·&nbsp; <strong>{restantes} restantes</strong>
          </div>
          <div class="barra-fondo">
            <div class="barra-fill barra-{cls}" style="width:{min(pct,100)}%"></div>
          </div>
          <button class="btn-detalle">Ver detalle de sesiones ›</button>
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

    .res-critico  {{ background: #fff1f0; color: #c0392b; }}
    .res-naranja  {{ background: #fff7f0; color: #e67e22; }}
    .res-amarillo {{ background: #fffbf0; color: #d4ac0d; }}
    .res-verde    {{ background: #f0fff4; color: #27ae60; }}

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
      margin-bottom: 10px;
    }}

    .barra-fill {{
      height: 100%;
      border-radius: 4px;
      transition: width 0.3s;
    }}

    .barra-critico, .barra-rojo {{ background: #e74c3c; }}
    .barra-naranja               {{ background: #e67e22; }}
    .barra-amarillo              {{ background: #f1c40f; }}
    .barra-verde                 {{ background: #27ae60; }}

    .btn-detalle {{
      width: 100%;
      text-align: center;
      font-size: 0.75rem;
      font-weight: 600;
      color: #667eea;
      background: none;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      padding: 6px 0;
      cursor: pointer;
      transition: all 0.15s;
    }}

    .btn-detalle:hover {{
      background: #f0f4ff;
      border-color: #667eea;
    }}

    /* ── MODAL ── */
    .modal-overlay {{
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.55);
      z-index: 200;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 16px;
    }}

    .modal-overlay.hidden {{ display: none; }}

    .modal {{
      background: white;
      border-radius: 16px;
      max-width: 520px;
      width: 100%;
      max-height: 82vh;
      overflow-y: auto;
      padding: 24px;
      position: relative;
      box-shadow: 0 20px 60px rgba(0,0,0,0.25);
    }}

    .modal-close {{
      position: absolute;
      top: 14px;
      right: 16px;
      background: #f0f2f5;
      border: none;
      border-radius: 50%;
      width: 30px;
      height: 30px;
      font-size: 1rem;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #4a5568;
      transition: background 0.15s;
    }}

    .modal-close:hover {{ background: #e2e8f0; }}

    .modal-header {{ margin-bottom: 14px; padding-right: 32px; }}

    .modal-header h2 {{
      font-size: 1.05rem;
      font-weight: 700;
      color: #1a1a2e;
      margin-bottom: 3px;
    }}

    .modal-header p {{
      font-size: 0.78rem;
      color: #718096;
    }}

    .modal-resumen {{
      display: flex;
      gap: 8px;
      margin-bottom: 16px;
    }}

    .modal-chip {{
      flex: 1;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 8px 4px;
      border-radius: 8px;
      gap: 2px;
    }}

    .modal-chip-num {{
      font-size: 1.3rem;
      font-weight: 700;
    }}

    .modal-chip-label {{
      font-size: 0.6rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      opacity: 0.75;
    }}

    .chip-contratadas {{ background: #f0f4ff; color: #3b5bdb; }}
    .chip-consumidas  {{ background: #f0f2f5; color: #495057; }}
    .chip-restantes-critico, .chip-restantes-rojo   {{ background: #fff1f0; color: #c0392b; }}
    .chip-restantes-naranja  {{ background: #fff7f0; color: #e67e22; }}
    .chip-restantes-amarillo {{ background: #fffbf0; color: #d4ac0d; }}
    .chip-restantes-verde    {{ background: #f0fff4; color: #27ae60; }}

    .modal-tabla-wrap {{ overflow-x: auto; margin-bottom: 14px; }}

    .modal-tabla {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.82rem;
    }}

    .modal-tabla th {{
      text-align: left;
      padding: 7px 10px;
      font-size: 0.72rem;
      font-weight: 700;
      color: #4a5568;
      text-transform: uppercase;
      letter-spacing: 0.4px;
      border-bottom: 2px solid #e2e8f0;
    }}

    .modal-tabla td {{
      padding: 7px 10px;
      border-bottom: 1px solid #f0f2f5;
      color: #2d3748;
    }}

    .modal-tabla tr:last-child td {{ border-bottom: none; }}

    .modal-tabla tr.fuera-pack td {{
      color: #c0392b;
      font-weight: 600;
    }}

    .modal-leyenda {{
      font-size: 0.72rem;
      color: #718096;
      border-top: 1px solid #e2e8f0;
      padding-top: 10px;
      line-height: 1.8;
    }}

    @media (max-width: 400px) {{
      .cards {{ padding: 10px; gap: 10px; }}
      .card-info {{ grid-template-columns: 1fr; }}
      .video-flotante {{ width: 100px; height: 100px; bottom: 12px; left: 12px; }}
      .modal {{ padding: 18px 14px; }}
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

  <!-- Modal historial de sesiones -->
  <div id="modal-overlay" class="modal-overlay hidden" role="dialog" aria-modal="true">
    <div class="modal">
      <button class="modal-close" id="modal-close-btn" aria-label="Cerrar">✕</button>
      <div class="modal-header">
        <h2 id="modal-paciente"></h2>
        <p id="modal-sub"></p>
      </div>
      <div class="modal-resumen" id="modal-resumen"></div>
      <div class="modal-tabla-wrap">
        <table class="modal-tabla">
          <thead>
            <tr>
              <th>#</th>
              <th>Fecha</th>
              <th>Estado</th>
            </tr>
          </thead>
          <tbody id="modal-tbody"></tbody>
        </table>
      </div>
      <div class="modal-leyenda">
        ✅ Realizada &nbsp;·&nbsp; 🔄 Recuperada &nbsp;·&nbsp; ⏸️ Suspendida &nbsp;·&nbsp; ⚠️ Fuera del pack
      </div>
    </div>
  </div>

  <script>
    (function () {{
      var selectKine    = document.getElementById('filtro-kine');
      var btnAlertas    = document.querySelectorAll('.btn-alerta');
      var cards         = document.querySelectorAll('#grid-cards .card');
      var sinResultados = document.getElementById('sin-resultados');
      var nivelActivo   = 'todos';

      function aplicarFiltros() {{
        var kineSeleccionado = selectKine.value;
        var visibles = 0;
        cards.forEach(function (card) {{
          var matchKine  = kineSeleccionado === 'todos' || card.dataset.kine === kineSeleccionado;
          var matchNivel = nivelActivo === 'todos' || card.dataset.nivel === nivelActivo;
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

      /* ── MODAL ── */
      var overlay  = document.getElementById('modal-overlay');
      var closeBtn = document.getElementById('modal-close-btn');

      function estadoEmoji(estado) {{
        var map = {{
          'Realizada':             '✅',
          'Recuperada':            '🔄',
          'Evaluación de ingreso': '✅',
          'Suspendida':            '⏸️'
        }};
        return (map[estado] || '') + ' ' + estado;
      }}

      function abrirModal(card) {{
        var paciente    = card.dataset.paciente;
        var kine        = card.dataset.kineDisplay;
        var inicio      = card.dataset.inicio;
        var contratadas = parseInt(card.dataset.contratadas) || 0;
        var consumidas  = parseInt(card.dataset.consumidas)  || 0;
        var restantes   = parseInt(card.dataset.restantes);
        var alertaCls   = card.dataset.alertaCls;
        var sesiones    = JSON.parse(card.dataset.sesiones);

        document.getElementById('modal-paciente').textContent = paciente;
        document.getElementById('modal-sub').textContent =
          'Kine: ' + kine + '  ·  Inicio pack: ' + inicio;

        var resumenEl = document.getElementById('modal-resumen');
        var chipRestClass = 'chip-restantes-' + alertaCls;
        resumenEl.innerHTML =
          '<div class="modal-chip chip-contratadas">' +
            '<span class="modal-chip-num">' + contratadas + '</span>' +
            '<span class="modal-chip-label">Contratadas</span>' +
          '</div>' +
          '<div class="modal-chip chip-consumidas">' +
            '<span class="modal-chip-num">' + consumidas + '</span>' +
            '<span class="modal-chip-label">Consumidas</span>' +
          '</div>' +
          '<div class="modal-chip ' + chipRestClass + '">' +
            '<span class="modal-chip-num">' + restantes + '</span>' +
            '<span class="modal-chip-label">Restantes</span>' +
          '</div>';

        var tbody = document.getElementById('modal-tbody');
        tbody.innerHTML = '';
        if (sesiones.length === 0) {{
          tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:#718096;padding:20px">Sin sesiones registradas</td></tr>';
        }} else {{
          sesiones.forEach(function (s) {{
            var tr = document.createElement('tr');
            if (s.fuera_pack) tr.classList.add('fuera-pack');
            var prefix = s.fuera_pack ? '⚠️ ' : '';
            tr.innerHTML =
              '<td>' + prefix + s.n + '</td>' +
              '<td>' + s.Fecha + '</td>' +
              '<td>' + estadoEmoji(s.Estado) + '</td>';
            tbody.appendChild(tr);
          }});
        }}

        overlay.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
      }}

      document.getElementById('grid-cards').addEventListener('click', function (e) {{
        var btn = e.target.closest('.btn-detalle');
        if (!btn) return;
        abrirModal(btn.closest('.card'));
      }});

      function cerrarModal() {{
        overlay.classList.add('hidden');
        document.body.style.overflow = '';
      }}

      closeBtn.addEventListener('click', cerrarModal);

      overlay.addEventListener('click', function (e) {{
        if (e.target === overlay) cerrarModal();
      }});

      document.addEventListener('keydown', function (e) {{
        if (e.key === 'Escape') cerrarModal();
      }});
    }})();
  </script>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTML generado: {os.path.abspath(output_path)}")

# ── MAIN ────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Dashboard Packs Kinexperience")
    parser.add_argument('--dry-run',      action='store_true', help='Simula alertas WhatsApp sin enviar mensajes reales')
    parser.add_argument('--no-whatsapp',  action='store_true', help='Salta completamente el envío de alertas WhatsApp')
    args = parser.parse_args()

    print("Conectando a Google Sheets...")
    cliente = conectar()

    print("Leyendo Ficha Central...")
    ficha = leer_sheet(cliente, FICHA_CENTRAL_ID, PESTAÑA_FICHA)

    print("Leyendo Registro de Sesiones...")
    registro = leer_sheet(cliente, REGISTRO_ID, PESTAÑA_REGISTRO)

    print("Calculando dashboard...")
    dashboard, sesiones_por_paciente = calcular_dashboard(ficha, registro)

    print(dashboard[['Paciente', 'Sesiones Restantes', 'Alerta']].to_string())

    print("\nEscribiendo en Drive...")
    escribir_dashboard(cliente, dashboard)

    print("\nGenerando HTML...")
    generar_html(dashboard, sesiones_por_paciente, output_path="index.html")

    if not args.no_whatsapp:
        print("\nProcesando alertas WhatsApp...")
        enviar_alertas_whatsapp(dashboard, dry_run=args.dry_run)
    else:
        print("\n⏭️  Alertas WhatsApp desactivadas (--no-whatsapp)")

if __name__ == "__main__":
    main()
