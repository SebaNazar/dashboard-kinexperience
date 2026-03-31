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

    if creds.expired:
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
