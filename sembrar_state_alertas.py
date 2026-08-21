#!/usr/bin/env python3.11
"""
sembrar_state_alertas.py — silencia alertas falsas antes de reanudar el dashboard
tras una interrupción larga.

## Por qué existe

`enviar_alertas_whatsapp()` dispara ante **cualquier** cambio de estado de un
paciente, incluida una mejora. Con el dashboard corriendo cada 30 minutos eso no
se nota: los cambios llegan de a uno y casi siempre son hacia peor. Pero cuando
el servicio estuvo caído varios días, la primera corrida compara el mundo de hoy
contra un state viejo, y a los pacientes que **mejoraron** en el intervalo les
manda un `kx_pack_critico` o un `kx_pack_terminado` al kine. Un "crítico" por
alguien que compró más sesiones es exactamente el ruido que enseña a ignorar la
alerta.

Este script siembra en la pestaña «Alertas State» el valor **actual** de esos
pacientes, de modo que la primera corrida los vea "sin cambio" y no les avise.
Los que empeoraron y los nuevos NO se tocan: esas alertas son legítimas y salen.

Se usó por primera vez el 21-ago-2026, al destrabar las 13 días de corridas
canceladas por un deployment de GitHub Pages encajado en `waiting`.

⚠️ Escribe en la pestaña «Alertas State» de la Ficha Central. Es state que el
propio `dashboard_pack.py` sobreescribe entero en cada corrida, no datos de
pacientes — pero es la Ficha Central igual. Por eso: dry-run por defecto, y
`--write` guarda antes una copia del state previo en disco.

⚠️ Correr ANTES de destrabar el workflow, no después.

Uso:
    python3.11 sembrar_state_alertas.py            # dry-run: muestra qué sembraría
    python3.11 sembrar_state_alertas.py --write    # siembra de verdad
"""
import argparse
import json
from datetime import datetime
from pathlib import Path

import dashboard_pack as dp


def a_int(valor):
    """Devuelve el int del estado, o None si no es numérico."""
    try:
        return int(str(valor).strip())
    except (ValueError, TypeError, AttributeError):
        return None


def clasificar(df, previo):
    """Separa a los que dispararían alerta en 'mejoraron' (ruido) y 'legítimos'.

    Replica la condición de disparo de enviar_alertas_whatsapp(): restantes <= 1
    y estado distinto al persistido. La comparación de nombres es exacta, igual
    que allá — leer_state_alertas() no normaliza las claves.
    """
    mejoraron, legitimos = [], []
    for _, row in df.iterrows():
        restantes = a_int(row["Sesiones Restantes"])
        if restantes is None or restantes > 1:
            continue

        paciente = str(row["Paciente"])
        actual = str(restantes)
        anterior = previo.get(paciente)
        if anterior == actual:
            continue  # ya silenciado, no dispara

        anterior_int = a_int(anterior)
        # Más sesiones restantes = mejor. Sólo es ruido si había estado previo
        # numérico y el paciente está mejor ahora que entonces.
        if anterior_int is not None and restantes > anterior_int:
            mejoraron.append((paciente, str(row["Kine"]), anterior_int, restantes))
        else:
            legitimos.append((paciente, str(row["Kine"]), anterior, restantes))
    return mejoraron, legitimos


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write", action="store_true",
                    help="Escribe en Sheets. Sin este flag es dry-run.")
    args = ap.parse_args()

    print("Conectando a Google Sheets...")
    cliente = dp.conectar()

    print("Leyendo Ficha Central y Registro de Sesiones...")
    ficha = dp.leer_sheet(cliente, dp.FICHA_CENTRAL_ID, dp.PESTAÑA_FICHA)
    registro = dp.leer_sheet(cliente, dp.REGISTRO_ID, dp.PESTAÑA_REGISTRO)
    df, _ = dp.calcular_dashboard(ficha, registro)

    ficha_sheet = cliente.open_by_key(dp.FICHA_CENTRAL_ID)
    previo = dp.leer_state_alertas(ficha_sheet)
    print(f"State persistido: {len(previo)} pacientes\n")

    mejoraron, legitimos = clasificar(df, previo)

    print("SE SIEMBRAN (mejoraron — la alerta sería falsa, no se avisa):")
    if not mejoraron:
        print("   (ninguno)")
    for pac, kine, antes, ahora in sorted(mejoraron, key=lambda x: x[3]):
        print(f"   {pac[:40]:41} {kine[:20]:21} {antes:>3} -> {ahora:>3}")

    print("\nNO SE TOCAN (alertas legítimas — van a salir al reanudar):")
    if not legitimos:
        print("   (ninguna)")
    for pac, kine, antes, ahora in sorted(legitimos, key=lambda x: x[3]):
        etiqueta = "nuevo" if antes is None else str(antes)
        print(f"   {pac[:40]:41} {kine[:20]:21} {etiqueta:>5} -> {ahora:>3}")

    print(f"\nResumen: {len(mejoraron)} a sembrar, {len(legitimos)} alertas "
          f"legítimas (~{len(legitimos) * 3} mensajes WhatsApp).")

    if not args.write:
        print("\n[DRY-RUN] No se escribió nada. Repetir con --write para sembrar.")
        return

    if not mejoraron:
        print("\nNada que sembrar — no se escribe.")
        return

    # Copia de seguridad antes de tocar la Ficha Central: guardar_state_alertas()
    # hace ws.clear() antes de escribir, así que sin esto un fallo a mitad de
    # camino deja el state vacío y sin forma de reconstruirlo.
    marca = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = Path(__file__).parent / f".alertas_state_backup_{marca}.json"
    backup.write_text(json.dumps(previo, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nBackup del state previo: {backup}")

    nuevo = dict(previo)
    for pac, _kine, _antes, ahora in mejoraron:
        nuevo[pac] = str(ahora)

    dp.guardar_state_alertas(ficha_sheet, nuevo)
    print(f"✅ Sembrados {len(mejoraron)} pacientes. Ya se puede destrabar el workflow.")


if __name__ == "__main__":
    main()
