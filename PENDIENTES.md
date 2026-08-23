# Pendientes — dashboard-pack

Abierto el **21-ago-2026**, al destrabar las 13 días de corridas canceladas por
un deployment de GitHub Pages encajado en `waiting`.

⚠️ **Este es un repo git aparte** (`SebaNazar/dashboard-kinexperience`), anidado
dentro de `kine-empresa/` pero independiente: un commit desde la raíz no lo
incluye.

---

## La alerta se dispara cuando el paciente mejora

**Qué pasa.** `enviar_alertas_whatsapp()` (`dashboard_pack.py`) decide avisar
comparando el estado actual del paciente contra el persistido:

```python
if estado_previo.get(paciente) == estado_actual:
    continue   # sin cambio, no reenvía
```

Cualquier valor **distinto** dispara. Como el estado es el número de sesiones
restantes, un paciente que **mejora** —de `-4` a `-2` porque compró más
sesiones, o porque se corrigió el registro— cuenta como cambio y le manda un
`kx_pack_critico` a él, a Mauricio y a su kinesiólogo. Lo mismo de `-3` a `0`,
que sale como `kx_pack_terminado`.

**Por qué no se había notado.** Con el dashboard corriendo cada 30 minutos los
cambios llegan de a uno y casi siempre son hacia peor: el paciente consume
sesiones. La mejora es rara y silenciosa. El defecto sólo se hace visible tras
una interrupción larga, cuando la primera corrida compara el mundo de hoy contra
un state de hace días. El 21-ago-2026, al reanudar, **4 de las 10 alertas
pendientes eran falsas por esta causa** — dos de ellas `kx_pack_critico` a José
Aguilar por pacientes que habían mejorado.

**Qué hay que cambiar.** Avisar sólo cuando el paciente **empeora**: el estado
nuevo tiene que ser numéricamente menor que el persistido. Cuando mejora, hay
que actualizar el state igual —para que el próximo empeoramiento se detecte
desde el valor correcto— pero sin mandar mensaje. En pseudocódigo:

```python
anterior = a_int(estado_previo.get(paciente))
if anterior is not None and restantes >= anterior:
    estado_nuevo[paciente] = estado_actual   # se registra…
    continue                                  # …pero no se avisa
```

Ojo con los dos bordes: el paciente **nuevo** (sin estado previo) sí tiene que
avisar, y el estado previo puede no ser numérico si alguien editó la pestaña a
mano — en ese caso conviene avisar, que es el lado seguro.

**Mitigación provisional.** `sembrar_state_alertas.py` (en este mismo
directorio) siembra el valor actual de los que mejoraron, para silenciarlos
antes de reanudar tras una caída. Es un parche operativo, no el arreglo: hay que
correrlo a mano y **antes** de destrabar. Cuando la condición se corrija en
`dashboard_pack.py`, este script deja de ser necesario.

**Costo de no arreglarlo.** Bajo en operación normal, alto en credibilidad: un
"crítico" por alguien que mejoró es exactamente el mensaje que enseña al equipo
a ignorar la alerta.

---

## Contexto: por qué el dashboard estuvo 13 días caído

No fue el código. El run del **8-ago 11:17 UTC** creó un deployment de
`github-pages` que quedó en estado `waiting` —esperando una aprobación sin
reviewers y sin wait timer, o sea imposible de dar— y retuvo para siempre el
slot de `concurrency: group: "pages"`. Las 408 corridas siguientes se encolaron
detrás y murieron en `cancelled`, sin ejecutar un solo step.

**Lo que hay que recordar de esto:** quedaron en `cancelled`, **no en
`failure`**, así que GitHub no mandó ni un correo. El canal de aviso existía y
estuvo mudo 13 días porque el estado era el equivocado. Se destrabó cancelando a
mano el run zombie (`gh run cancel 31254628920`).

---

## Un 503 al leer el state puede disparar una avalancha de WhatsApp

**Qué pasa.** `leer_state_alertas()` se traga cualquier excepción y devuelve un
diccionario vacío:

```python
def leer_state_alertas(sheet):
    try:
        ...
    except Exception:
        return {}
```

Si Google responde un 503 justo en esa lectura —el mismo error transitorio que
tumbaba el 6-33% de las corridas diarias— el state vuelve **vacío**, y entonces
`estado_previo.get(paciente)` devuelve `None` para todos. Como `None` nunca
coincide con el estado actual, **cada paciente en alerta se considera un cambio**
y se le manda WhatsApp a él, a Mauricio y a su kinesiólogo. Con ~10 pacientes en
alerta son ~30 mensajes de una sentada, por un error de red de dos segundos.

**Por qué no se ha visto.** Hace falta que el 503 caiga exactamente en esa
llamada, que es una de varias por corrida. Es raro, pero el 21-ago-2026 se vio
lo cerca que está: al reanudar tras la caída salieron 10 alertas de golpe por
comparar contra un state viejo. Acá el gatillo sería comparar contra un state
*vacío*, que es peor.

**Qué hay que cambiar.** Distinguir «la pestaña no existe todavía» (que sí
justifica `{}`, y es el caso para el que se escribió ese `except`) de «no se
pudo leer» (que debe abortar). Si no se pudo leer el state, lo correcto es no
mandar ninguna alerta en esa corrida: saltear un ciclo de 30 minutos no cuesta
nada, y treinta WhatsApp falsos sí.

**Relación con el arreglo del 23-ago.** El reintento agregado en `leer_sheet`
(commit `04dd25b`) **no cubre esta ruta**: cubre las dos lecturas grandes del
principio, no la del state. Si se quiere cerrar esto, lo natural es envolver
también esta llamada con `_con_reintento` y estrechar el `except`.

**Costo de no arreglarlo.** Bajo en probabilidad, muy alto en impacto: una
avalancha de mensajes falsos a los kines y a Mauricio es el peor resultado
posible de un sistema de alertas.
