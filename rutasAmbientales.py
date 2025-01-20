import redis
import json

# Conexión a Redis
baseDatosRedis = redis.Redis(host='localhost', port=6379, db=0)

# ==========================
# 1. Crear registros clave-valor
# ==========================
def crearRegistros():
    """Crear registros clave-valor con datos de rutas."""
    print("Punto 1: Crear registros clave-valor.")
    
    # Datos de las rutas
    ruta1 = {"kilometros": 5, "dificultad": "Fácil", "floraYFauna": "Flora típica", "numParticipantes": 10}
    ruta2 = {"kilometros": 15, "dificultad": "Moderada", "floraYFauna": "Fauna variada", "numParticipantes": 8}
    ruta3 = {"kilometros": 20, "dificultad": "Difícil", "floraYFauna": "Fauna salvaje", "numParticipantes": 5}
    ruta4 = {"kilometros": 8, "dificultad": "Fácil", "floraYFauna": "Flora abundante", "numParticipantes": 12}
    
    # Guardamos las rutas en Redis usando claves de tipo "ruta:{id}"
    baseDatosRedis.set("ruta:1", json.dumps(ruta1))
    baseDatosRedis.set("ruta:2", json.dumps(ruta2))
    baseDatosRedis.set("ruta:3", json.dumps(ruta3))
    baseDatosRedis.set("ruta:4", json.dumps(ruta4))

    # Imprimimos los registros creados
    print("Registros creados correctamente:")
    print(" - ruta:1 ->", ruta1)
    print(" - ruta:2 ->", ruta2)
    print(" - ruta:3 ->", ruta3)
    print(" - ruta:4 ->", ruta4)
    print("---------------------------------------------------------------------------")

# ==========================
# 2. Obtener y mostrar el número de claves registradas
# ==========================
def obtenerNumeroDeClaves():
    """Mostrar el número de claves registradas en Redis."""
    print("Punto 2: Número de claves registradas.")
    
    claves = baseDatosRedis.keys()  # Obtenemos todas las claves
    print(f"Número de claves registradas: {len(claves)}")
    print("---------------------------------------------------------------------------")

# ==========================
# 3. Obtener y mostrar un registro en base a una clave
# ==========================
def obtenerRegistro():
    """Obtener y mostrar un registro basado en la clave 'ruta:1'."""
    print("Punto 3: Obtener y mostrar un registro en base a una clave.")
    
    ruta_id = "ruta:1"  # Especificamos que vamos a buscar la ruta con la clave "ruta:1"
    ruta = baseDatosRedis.get(ruta_id)  # Recuperamos el valor asociado a la clave
    if ruta:
        ruta_json = json.loads(ruta)  # Convertimos el valor en JSON
        print(f"Clave: {ruta_id} - Detalles: {ruta_json}")
    else:
        print(f"Error: No se ha encontrado la clave {ruta_id} en Redis.")
    print("---------------------------------------------------------------------------")

# ==========================
# 4. Actualizar el valor de una clave y mostrar el nuevo valor
# ==========================
def actualizarRegistro():
    """Actualizar el valor de un registro en base a la clave 'ruta:2'."""
    print("Punto 4: Actualizar el valor de una clave y mostrar el nuevo valor.")
    
    ruta_id = "ruta:2"  # Especificamos que vamos a actualizar el registro con la clave "ruta:2"
    ruta_actualizada = {"kilometros": 16, "dificultad": "Moderada", "floraYFauna": "Flora variada", "numParticipantes": 10}
    
    # Actualizamos el registro
    baseDatosRedis.set(ruta_id, json.dumps(ruta_actualizada))
    
    # Mostramos el nuevo valor
    ruta = baseDatosRedis.get(ruta_id)
    if ruta:
        ruta_json = json.loads(ruta)
        print(f"Clave: {ruta_id} - Detalles actualizados: {ruta_json}")
    else:
        print(f"Error: No se ha encontrado la clave {ruta_id} en Redis.")
    print("---------------------------------------------------------------------------")

# ==========================
# 5. Eliminar una clave-valor y mostrar la clave y el valor eliminado
# ==========================
def eliminarRegistro():
    """Eliminar un registro con la clave 'ruta:3'."""
    print("Punto 5: Eliminar una clave-valor y mostrar la clave y el valor eliminado.")
    
    ruta_id = "ruta:3"  # Especificamos que vamos a eliminar la clave "ruta:3"
    ruta = baseDatosRedis.get(ruta_id)  # Obtenemos el valor antes de eliminarlo
    if ruta:
        ruta_json = json.loads(ruta)  # Convertimos el valor en JSON
        baseDatosRedis.delete(ruta_id)  # Eliminamos la clave
        print(f"Clave eliminada: {ruta_id} - Valor eliminado: {ruta_json}")
    else:
        print(f"Error: No se ha encontrado la clave {ruta_id} en Redis.")
    print("---------------------------------------------------------------------------")

# ==========================
# 6. Obtener y mostrar todas las claves guardadas
# ==========================
def obtenerTodasLasClaves():
    """Obtener y mostrar todas las claves almacenadas en Redis."""
    print("Punto 6: Obtener y mostrar todas las claves guardadas.")
    
    claves = baseDatosRedis.keys()  # Obtenemos todas las claves
    print("Claves registradas en Redis:")
    for clave in claves:
        print(clave.decode('utf-8'))
    print("---------------------------------------------------------------------------")

# ==========================
# 7. Obtener y mostrar todos los valores guardados
# ==========================
def obtenerTodosLosValores():
    """Obtener y mostrar todos los valores guardados en Redis."""
    print("Punto 7: Obtener y mostrar todos los valores guardados.")
    
    claves = baseDatosRedis.keys()  # Obtenemos todas las claves
    print("Valores registrados en Redis:")
    for clave in claves:
        valor = baseDatosRedis.get(clave)
        if valor:
            valor_json = json.loads(valor)
            print(f"Clave: {clave.decode('utf-8')} - Valor: {valor_json}")
    print("---------------------------------------------------------------------------")

# ==========================
# 8. Obtener y mostrar varios registros con un patrón en común usando *
# ==========================
def obtenerRegistrosConPatronAsterisco():
    """Obtener y mostrar registros que coincidan con un patrón '*'."""
    print("Punto 8: Obtener y mostrar varios registros con un patrón en común usando '*'")
    
    # Buscamos las rutas que comienzan con 'ruta:1' usando '*' como patrón
    rutas = baseDatosRedis.keys("ruta:*")
    print(f"Rutas que coinciden con el patrón 'ruta:*': {rutas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 9. Obtener y mostrar varios registros con un patrón en común usando []
# ==========================
def obtenerRegistrosConPatronCorchetes():
    """Obtener y mostrar registros que coincidan con un patrón '[1]'."""
    print("Punto 9: Obtener y mostrar varios registros con un patrón en común usando '[]'")
    
    # Buscamos las rutas que contienen un '1' en el medio de la clave usando '[]'
    rutas = baseDatosRedis.keys("ruta:[1]*")
    print(f"Rutas que coinciden con el patrón 'ruta:[1]*': {rutas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 10. Obtener y mostrar varios registros con un patrón en común usando ?
# ==========================
def obtenerRegistrosConPatronInterrogacion():
    """Obtener y mostrar registros que coincidan con un patrón '?'."""
    print("Punto 10: Obtener y mostrar varios registros con un patrón en común usando '?'")
    
    # Buscamos las rutas que contienen un '1' o '2' en la clave usando '?' como comodín
    rutas = baseDatosRedis.keys("ruta:?")
    print(f"Rutas que coinciden con el patrón 'ruta:?': {rutas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 11. Obtener y mostrar varios registros y filtrarlos por un valor en concreto
# ==========================
def obtenerRegistrosPorValor():
    """Obtener y mostrar registros filtrados por valor."""
    print("Punto 11: Obtener y mostrar varios registros y filtrarlos por un valor en concreto.")
    
    valor_filtro = "Fácil"
    rutas_filtradas = []
    for clave in baseDatosRedis.keys():
        valor = json.loads(baseDatosRedis.get(clave))
        if valor.get("dificultad") == valor_filtro:
            rutas_filtradas.append((clave, valor))
    print(f"Rutas con dificultad '{valor_filtro}': {rutas_filtradas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 12. Actualizar una serie de registros en base a un filtro
# ==========================
def actualizarRegistrosPorFiltro():
    """Actualizar registros basados en un filtro."""
    print("Punto 12: Actualizar una serie de registros en base a un filtro.")
    
    valor_filtro = "Fácil"
    print(f"Aplicando filtro: dificultad = '{valor_filtro}'")
    
    registros_actualizados = []
    for clave in baseDatosRedis.keys():
        valor = json.loads(baseDatosRedis.get(clave))
        if valor.get("dificultad") == valor_filtro:
            valor_anterior = valor.copy()  # Guardar el valor antes de la actualización
            valor["numParticipantes"] += 1
            baseDatosRedis.set(clave, json.dumps(valor))
            registros_actualizados.append((clave.decode('utf-8'), valor_anterior, valor))
            print(f"Registro actualizado: {clave.decode('utf-8')}")
    
    print("Registros actualizados:")
    for clave, valor_anterior, valor_actual in registros_actualizados:
        print(f"Clave: {clave}")
        print(f" - Antes: {valor_anterior}")
        print(f" - Después: {valor_actual}")
    print("---------------------------------------------------------------------------")

# ==========================
# ==========================
# 13. Eliminar una serie de registros en base a un filtro
# ==========================
def eliminarRegistrosPorFiltro():
    """Eliminar registros basados en un filtro."""
    print("Punto 13: Eliminar una serie de registros en base a un filtro.")
    
    valor_filtro = "Moderada"
    print(f"Aplicando filtro: dificultad = '{valor_filtro}'")

    # Mostrar registros antes de la eliminación
    print("Registros antes de la eliminación:")
    registros_antes = [(clave.decode('utf-8'), json.loads(baseDatosRedis.get(clave))) for clave in baseDatosRedis.keys()]
    for clave, valor in registros_antes:
        print(f"Clave: {clave} - Valor: {valor}")
    
    # Eliminar registros que coincidan con el filtro
    registros_eliminados = []
    for clave in baseDatosRedis.keys():
        valor = json.loads(baseDatosRedis.get(clave))
        if valor.get("dificultad") == valor_filtro:
            registros_eliminados.append((clave.decode('utf-8'), valor))
            baseDatosRedis.delete(clave)
    
    print("Registros eliminados:")
    for clave, valor in registros_eliminados:
        print(f"Clave: {clave} - Valor: {valor}")
    
    # Mostrar registros después de la eliminación
    print("Registros después de la eliminación:")
    registros_despues = [(clave.decode('utf-8'), json.loads(baseDatosRedis.get(clave))) for clave in baseDatosRedis.keys()]
    for clave, valor in registros_despues:
        print(f"Clave: {clave} - Valor: {valor}")

    print("---------------------------------------------------------------------------")

# Ejemplo de llamada a la función
eliminarRegistrosPorFiltro()


# ==========================
# 14. Crear una estructura en JSON de array de los datos que vais a almacenar
# ==========================
def crearEstructuraJSON():
    """Crear una estructura en JSON de las rutas."""
    print("Punto 14: Crear una estructura en JSON de las rutas.")
    
    rutas_json = [
        {"kilometros": 5, "dificultad": "Facil", "floraYFauna": "Flora tipica", "numParticipantes": 10},
        {"kilometros": 15, "dificultad": "Moderada", "floraYFauna": "Fauna variada", "numParticipantes": 8},
        {"kilometros": 20, "dificultad": "Dificil", "floraYFauna": "Fauna salvaje", "numParticipantes": 5},
        {"kilometros": 8, "dificultad": "Facil", "floraYFauna": "Flora abundante", "numParticipantes": 12}
    ]
    
    print("Estructura JSON creada:")
    print(json.dumps(rutas_json, indent=4))
    print("---------------------------------------------------------------------------")

# ==========================
# 15. Realizar un filtro por cada atributo de la estructura JSON anterior
# ==========================
def realizarFiltroJSON():
    """Filtrar por dificultad 'Fácil' y mostrar los resultados."""
    print("Punto 15: Filtrar por dificultad 'Fácil'.")
    
    rutas_json = [
        {"kilometros": 5, "dificultad": "Facil", "floraYFauna": "Flora tipica", "numParticipantes": 10},
        {"kilometros": 15, "dificultad": "Moderada", "floraYFauna": "Fauna variada", "numParticipantes": 8},
        {"kilometros": 20, "dificultad": "Dificil", "floraYFauna": "Fauna salvaje", "numParticipantes": 5},
        {"kilometros": 8, "dificultad": "Facil", "floraYFauna": "Flora abundante", "numParticipantes": 12}
    ]
    
    rutas_filtro = [ruta for ruta in rutas_json if ruta["dificultad"] == "Facil"]
    print("Rutas filtradas por dificultad 'Facil':")
    print(json.dumps(rutas_filtro, indent=4))
    print("---------------------------------------------------------------------------")

# ==========================
# 16. Crear una lista en Redis
# ==========================
def crearLista():
    """Crear una lista en Redis y almacenar varias rutas."""
    print("Punto 16: Crear una lista en Redis.")
    
    lista_rutas = [
        {"kilometros": 5, "dificultad": "Facil", "floraYFauna": "Flora tipica", "numParticipantes": 10},
        {"kilometros": 15, "dificultad": "Moderada", "floraYFauna": "Fauna variada", "numParticipantes": 8}
    ]
    
    for ruta in lista_rutas:
        baseDatosRedis.rpush("lista_rutas", json.dumps(ruta))  # Añadir cada ruta a la lista
    
    print(f"Lista de rutas almacenada: {lista_rutas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 17. Obtener elementos de una lista con un filtro en concreto
# ==========================
def obtenerElementosDeLista():
    """Obtener elementos de la lista de rutas con un filtro específico (dificultad 'Fácil')."""
    print("Punto 17: Obtener elementos de una lista con un filtro en concreto.")
    
    rutas_lista = baseDatosRedis.lrange("lista_rutas", 0, -1)  # Obtenemos todas las rutas en la lista
    rutas_filtradas = []
    
    for ruta in rutas_lista:
        ruta_json = json.loads(ruta)
        if ruta_json["dificultad"] == "Facil":
            rutas_filtradas.append(ruta_json)
    
    print(f"Rutas con dificultad 'Fácil': {rutas_filtradas}")
    print("---------------------------------------------------------------------------")

# ==========================
# 18. Uso de otros tipos de datos en Redis: Set y Hashes
# ==========================
def trabajarConSetsYHashes():
    """Usar un Set y un Hash en Redis."""
    print("Punto 18: Uso de Set y Hashes.")
    
    # Usamos un Set para almacenar las rutas
    baseDatosRedis.sadd("set_rutas", "ruta:1", "ruta:2", "ruta:4")
    
    # Usamos un Hash para almacenar los detalles de una ruta
    ruta_hash = {
        "kilometros": 15,
        "dificultad": "Moderada",
        "floraYFauna": "Fauna variada",
        "numParticipantes": 8
    }
    baseDatosRedis.hset("hash_ruta_2", mapping=ruta_hash)
    
    # Obtener el Set y Hash
    set_rutas = baseDatosRedis.smembers("set_rutas")
    ruta_hash_obtenida = baseDatosRedis.hgetall("hash_ruta_2")
    
    print(f"Set de rutas: {[elem.decode('utf-8') for elem in set_rutas]}")
    print(f"Hash de la ruta 2: { {campo.decode('utf-8'): valor.decode('utf-8') for campo, valor in ruta_hash_obtenida.items()} }")
    print("---------------------------------------------------------------------------")

# Ejecutar todas las funcionalidades
def ejecutarFuncionalidades():
    """Ejecutar todas las funcionalidades implementadas."""
    print("Ejecución de todas las funcionalidades:")
    crearRegistros()
    obtenerNumeroDeClaves()
    obtenerRegistro()
    actualizarRegistro()
    eliminarRegistro()
    obtenerTodasLasClaves()
    obtenerTodosLosValores()
    obtenerRegistrosConPatronAsterisco()
    obtenerRegistrosConPatronCorchetes()
    obtenerRegistrosConPatronInterrogacion()
    obtenerRegistrosPorValor()
    actualizarRegistrosPorFiltro()
    eliminarRegistrosPorFiltro()
    crearEstructuraJSON()
    realizarFiltroJSON()
    crearLista()
    obtenerElementosDeLista()
    trabajarConSetsYHashes()

# Llamar a la función para ejecutar las funcionalidades
ejecutarFuncionalidades()
