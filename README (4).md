# ETL Ventas Colombia con PySpark

Prueba técnica de Ingeniería de Datos (ETL). El proceso limpia el archivo `ventas_colombia_sucio.csv` aplicando las reglas de calidad del enunciado y responde 10 preguntas sobre los pedidos con estado **Entregado**.

Autores: Juan José Montezuma y Miguel Ángel Mendoza

## Estructura

```
.
├── etl_ventas_colombia.py     # Script ETL + respuestas
├── ventas_colombia_sucio.csv  # Dataset de entrada
└── README.md
```

## Requisitos

- Python 3.9 o superior
- Java 17 o 21 (necesario para Spark)
- Librería `pyspark`

Verifica la instalación:

```bash
python --version
java -version
```

Si no tienes Java:

- **Windows:** instala Temurin 17 desde https://adoptium.net
- **Ubuntu/Debian:** `sudo apt install openjdk-17-jre`
- **macOS:** `brew install openjdk@17`

## Instalación

```bash
pip install pyspark
```

## Ejecución

1. Deja `etl_ventas_colombia.py` y `ventas_colombia_sucio.csv` en la **misma carpeta**.
2. Abre una terminal en esa carpeta.
3. Ejecuta:

```bash
python etl_ventas_colombia.py
```

## Qué hace el script

1. **Lectura:** carga el CSV como texto (UTF-8).
2. **Normalización de texto:** elimina espacios y unifica mayúsculas, tildes y variantes de escritura en `city` y `product` (por ejemplo `" bogota "` → `Bogotá`, `Laptop Pro14` → `Laptop Pro 14`). No se crean registros nuevos.
3. **Tipos:** convierte fechas y columnas numéricas. Los valores inválidos pasan a `NULL` (`spark.sql.ansi.enabled=false`).
4. **Reglas de calidad:**
   - `order_id` no nulo y un solo registro por pedido.
   - `order_date` válida y del año 2025.
   - `city` y `product` no nulos.
   - `quantity` entre 1 y 20.
   - `unit_price_cop` mayor que 0 y menor que 10.000.000.
   - `customer_age` entre 18 y 100.
   - `shipping_days` entre 1 y 15.
   - `returned_qty` entre 0 y `quantity`.
5. **Valor de venta:** `sales_value = quantity * unit_price_cop`.
6. **Consultas:** responde las preguntas 1 a 10 sobre pedidos Entregados (la pregunta 2 usa pedidos Devueltos).

## Resultados esperados

```
P1  Smartphone X        2245 unidades
P2  1.7034...
P3  Bogotá              2271 pedidos
P4  Computadores        5653948750
P5  Web                 2347 pedidos
P6  1518048.17...
P7  Cundinamarca        3821 unidades
P8  Contraentrega       2395 pedidos
P9  3.1187...
P10 Retail              4816329445
```

Respuestas: **1-B, 2-B, 3-A, 4-C, 5-B, 6-A, 7-D, 8-A, 9-A, 10-C**

## Solución de problemas

| Problema | Solución |
|---|---|
| `JAVA_HOME is not set` o `Java gateway exited` | Instala Java y verifica que `java -version` funcione en la terminal. |
| `Path does not exist: ventas_colombia_sucio.csv` | Ejecuta el script desde la carpeta que contiene el CSV, o usa la ruta completa en `spark.read.csv(...)`. |
| Error de `winutils` o `HADOOP_HOME` en Windows | Normalmente es solo una advertencia. Si el script se detiene, ejecútalo en WSL o Google Colab. |
| Mensajes "proceso terminado" al final (Windows) | Son normales: Spark cerrando sus procesos. |
| `CAST_INVALID_INPUT` | Verifica que el script tenga `spark.sql.ansi.enabled=false` en la creación de la sesión. |

## Alternativa: Google Colab

```python
!pip install pyspark
# Sube etl_ventas_colombia.py y ventas_colombia_sucio.csv al panel de archivos
!python etl_ventas_colombia.py
```
