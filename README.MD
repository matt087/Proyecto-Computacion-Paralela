# Proyecto de Computación Paralela: Análisis de Transacciones Retail 

Este repositorio contiene la implementación del proyecto final para la asignatura de Computación Paralela. El objetivo es procesar un dataset masivo de transacciones comerciales utilizando **Apache Spark (PySpark)** bajo un modelo de programación distribuida.

---

## 1. Requisitos Previos

Para ejecutar este proyecto correctamente en un entorno local, asegúrese de tener instalado el siguiente software:

* **Python:** Versión 3.8 o superior.
* **Java Development Kit (JDK):** Versión 8 o 11.
* **Apache Spark:** Librerías de PySpark.
* **Sistema Operativo:** Windows, Linux o macOS.

## 2. Descripción del Entorno y Configuración Spark

Para la ejecución de este avance, se ha configurado un entorno de desarrollo **pseudo-distribuido** que simula el comportamiento de un clúster utilizando los recursos locales. Esta configuración permite validar la lógica de paralelismo y el flujo de datos antes de un despliegue en infraestructura real.

### Detalles de la Configuración

* **Versión de Apache Spark:** `4.1.1`
* **Lenguaje de Programación:** Python 3.11.9
* **Entorno de Ejecución:** `master("local[*]")`
* **Gestión de Memoria:** `spark.executor.memory = "4g"`
* **Ajuste de Particiones:** `spark.sql.shuffle.partitions = "8"`

## 3. Instrucciones de Ejecución

Siga estos pasos detallados para poner en marcha el proyecto en su entorno local.

### Paso 1: Clonar el Repositorio
Descargue el código fuente y el dataset directamente desde el repositorio oficial del proyecto. Abra su terminal y ejecute:

```bash
git clone https://github.com/matt087/Proyecto-Computacion-Paralela  
cd Proyecto-Computacion-Paralela
```
### 2. Instalación de dependencias
Ejecutar el siguiente comando:

```bash
pip install -r requirements.txt
```

## Paso 3: Ejecución del Procesamiento
El dataset (transactions.csv) ya se encuentra incluido en la carpeta data/, por lo que no requiere configuración adicional. Ejecute el script principal con el siguiente comando:

```bash
python src/main.py
```
### Paso 4: Verificación de Resultados
Al finalizar la ejecución, revise la carpeta `output/` generada automáticamente en la raíz del proyecto. Allí encontrará dos subcarpetas con los resultados persistidos en formato CSV:

* **`ventas_por_tienda_csv/`**: Contiene el archivo con el cálculo de ingresos totales por sucursal.
* **`anomalias_detectadas/`**: Contiene el reporte detallado de los días donde se detectaron devoluciones inusuales (superiores al umbral estadístico).

> **Nota:** El script está configurado para sobrescribir resultados anteriores (`mode("overwrite")`), por lo que no es necesario eliminar manualmente esta carpeta antes de una nueva ejecución.
