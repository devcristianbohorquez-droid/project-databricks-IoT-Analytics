<div align="center">

# 🏗️ IoT Tower Maintenance ETL Pipeline
### Arquitectura Medallion en Azure Databricks

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Azure Data Lake](https://img.shields.io/badge/Azure_Data_Lake-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)

*Pipeline automatizado de datos IoT para monitoreo y mantenimiento predictivo de torres industriales, implementando arquitectura Medallion (Bronze, Silver y Gold) con almacenamiento en Delta Lake y despliegue continuo.*

</div>
---

## 🎯 Descripción

Pipeline de ingeniería de datos diseñado bajo la Arquitectura Medallion (Bronze → Silver → Golden) implementado en Azure Databricks sobre Delta Lake, utilizando Unity Catalog para gobernanza centralizada.

El proyecto simula un entorno de Industrial IoT Predictive Maintenance, procesando eventos de sensores en rigs industriales:

Temperatura

Presión

Vibración

Flujo

Se aplican métricas estadísticas avanzadas (moving average y desviación estándar) para detectar anomalías operativas en tiempo casi real.











🏗️ Arquitectura General

🔄 Flujo de Datos


```mermaid
flowchart LR
    A[Raw CSV - IoT Sensors] --> B[Bronze Layer - Delta]
    B --> C[Silver Layer - Clean + Anomaly Detection]
    C --> D[Golden Layer - Daily KPIs]
    D --> E[BI / Dashboards / Analytics]
```

🏛️ Arquitectura Física en ADLS

```mermaid
flowchart TD
    subgraph ADLS Gen2
        R[raw container]
        B[bronze container]
        S[silver container]
        G[golden container]
    end

    R --> B
    B --> S
    S --> G
```
<img width="1031" height="361" alt="image" src="https://github.com/user-attachments/assets/01141b64-b0e7-4fcc-a280-1ae8e4c8d558" />

Cada capa está registrada en Unity Catalog con su respectiva External Location y control de credenciales.

<table>
<tr>
<td width="33%" valign="top">

### 🥉 Bronze Layer  
**Tabla:** `sensor_events`

**Rol:** Ingesta estructurada y trazable  

**Características:**
- ✔ Esquema explícito y tipado
- ✔ Timestamp técnico de ingesta
- ✔ Particionado por fecha
- ✔ Sin reglas de negocio
- ✔ Preservación completa del dato original

**Propósito:**  
Zona de aterrizaje confiable que actúa como *single source of truth* para reprocesos.

</td>

<td width="33%" valign="top">

### 🥈 Silver Layer  
**Tabla:** `sensor_events_clean`

**Rol:** Calidad, enriquecimiento y lógica analítica  

**Características:**
- ✔ Limpieza y validación de datos
- ✔ Eliminación de valores inválidos
- ✔ Moving Average por ventana temporal
- ✔ Desviación estándar por sensor
- ✔ Clasificación `ANOMALY / NORMAL`

**Propósito:**  
Datos listos para análisis avanzado y detección de comportamiento anómalo.

</td>

<td width="33%" valign="top">

### 🥇 Golden Layer  
**Tabla:** `rig_daily_summary`

**Rol:** Consumo analítico y KPIs de negocio  

**Características:**
- ✔ Agregaciones diarias
- ✔ Promedios, máximos y mínimos
- ✔ Conteo de eventos anómalos
- ✔ Optimizado para BI y dashboards
- ✔ Baja latencia de consulta

**Propósito:**  
Vista ejecutiva para monitoreo operativo y toma de decisiones.

</td>
</tr>
</table>



🧠 Detección de Anomalías

value > moving_avg + 3 * std_dev

Donde:

- moving_avg = media móvil de 10 eventos

- std_dev = desviación estándar en la ventana

- Regla de 3 sigmas para detección outliers

Esto permite identificar comportamiento anómalo por:

- rig_id

- sensor_id

- orden temporal

📊 Modelo de Datos

```mermaid
erDiagram
    SENSOR_EVENTS {
        string event_id
        string rig_id
        string sensor_id
        string sensor_type
        double value
        timestamp event_timestamp
        date ingestion_date
    }

    SENSOR_EVENTS_CLEAN {
        double moving_avg
        double std_dev
        string anomaly_flag
    }

    RIG_DAILY_SUMMARY {
        string rig_id
        string sensor_type
        date ingestion_date
        double avg_value
        double max_value
        double min_value
        int anomaly_count
    }

    SENSOR_EVENTS ||--|| SENSOR_EVENTS_CLEAN : transforms
    SENSOR_EVENTS_CLEAN ||--o{ RIG_DAILY_SUMMARY : aggregates
```
📁 Estructura del Proyecto
iot-predictive-maintenance/
iot-predictive-maintenance/
├── README.md
├── environment/
│   ├── 00_create_catalog_and_schemas.sql
│   ├── 01_create_external_locations.sql
│   └── 02_create_tables.sql
├── bronze/
│   └── 10_raw_to_bronze_sensor_events.py
├── silver/
│   └── 20_bronze_to_silver_sensor_events.py
├── golden/
│   └── 30_silver_to_golden_rig_daily_summary.py
├── jobs/
│   ├── raw_to_bronze_job.json
│   ├── bronze_to_silver_job.json
│   └── silver_to_golden_job.json
├── workflows/
│   └── iot_predictive_maintenance_workflow.json
├── data/
│   └── sample/
│       └── sensor_events_sample.csv
└── utils/
    ├── schemas.py
    └── constants.py
🔐 Gobernanza y Seguridad

Implementado con:

 - Unity Catalog

 - xternal Locations por capa

 - Storage Credentials centralizadas

 - Separación Infraestructura vs Procesamiento

Principio aplicado:

Infraestructura (DDL) ≠ Procesamiento (DML)

Los jobs no crean tablas.
Solo insertan datos.

Arquitectura enterprise real.


Pipeline compuesto por 3 Jobs dependientes:

```mermaid
flowchart LR
    A[Job 1: Raw → Bronze]
    B[Job 2: Bronze → Silver]
    C[Job 3: Silver → Golden]

    A --> B --> C

```
⚙️ Requisitos Previos


☁️ Plataforma y Accesos

- Cuenta de Azure con permisos para crear y administrar recursos

-Azure Databricks con workspace operativo

-Cluster activo en Databricks

 -Nombre sugerido: Cluster1

 -Runtime compatible con Spark 3.x

📦 Almacenamiento

- Azure Data Lake Storage Gen2 configurado

- Contenedores separados por capa:

    - raw, bronze, silver, golden

- External Locations y Storage Credentials correctamente definidos

🐙 Control de Versiones

GitHub

- Repositorio inicializado

- Permisos de administrador para configurar ramas y CI/CD (opcional)

📊 Visualización y Análisis

- Power BI Desktop 
Para consumo de KPIs desde la capa Golden



 ⚙️Configurado en Databricks Workflows con:

 - Control de concurrencia

 - Logs detallados

 - Reintentos automáticos

 - Parametrización vía JSON
   



🧪 Validaciones Implementadas
- Esquema explícito
- Filtrado de valores negativos
- Columnas técnicas de auditoría
- Particionado por fecha
- Detección estadística de anomalías

# 📈 Beneficios de la Arquitectura
- Separación clara de responsabilidades
- Escalabilidad horizontal
- ACID transactions con Delta Lake
- Preparado para integración con BI
- Base sólida para evolucionar a streaming
- Gobernanza enterprise-ready

# 🎯 Competencias Demostradas

- Diseño de arquitectura Medallion
- Implementación en entorno cloud Azure
- Gobernanza con Unity Catalog
- Procesamiento distribuido con Spark
- Modelado analítico para mantenimiento predictivo
- Construcción de pipelines productivos



**Características**:
- ✅ Datos tal como vienen de origen
- ✅ Timestamp de ingesta
- ✅ Preservación histórica
- ✅ Sin validaciones

</td>
<td width="33%" valign="top">

#### 🥈 Silver Layer
**Propósito**: Modelo dimensional

**Tablas**:
- category_sales
- product_sales
- store_sales
- store_warranty_status
- warranty_products

**Características**:
- ✅ Star Schema
- ✅ Datos normalizados
- ✅ Validaciones completas

</td>
<td width="33%" valign="top">

#### 🥇 Gold Layer
**Propósito**: Analytics-ready

**Tablas**:
- kpi_category_sales        : Monto total en ventas agrupado por categoría y año
- kpi_product_sales         : Monto total en ventas agrupado por producto y año
- kpi_store_sales           : Monto total en ventas agrupado por tienda y año
- kpi_store_warranty_status : Total de reclamos por tienda en los diferentes estatus pivot
- kpi_product_warranty      : Productos con mayor reclamos post venta (garantía)

**Características**:
- ✅ Pre-agregados
- ✅ Optimizado para BI
- ✅ Performance máximo
- ✅ Actualizaciones automáticas

</td>
</tr>
</table>

---

## 📁 Estructura del Proyecto

iot-predictive-maintenance/
│
├── README.md
│
├── environment/
│   ├── 00_create_catalog_and_schemas.sql
│   ├── 01_create_external_locations.sql
│   └── 02_create_tables.sql
│
├── bronze/
│   └── 10_raw_to_bronze_sensor_events.py
│
├── silver/
│   └── 20_bronze_to_silver_sensor_events.py
│
├── golden/
│   └── 30_silver_to_golden_rig_daily_summary.py
│
├── jobs/
│   ├── raw_to_bronze_job.json
│   ├── bronze_to_silver_job.json
│   └── silver_to_golden_job.json
│
├── workflows/
│   └── iot_predictive_maintenance_workflow.json
│
├── data/
│   └── sample/
│       └── sensor_events_sample.csv
│
└── utils/
    ├── schemas.py
    └── constants.py


---

## 🛠️ Tecnologías

<div align="center">

| Tecnología | Propósito |
|:----------:|:----------|
| ![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) | Motor de procesamiento distribuido Spark |
| ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white) | Storage layer con ACID transactions |
| ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white) | Framework de transformación de datos |
| ![ADLS](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white) | Data Lake para almacenamiento persistente |
| ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white) | Automatización CI/CD |
| ![Databricks Dashboards](https://img.shields.io/badge/Databricks Dashboards-F2C81?style=for-the-badge&logo=databricks&logoColor=black) |  Visualización |

</div>

---

## ⚙️ Requisitos Previos

- ☁️ Cuenta de Azure con acceso a Databricks
- 💻 Workspace de Databricks configurado
- 🖥️ Cluster activo (nombre: Cluster1)
- 🐙 Cuenta de GitHub con permisos de administrador
- 📦 Azure Data Lake Storage Gen2 configurado
- 📊 Power BI Desktop (opcional para visualización)

---

## 🚀 Instalación y Configuración

### 1️⃣ Clonar el Repositorio

bash
git clone https://github.com/guaru/project-databricks.git
cd project-databricks


### 2️⃣ Configurar Databricks Token

1. Ir a Databricks Workspace
2. **User Settings** → **Developer** → **Access Tokens**
3. Click en **Generate New Token**
4. Configurar:
   - **Comment**: GitHub CI/CD
   - **Lifetime**: 90 days
5. ⚠️ Copiar y guardar el token

### 3️⃣ Configurar GitHub Secrets

En tu repositorio: **Settings** → **Secrets and variables** → **Actions**

| Secret Name | Valor Ejemplo |
|------------|---------------|
| DATABRICKS_HOST | https://adb-xxxxx.azuredatabricks.net |
| DATABRICKS_TOKEN | dapi_xxxxxxxxxxxxxxxx |

### 4️⃣ Verificar Storage Configuration

python
storage_path = "abfss://raw@adlsprojectsmartdata.dfs.core.windows.net"


<div align="center">

✅ **¡Configuración completa!**

</div>

---

## 💻 Uso

### 🔄 Despliegue Automático (Recomendado)

bash
git add .
git commit -m "✨ feat: mejoras en pipeline"
git push origin master


**GitHub Actions ejecutará**:
- 📤 Deploy de notebooks a /Production/ETL-APPLE
- 🔧 Creación del workflow WF_PROD_ETL_APPLE_SALES
- ▶️ Ejecución completa:  Bronze → Silver → Gold
- 📧 Notificaciones de resultados

### 🖱️ Despliegue Manual desde GitHub

1. Ir al tab **Actions** en GitHub
2. Seleccionar **Deploy ETL Apple Sales And Warranty**
3. Click en **Run workflow**
4. Seleccionar rama main
5. Click en **Run workflow**

### 🔧 Ejecución Local en Databricks

Navegar a /Production/ETL-APPLE y ejecutar en orden:

- Enviroment preparation.py         → Crear esquema
- ingest_catalogs.py                → Bronze Layer
- ingest_sales.py                   → Bronze Layer
- ingest_warranty.py                → Bronze Layer
- transform_sales.py                → Silver Layer
- transform_warranty.py             → Silver Layer
- load_sales.py                     → Gold Layer
- load_warranty.py                  → Gold Layer


---


## 🔄 CI/CD

### Pipeline de GitHub Actions

yaml
Workflow: Deploy ETL Apple Sales And Warranty
├── Deploy notebooks → /Production/ETL-APPLE
├── Eliminar workflow antiguo (si existe)
├── Buscar cluster configurado
├── Crear nuevo workflow con 4 tareas
├── Ejecutar pipeline automáticamente
└── Monitorear y notificar resultados


### 🔄  Workflow Databricks
![Texto descriptivo](CICD_ETL_APPLE.png)
⏰ Schedule: Diario 8:00 AM (Lima)
⏱️ Timeout total: 4 horas
 🔒 Max concurrent runs: 1
⏰ Notificaciones: 
      success: isc.ventura@gmail.com
      failed:  isc.ventura@gmail.com


---

## 📈 Dashboards
https://github.com/guaru/project-databricks/tree/dev/dashboards

## 🔍 Monitoreo

### En Databricks

**Workflows**:
- Ir a **Workflows** en el menú lateral
- Buscar ETL_PROD_APPLE_SALES
- Ver historial de ejecuciones

**Logs por Tarea**:
- Click en una ejecución específica
- Click en cada tarea para ver logs detallados
- Revisar stdout/stderr en caso de errores

### En GitHub Actions

- Tab **Actions** del repositorio
- Ver historial de workflows
- Click en ejecución específica para detalles
- Revisar logs de cada step

---

## 👤 Autor

<div align="center">

### Cristian Bohorquez Rodriguez

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/cristian-bohorquez-b02b9820a/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/devcristianbohorquez-droid)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)]
(cristian.bohorquez.rodriguez@gmail.com)

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

</div>

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

<div align="center">

**Proyecto**: Data Engineering - Arquitectura Medallion  
**Tecnología**: Azure Databricks + Delta Lake + CI/CD  
**Última actualización**: 2025


</div>
