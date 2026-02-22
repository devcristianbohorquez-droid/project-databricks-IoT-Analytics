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
## ⚙️ Requisitos Previos
 
### ☁️ Plataforma y Accesos

- Cuenta de Azure con permisos para crear y administrar recursos
- Azure Databricks con workspace operativo
- Cluster activo en Databricks
- Nombre sugerido: Cluster1
- Runtime compatible con Spark 3.x


### 📦 Almacenamiento

- Azure Data Lake Storage Gen2 configurado
- Contenedores separados por capa:
    - raw, bronze, silver, golden
- External Locations y Storage Credentials correctamente definidos


### 🐙 Control de Versiones
GitHub
- Repositorio inicializado
- Permisos de administrador para configurar ramas y CI/CD (opcional)


###  📊 Visualización y Análisis
- Power BI Desktop 
Para consumo de KPIs desde la capa Golden

## 🚀 Instalación y Configuración


<div align="center">

✅ **¡Configuración completa!**

</div>

---


<img width="1156" height="386" alt="image" src="https://github.com/user-attachments/assets/6d6bec76-ac4f-43b0-9e91-269c9bde385b" />

---




## 👤 Autor

<div align="center">

### Cristian Bohorquez Rodriguez

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/cristian-bohorquez-b02b9820a/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/devcristianbohorquez-droid)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:cristian.bohorquez.rodriguez@gmail.com)

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

</div>ad

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

<div align="center">

**Proyecto**: Data Engineering - Arquitectura Medallion  
**Tecnología**: Azure Databricks + Delta Lake + CI/CD  
**Última actualización**: 2026


</div>
