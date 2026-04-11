

Proyecto 3 — Construcción de un Data Lakehouse para STEDI

## Descripción

El proyecto implementa un pipeline ETL que procesa datos de sensores IoT (acelerómetro y step trainer) junto con información de clientes, aplicando una arquitectura de lakehouse con tres zonas: Zona cruda, zona de confianza o trusted y la zona curada
El objetivo es construir un dataset curado para entrenar un modelo de Machine Learning que detecte pasos en tiempo real a partir de los datos del acelerómetro.

## Arquitectura

Landing                             Trusted                              Curated 
┌──────────────────┐       ┌──────────────────────────┐       ┌─────────────────────────────┐
│ raw_customers    │──────>│ customers_trusted        │──────>│ customers_curated           │
│ raw_accelerometer│──────>│ accelerometer_trusted    │       │ machine_learning_curated    │
│ raw_step_trainer │──────>│ step_trainer_trusted     │──────>│                             │
└──────────────────┘       └──────────────────────────┘       └─────────────────────────────┘

## Estructura del proyecto

```
jsedan_STEDI/
├── landing_sql/                          # DDL para tablas en zona landing
│   ├── accelerometer_landing.sql         # Tabla raw_accelerometer
│   ├── customers_landing.sql             # Tabla raw_customers
│   └── step_trainer_landing.sql          # Tabla raw_step_trainer
├── scripts/                              # Glue Jobs (ETL)
│   ├── job_customers_trusted.py          # Landing → Trusted (filtra por consentimiento)
│   ├── job_accelerometer_trusted.py      # Landing → Trusted (cruza con customers_trusted)
│   ├── job_step_trainer_trusted.py       # Landing → Trusted (cruza con customers_curated)
│   ├── job_customers_curated.py          # Trusted → Curated (cruza con accelerometer)
│   └── job_machine_learning_curated.py   # Trusted → Curated (join step_trainer + accelerometer)
├── querys_screenshots/                   # Evidencias de validación
│   ├── accelerometer_landing.png
│   ├── customer_landing.png
│   ├── step_trainer_landing.png
│   ├── Validaciones_tablas.png
│   └── Validaciones.png
└── README.md
```

## ETL

#### 1. Landing → Trusted

- **customers_trusted**: Filtra solo los clientes que permitieron usar sus datos
- **accelerometer_trusted**: A través del email revisa quienes son los usuarios que deben quedar aquí
- **step_trainer_trusted**: Registros de los usuarios que permitieron el uso de sus datos

#### 2. Trusted → Curated

- **customers_curated**: Clientes unicos que permiten usar sus datos
- **machine_learning_curated**: registros que coinciden en fecha con los que se presentaron en el accelerometer


## Tablas del catálogo

| Tabla | Zona | Fuente |
|-------|------|--------|
| `raw_customers` | Landing | `s3://jsedan-files/customer/landing/` |
| `raw_accelerometer` | Landing | `s3://jsedan-files/accelerometer/landing/` |
| `raw_step_trainer` | Landing | `s3://jsedan-files/step_trainer/landing/` |
| `customers_trusted` | Trusted | `s3://jsedan-files/trusted/customers/` |
| `accelerometer_trusted_i` | Trusted | `s3://jsedan-files/trusted/accelerometer/` |
| `step_trainer_trusted` | Trusted | `s3://jsedan-files/trusted/step_trainer/` |
| `customers_curated` | Curated | `s3://jsedan-files/curated/customers/` |
| `machine_learning_curated` | Curated | `s3://jsedan-files/curated/machine_learning/` |
