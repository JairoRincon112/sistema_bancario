# Reporte Técnico: Control de Concurrencia en Sistema Bancario

**Autores:**  
- Jairo Andrés Rincón Blanco  
- Andrés Camilo Cuvides Ortega  

**Asignatura:** Base de Datos  
**Docente:** Hely Suárez  
**Institución:** FESC – Cúcuta, 2025  

---

## Introducción

Este documento resume la implementación y evaluación de **técnicas de control de concurrencia** en un sistema bancario desarrollado con **PostgreSQL 16**.  
El objetivo principal fue demostrar los **problemas clásicos de concurrencia**:

- Lectura sucia  
- Lectura no repetible  
- Fenómeno fantasma  

Posteriormente, se implementaron tres enfoques de control de concurrencia:  
**Bloqueo en Dos Fases (2PL)**, **Ordenamiento por Timestamps (TS)** y **Control Optimista (OCC)**.

---

## Técnicas Implementadas

### Bloqueo en Dos Fases (2PL)
Asegura la consistencia bloqueando los recursos antes de modificarlos y liberándolos solo al finalizar la transacción.  
Evita inconsistencias, pero puede generar **interbloqueos (deadlocks)**.

### Ordenamiento por Marcas de Tiempo (Timestamp Ordering)
Asigna una marca de tiempo a cada transacción para definir su orden lógico.  
Evita bloqueos, aunque puede provocar **abortos** cuando ocurren conflictos.

### Control Optimista (OCC)
Permite ejecutar transacciones sin bloqueos y valida versiones antes de confirmar.  
Minimiza tiempos de espera, pero puede generar **abortos** si hay conflictos en la fase de validación.

---

## Resultados y Métricas Simuladas

- **OCC** ofrece el **mejor rendimiento** en escenarios con baja contención, al no usar bloqueos.  
- En **OCC**, los **abortos aumentan** cuando las transacciones acceden simultáneamente a los mismos recursos.  
- **2PL** garantiza la **mayor consistencia**, pero con **más latencia** debido a los bloqueos.  
- **Timestamp Ordering** logra un **balance intermedio**, con menos abortos que OCC, aunque con un rendimiento ligeramente menor.

---

## Conclusiones

Cada técnica presenta ventajas y limitaciones según el contexto de uso:

| Técnica | Ideal Para | Ventajas | Desventajas |
|----------|-------------|-----------|--------------|
| **2PL** | Sistemas financieros | Alta consistencia | Mayor latencia y posibles deadlocks |
| **Timestamp Ordering** | Sistemas distribuidos | Buen balance entre rendimiento y abortos | Abortos moderados |
| **OCC** | Aplicaciones web con muchas lecturas | Alto rendimiento y baja espera | Abortos frecuentes en alta contención |

En conclusión, el sistema bancario desarrollado demuestra los **principios teóricos del control de concurrencia** y evidencia los **trade-offs entre rendimiento y consistencia** que enfrentan los sistemas transaccionales modernos.

---
