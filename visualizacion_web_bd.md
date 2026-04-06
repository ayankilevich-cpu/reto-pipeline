# Plan: Visualizaciones Dinámicas Web desde Base de Datos

> **Estado:** Planificación  
> **Fecha inicio:** 27 de enero de 2026  
> **Última actualización:** 27 de enero de 2026

---

## Objetivo

Generar visualizaciones dinámicas a partir de datos en Google Sheets (con futura migración a PostgreSQL) para mostrar en el sitio web de la fundación.

---

## Arquitectura General

```
[Google Sheets / PostgreSQL] → [API/Backend] → [Librería de Visualización] → [Sitio Web]
```

---

## Opciones de Implementación

### Opción 1: Solución Simple (Google Sheets → Web directamente)
- Publicar datos de Sheets como JSON
- Conectar desde frontend con JavaScript
- Librerías: Chart.js, ApexCharts, Plotly.js
- **Pros:** Rápido, sin backend propio
- **Contras:** Limitaciones de seguridad, rate limits

### Opción 2: Solución Intermedia (Con Backend Ligero) ⭐ RECOMENDADA
- Backend en Python (FastAPI/Flask)
- Conecta a Google Sheets API (ahora) y PostgreSQL (futuro)
- Endpoints específicos para visualizaciones
- Frontend con Chart.js, ECharts, o D3.js
- **Pros:** Mayor control, fácil migración, mejor performance
- **Contras:** Requiere hosting del backend

### Opción 3: Herramientas No-Code/Low-Code
- Google Looker Studio (ex Data Studio)
- Metabase (open source)
- Retool
- **Pros:** Sin código, rápido
- **Contras:** Menor personalización

### Opción 4: Solución Profesional/Escalable
- PostgreSQL + Backend API REST/GraphQL
- Dashboard con Recharts, Vue-ECharts, D3.js
- O framework como Apache Superset
- **Pros:** Máxima flexibilidad
- **Contras:** Mayor complejidad

---

## Progresión Recomendada

```
Fase 1: Backend simple (FastAPI) + Google Sheets API + Chart.js
    ↓
Fase 2: Migrar conexión de Sheets a PostgreSQL (cambio mínimo)
    ↓
Fase 3: Agregar más visualizaciones según necesidad
```

---

## Preguntas para el Equipo de Sistemas

### Tecnología del Sitio Web
- [ ] ¿En qué tecnología/framework está construido? (WordPress, React, Vue, HTML estático)
- [ ] ¿Usan algún CMS (gestor de contenidos)?
- [ ] ¿Hay un sistema de templates/plantillas?

### Hosting e Infraestructura
- [ ] ¿Dónde está alojado el sitio? (servidor propio, AWS, hosting compartido)
- [ ] ¿Tienen capacidad para agregar servicios adicionales? (contenedores, apps Python/Node)
- [ ] ¿Hay algún servidor de aplicaciones disponible?
- [ ] ¿Usan algún servicio de nube? (Google Cloud, AWS, Azure)

### Acceso y Permisos
- [ ] ¿Puedo tener acceso al repositorio de código?
- [ ] ¿Hay un entorno de desarrollo/staging para probar?
- [ ] ¿Quién aprueba cambios en el sitio?
- [ ] ¿Cómo es el flujo de deploy? (Git, FTP, panel de control)

### Seguridad y Políticas
- [ ] ¿Hay restricciones de CORS configuradas?
- [ ] ¿Se permite cargar scripts externos (CDN)?
- [ ] ¿Hay Content Security Policy (CSP)?
- [ ] ¿Requieren revisión de seguridad para nuevo código?

### Capacidades Actuales
- [ ] ¿Ya tienen alguna visualización o gráfico en el sitio?
- [ ] ¿Usan alguna librería JavaScript actualmente? (jQuery, etc.)
- [ ] ¿El sitio tiene backend propio o es solo frontend?
- [ ] ¿Hay alguna API interna ya implementada?

### Base de Datos y Datos
- [ ] ¿Tienen ya una base de datos en el servidor? (MySQL, PostgreSQL)
- [ ] ¿Hay planes de migrar a PostgreSQL? ¿Cuándo?
- [ ] ¿Dónde se alojaría la base de datos?

### Integración y Embebido
- [ ] ¿Se pueden embeber iframes en el sitio?
- [ ] ¿Puedo agregar código JavaScript personalizado a páginas específicas?
- [ ] ¿Hay una sección del sitio destinada a estadísticas/datos?

---

## Respuestas del Equipo de Sistemas

> _Completar esta sección cuando respondan_

### Tecnología
- Framework/CMS: 
- Templates: 

### Hosting
- Ubicación: 
- Capacidad adicional: 
- Servicios de nube: 

### Acceso
- Repositorio: 
- Entorno staging: 
- Proceso deploy: 

### Seguridad
- CORS: 
- Scripts externos: 
- CSP: 

### Capacidades
- Visualizaciones existentes: 
- Librerías JS: 
- Backend existente: 

### Base de Datos
- BD actual: 
- Plan PostgreSQL: 

---

## Flujo de Trabajo de Desarrollo

```
FASE 1: DESARROLLO LOCAL (en tu ordenador)
──────────────────────────────────────────
   Tu PC
   ├── Escribes el código
   ├── Pruebas con datos reales de Sheets
   ├── Ves las visualizaciones en localhost
   └── Iteras hasta que funcione bien


FASE 2: PREPARACIÓN PARA PRODUCCIÓN
──────────────────────────────────────────
   Tu PC
   ├── Documentas requisitos (dependencias, variables)
   ├── Creas archivos de configuración
   └── Empaquetas el proyecto


FASE 3: DESPLIEGUE (con ayuda de Sistemas)
──────────────────────────────────────────
   Servidor de la Fundación
   ├── Subes tu código al servidor
   ├── Configuras variables de entorno (credenciales)
   ├── El código corre 24/7 en el servidor
   └── El sitio web muestra las visualizaciones
```

---

## Componentes del Proyecto

| Componente | Dónde se desarrolla | Dónde se ejecuta |
|------------|--------------------|-----------------------------|
| **Backend/API** (Python) | Tu ordenador local | Servidor de la fundación |
| **Frontend** (JS + gráficos) | Tu ordenador local | Navegador del visitante |

---

## Opciones de Hosting para Backend

| Opción | Descripción | Complejidad |
|--------|-------------|-------------|
| Servidor propio de la fundación | Sistemas lo configura | Media |
| Heroku / Railway / Render | Plataformas gratuitas/baratas | Baja |
| Google Cloud Run | Si ya usan Google (Sheets) | Media |
| Vercel / Netlify | Solo frontend + funciones | Baja |

---

## Próximos Pasos

1. [ ] Enviar preguntas al equipo de sistemas
2. [ ] Definir qué visualizaciones se necesitan (tipos de gráficos)
3. [ ] Preparar datos en Sheets (estructura limpia)
4. [ ] Crear boceto/mockup de las visualizaciones
5. [ ] Recibir respuestas de sistemas
6. [ ] Definir arquitectura final según infraestructura disponible
7. [ ] Comenzar desarrollo

---

## Visualizaciones Planificadas

> _Completar con las visualizaciones específicas que se necesitan_

| Visualización | Tipo de Gráfico | Datos Fuente | Prioridad |
|---------------|-----------------|--------------|-----------|
| Ejemplo: Donaciones por mes | Barras | Hoja "Donaciones" | Alta |
| | | | |
| | | | |

---

## Notas Adicionales

_Espacio para agregar notas durante el desarrollo del proyecto_

---

## Referencias

- [Chart.js](https://www.chartjs.org/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [Plotly.js](https://plotly.com/javascript/)
- [D3.js](https://d3js.org/)
