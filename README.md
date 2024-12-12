# Proyecto de Prueba de Funcionalidades Básicas de MinIO

Este proyecto en Java demuestra las operaciones fundamentales con MinIO, un servicio de almacenamiento compatible con Amazon S3. A través de este código, se ilustran acciones como la conexión al servidor, creación de buckets, carga y descarga de archivos, gestión de objetos y configuración de políticas de acceso.

## Requisitos Previos

- Java 11: Asegúrate de tener instalado Java 11 o una versión superior.
- MinIO Server: Debes tener acceso a un servidor MinIO en funcionamiento. Puedes instalarlo localmente o utilizar una instancia remota.
- MinIO Client (mc): Herramienta de línea de comandos para interactuar con el servidor MinIO.

## Configuración del Entorno

- Cambiar la variable SERVER: Hay que cambiar la variable SERVER de App.java por la url al servidor de minio
- Configurar las Credenciales: En el archivo App.java, actualiza las variables ACCESS_KEY, SECRET_KEY, ACCESS_KEY_2 y SECRET_KEY_2 con las credenciales correspondientes a tu servidor MinIO.
- Actualizar la Ubicación de Archivos: Modifica la variable LOCATION para que apunte al directorio donde se encuentran los archivos que deseas cargar.
