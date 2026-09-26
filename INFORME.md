Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

## Identificación de consultas concurrentes

Para permitir el procesamiento concurrente de múltiples clientes, cada conexión
aceptada por el Gateway se asocia a un identificador único de consulta
(`request_id`). Este identificador no modifica el protocolo externo utilizado por
los clientes, sino que se incorpora únicamente a los mensajes internos del
sistema.

Los mensajes de datos y de finalización incluyen el `request_id`, y las
instancias de Sum y Aggregation mantienen su estado separado por consulta. De
esta forma, los mensajes de distintos clientes pueden circular por las mismas
colas sin que sus datos sean acumulados conjuntamente.

El identificador también se propaga junto con los resultados para permitir que
el Gateway relacione cada top con el cliente correspondiente. Esta estrategia
constituye la base para escalar el sistema respecto de la cantidad de clientes;
la coordinación de la finalización entre múltiples instancias y la consolidación
de resultados parciales se implementan en etapas posteriores.
