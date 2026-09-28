# Informe de coordinación

## Identificación de consultas

Cada conexión aceptada por el Gateway representa una consulta independiente.
Al crear el `MessageHandler` se genera un identificador único (`request_id`).
Este identificador no modifica el protocolo externo utilizado entre el cliente y
el Gateway: se incorpora únicamente a los mensajes internos del sistema.

Los mensajes internos de datos tienen la forma:

```text
[request_id, "DATA", fruta, cantidad]
```

El fin de una consulta se representa como:

```text
[request_id, "EOF"]
```

El `request_id` se conserva durante todo el pipeline, lo que permite mantener
separados los datos y los resultados de clientes concurrentes.

## Coordinación de Sum

Las instancias de Sum consumen la cola compartida de entrada. RabbitMQ reparte
los mensajes entre las réplicas, por lo que cada instancia procesa una parte de
los registros recibidos.

Cada Sum mantiene acumuladores independientes por `request_id`. Para evitar que
el uso de memoria crezca indefinidamente con el volumen de datos, los
acumuladores se dividen en lotes limitados por `SUM_BATCH_SIZE`. Cuando un lote
alcanza el límite configurado, Sum envía sus subtotales y libera la memoria
asociada. Al recibir el cierre de una consulta, el Gateway incluye en el mensaje EOF la
cantidad total de registros de esa consulta. Cada Sum informa su progreso por
`request_id` y por instancia. Las réplicas mantienen una barrera distribuida y
envían el lote restante y su notificación de finalización sólo cuando la suma
de los progresos alcanza la cantidad total esperada.

Las notificaciones de progreso y de cierre se publican mediante un exchange de
control para que todas las réplicas de Sum conozcan el estado de la consulta.
El cierre de cada Sum se publica en el mismo flujo de salida que sus datos, de
modo que Aggregation recibe los mensajes DATA antes del EOF correspondiente.
Aggregation considera finalizada una consulta cuando recibe la notificación de
todas las instancias de Sum configuradas.

## Distribución entre Aggregation

Los subtotales no se envían por broadcast a todas las instancias de
Aggregation. Para cada fruta se calcula una partición determinista utilizando
el identificador de la consulta y el nombre de la fruta:

```text
hash(request_id + fruta) % cantidad_de_aggregators
```

El resultado se publica únicamente en el exchange asociado al Aggregator
seleccionado. De esta manera, una fruta de una consulta siempre es procesada
por una sola instancia y se evita el procesamiento redundante. Al incluir el
`request_id`, distintos clientes pueden distribuir sus frutas de manera
independiente y aprovechar todas las réplicas disponibles.

Cada Aggregator mantiene un top parcial separado por consulta. Cuando recibe la
finalización de todas las instancias de Sum, calcula su top parcial y lo envía
al Join junto con su propio identificador:

```text
[request_id, "PARTIAL_TOP", aggregation_id, top_parcial]
```

## Coordinación de Join

Join agrupa los resultados parciales por `request_id` y por
`aggregation_id`. No genera el resultado final al recibir un único parcial:
espera un resultado de cada Aggregator configurado. Luego combina los parciales,
acumula los valores de frutas repetidas y calcula el top final.

El resultado se envía al Gateway con la forma:

```text
[request_id, "FINAL_TOP", top_final]
```

El Gateway utiliza el `request_id` para entregar la respuesta al cliente
correspondiente.

## Escalabilidad

### Cantidad de clientes

Los datos de varias consultas pueden circular por las mismas colas sin mezclarse,
porque todos los mensajes internos llevan `request_id` y cada control mantiene
su estado separado por consulta. Las respuestas también se correlacionan con
su cliente mediante ese identificador.

### Grandes volúmenes de datos

El procesamiento se realiza de forma incremental. Sum no necesita conservar en
memoria todos los registros recibidos: acumula una cantidad limitada de frutas
distintas, envía subtotales parciales y libera cada lote. Aggregation combina
esos subtotales, por lo que el volumen total de registros puede ser mayor que
la memoria disponible para un único lote.

### Cantidad de controles

La cantidad de réplicas se obtiene de la configuración del escenario. Las
instancias de Sum se reparten los mensajes de entrada mediante RabbitMQ y cada
fruta se enruta a un único Aggregator usando una partición determinista. Join
espera la participación de todas las réplicas de Aggregation antes de producir
el resultado final. Así, las réplicas agregan capacidad de procesamiento en
lugar de repetir el mismo trabajo.
