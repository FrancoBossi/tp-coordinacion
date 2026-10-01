# Informe de coordinación y escalabilidad

## 1. Identificación de las consultas

Cada conexión aceptada por el Gateway representa una consulta independiente.
Para identificarla, el `MessageHandler` genera un `request_id` único. Este
identificador no modifica el protocolo externo entre el cliente y el Gateway:
se agrega únicamente a los mensajes internos.

Los mensajes de datos tienen la siguiente estructura:

```text
[request_id, "DATA", fruta, cantidad]
```

El cierre de la entrada se informa mediante:

```text
[request_id, "EOF", cantidad_de_registros]
```

El `request_id` acompaña a los datos y a todos los mensajes de coordinación
hasta la respuesta final. De este modo, los estados de consultas concurrentes
se mantienen separados y el Gateway puede entregar cada resultado al cliente
correspondiente.

## 2. Coordinación de las instancias de Sum

Las instancias de Sum consumen una cola de entrada compartida. RabbitMQ
distribuye los mensajes entre las réplicas, permitiendo que procesen
concurrentemente distintos registros.

Para garantizar que una fruta se sume en un único lugar, cada par
`(request_id, fruta)` se asigna determinísticamente a una réplica de Sum:

```text
hash(request_id + fruta) % cantidad_de_sums
```

Si el registro llega a una réplica distinta de la responsable, se reenvía a
través de una cola durable de comunicación entre instancias (`INTER_SUM_DATA`).
La réplica responsable acumula todos los registros de esa fruta utilizando
`FruitItem.__add__`. Por lo tanto, la operación de suma se realiza únicamente
en Sum.

Cada instancia mantiene sus acumuladores separados por `request_id`. Al
recibir un registro procesado, informa su progreso. Cuando el Gateway recibe el
fin de una consulta, incluye la cantidad total de registros enviados. Esa
cantidad se distribuye mediante las colas de control durables, junto con los
mensajes de progreso de cada réplica.

La consulta se considera completa cuando la suma de los progresos de todas las
réplicas alcanza la cantidad total esperada. Recién entonces cada Sum publica
sus subtotales y su mensaje `EOF`. Los datos se publican antes del `EOF`
correspondiente para garantizar que Aggregation no finalice prematuramente.

## 3. Coordinación de las instancias de Aggregation

Sum envía un único subtotal definitivo por fruta. Aggregation no vuelve a
sumar cantidades y no utiliza `FruitItem.__add__`: conserva los subtotales
recibidos y los ordena para construir un top parcial.

Los subtotales se distribuyen sin broadcast. La instancia de Aggregation se
selecciona mediante una partición determinística:

```text
hash(request_id + fruta) % cantidad_de_aggregations
```

Así, todos los subtotales de una consulta llegan a una única réplica de
Aggregation y cada réplica procesa una parte diferente del trabajo.

Cada Aggregation espera recibir el `EOF` de todas las instancias de Sum. Una
vez cumplida esa barrera, publica un resultado parcial con su identificador:

```text
[request_id, "PARTIAL_TOP", aggregation_id, top_parcial]
```

Join espera un resultado parcial de cada Aggregation. Luego reúne los parciales
y produce el resultado final:

```text
[request_id, "FINAL_TOP", top_final]
```

El Gateway utiliza el `request_id` para asociar ese resultado con el cliente
que originó la consulta.

## 4. Escalabilidad respecto de los clientes

El sistema puede atender varias consultas concurrentes utilizando las mismas
colas y réplicas. La separación no depende de crear una infraestructura
distinta para cada cliente, sino de conservar el `request_id` en todos los
mensajes y estructuras de estado.

Los acumuladores, las barreras de finalización y los resultados parciales se
indexan por `request_id`. Por lo tanto, la finalización o el resultado de una
consulta no afecta el procesamiento de otra consulta concurrente.

Además, el hash incluye el identificador de la consulta. Esto evita que todas
las consultas distribuyan necesariamente las mismas frutas sobre las mismas
réplicas y favorece un reparto más equilibrado del trabajo.

## 5. Escalabilidad frente a grandes volúmenes de datos

Los registros se procesan de manera incremental. Sum no conserva la secuencia
completa de entrada: cada registro se suma en el acumulador correspondiente o
se reenvía a la réplica responsable. Una vez procesado, el registro original
deja de ocupar espacio en la cola de trabajo.

La memoria utilizada por Sum depende principalmente de la cantidad de frutas
distintas activas por consulta, y no de la cantidad total de registros
recibidos. Aggregation tampoco conserva los registros originales: almacena los
subtotales necesarios para calcular su top parcial.

La barrera basada en el conteo de registros permite detectar que todas las
instancias terminaron de procesar la entrada antes de publicar los resultados.
De esta manera, el sistema evita depender de una única réplica para detectar el
fin de la consulta.

## 6. Escalabilidad respecto de la cantidad de controles

La cantidad de réplicas de Sum y Aggregation se obtiene de la configuración
del escenario. La solución no está ligada a una cantidad fija de instancias:

- Sum reparte el trabajo de entrada y deriva cada fruta a una única réplica
  responsable.
- Aggregation recibe particiones exclusivas de los subtotales.
- Join espera explícitamente la participación de todas las réplicas de
  Aggregation.

Por lo tanto, agregar réplicas aumenta la capacidad de procesamiento sin
replicar innecesariamente el mismo trabajo. Las colas durables de datos y de
control también permiten que los mensajes no se pierdan si una instancia tarda
en iniciar.

## 7. Terminación ordenada

Sum, Aggregation y Join registran handlers para `SIGTERM` y `SIGINT`. Ante una
señal, cada proceso solicita detener el consumo y conserva abiertas las
conexiones mientras RabbitMQ termina de procesar la operación de cierre.

Cuando el consumo finaliza, el bloque `finally` cierra las colas, exchanges y
conexiones utilizadas por el proceso. Esta secuencia evita cerrar un descriptor
mientras Pika todavía lo está utilizando y permite que los contenedores
terminen correctamente con código de salida `0`.
