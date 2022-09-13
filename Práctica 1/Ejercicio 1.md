# 1. Responda para cada job: ¿Cuántas veces (invocaciones) se ejecuta la función map? ¿Cuántas veces (invocaciones) se ejecuta la función reduce? ¿Cuántos mappers se ejecutan? ¿Cuántos reducers se ejecutan? ¿Qué datos recibe cada función reduce? ¿Cuál es la salida de cada job?

## a)
Devuelve la cantidad de pares clave-valor en el dataset.

* La función `map` se ejecuta 16 veces (una vez por par clave-valor)
* La función `reduce` se ejecuta una vez y recibe como datos los pares de clave-valor

#
## b)
Devuelve la suma total de los valores del dataset.

* La función `map` se ejecuta 16 veces (una vez por clave-valor)
* La función `reduce` se ejecuta una vez y recibe los valores de los pares del dataset

#
## c)
Devuelve dos entradas:

En la primera devuelve la clave mayor entre los valores menores a 30,

En la segunda devuelve la clave mayor entre los valores mayores a 30


* La función `map` se ejecuta 16 veces (una vez por clave-valor)
* La función `reduce` se ejecuta dos veces (una por cada entrada); en la primera ejecución recibe las claves de los valores menores a 30, y en la segunda recibe las claves de los mayores a 30

#
## d)
Devuelve la suma de los valores que comparten la misma clave en el dataset
* La función `map` se ejecuta 16 veces (una vez por clave-valor)
* La función `reduce` se ejecuta 14 veces (una vez por cada clave no repetida) y en cada ejecución recibe los valores que corresponden a esa clave, repetidos varias veces según su valor

#
## e)
Devuelve las veces que el valor de una clave ya se repitió en otras anteriores.

* La función `map` se ejecuta 16 veces (una vez por clave-valor)
* La función `reduce` se ejecuta 12 veces (una vez por cada valor no repetido) y en cada ejecución recibe un valor y las claves en las que se repitió