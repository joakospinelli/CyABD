# 2) ¿Qué operaciones de resumen realizadas por los reducers se ven beneficiados por la definición de funciones de comparación personalizadas? ¿Qué consideraciones habría que tener en cuenta relacionado con la tarea de los mappers?

* Comparando en el shuffle, podemos agrupar las claves que deseemos (aunque comparándolas sean distintas) y enviarlas a una misma función *reduce*. En este caso, el *mapper* tendría que asignarle la clave que querramos relacionar a cada tupla.
* Comparando en el sort, podemos agregar las tuplas de un tipo determinado primero y usarlas para filtrar la información que querramos de las otras tuplas. En este caso, el *mapper* tendría que agregarle a la clave de cada tupla un campo que determine el "tipo" o lo distinga de otras.
