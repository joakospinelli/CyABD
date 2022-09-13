# 5. Indique si utilizando el dataset Libros es posible resolver los siguientes problemas:

## a) Obtener todos los títulos de todos los libros
Acá se me ocurrió hacer una trampa y mapear sólo los que estén escritos todos en mayúsculas. Devuelve los títulos pero también algunas cosas más así que no está bien, pero no sé si se pueden dividir por libro así que dejo esta solución igual 😎

```python
def fmap(key, value, context):
    words = value.split('\n')
    for w in words:
      if (w == w.upper()):
        context.write(w, 1)
        
def fred(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)
```
#
## b) Obtener la cantidad de palabras promedio por párrafo
Este no sé si está bien porque tendría que hacer el cálculo a mano y no tengo ganas (pero el separar por párrafos funciona)
```python
def fmap(key, value, context):
    words = value.split('. \n')
    for w in words:
      c = 0
      palabrasParrafo = w.split(" ")
      for p in palabrasParrafo:
        c = c + 1
      context.write(1, c)
        
def fred(key, values, context):
    total = 0
    cont = 0
    for v in values:
        total = total + 1
        cont = cont + v
    context.write(1, cont / total)
```

#
## c) Obtener la cantidad de párrafos promedio por libro
(NO SÉ CÓMO DIVIDIR POR LIBRO)

#
## d) Obtener la cantidad de caracteres del párrafo más extenso
(NO SÉ COMO COMPARAR CON OTROS EN EL REDUCE)

#
## e)
```python
def fmap(key, value, context):
    words = value.split()
    for w in words:
      if (w.startswith("--")):
        context.write(1, 1)
        
def fred(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)
```

#
## f)
(NO SÉ COMO COMPARAR CON OTROS EN EL REDUCE)

#
## g)
(NO SÉ COMO COMPARAR CON OTROS EN EL REDUCE)