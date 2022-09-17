# 5. Indique si utilizando el dataset Libros es posible resolver los siguientes problemas:

## a) Obtener todos los títulos de todos los libros
(Se supone que se puede hacer una trampa con la key asociada a cada renglón pero no sé bien como hacerlo xd)

#
## b) Obtener la cantidad de palabras promedio por párrafo
(Este no sé si está bien porque tendría que hacer el cálculo a mano y no tengo ganas, pero el separar por párrafos funciona)
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

No es posible obtener el promedio por cada libro porque la función `map` no recibe los datos en el orden físico exacto, y no puede determinar el archivo del que provienen.

#
## d) Obtener la cantidad de caracteres del párrafo más extenso

Pude hacer lo de contar la cantidad de caracteres de cada párrafo, pero no sé como hacer lo de obtener el más extenso (CREO que no se puede hacer).
```python
def fmap(key, value, context):
    words = value.split('. \n')
    for w in words:
      c = 0
      palabrasParrafo = len(w)
      context.write(w, palabrasParrafo)
        
def fred(key, values, context):
    total = 0
    for v in values:
      context.write(key, v)
```

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
```python
def fmap(key, value, context):
    words = value.split("\n")
    for w in words:
      if (w.startswith("--")):
        # En el valor guardo una tupla (dialogo-caracteres)
        context.write(1, (w, len(w)))
        
def fred(key, values, context):
    max = -1
    maxDialogo = ''
    for v in values:
        if (v[1] > max):
          max = v[1]
          maxDialogo = v[0]
    context.write("DIALOGO MÁS LARGO", (maxDialogo, max))
```

#
## g)
No es posible obtener el top 20 de palabras en cada libro porque la función `map` no recibe los datos en el orden físico exacto, y no puede determinar el archivo del que provienen.