# 1) El siguiente job en MapReduce permite contabilizar cuantas palabras comienzan con cada una de las letras del abecedario.
```python
def map(key, values, context):
    words = values.split()
    for w in words:
        context.write(w[0], 1)

def reduce(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)
```
### a. Solucione el problema del “case sensitive” usando comparadores.
```python
def cmpShuffle(key, anotherKey):
  if key.upper() == anotherKey:
    return 0
  else:
    return -1

#...

job.setShuffleCmp(cmpShuffle)
```
## b. ¿Cuántos reducers se ejecutan en este problema? Sabiendo que se cuenta con el doble de nodos para la tarea de reduce ¿Cómo podría usar los comparadores para aprovechar todos los nodos?

Se ejecutan 2 por cada letra del abecedario, suponiendo que todas tienen al menos una palabra tanto en mayúscula como en minúscula (en total serían 56).

Una manera de aprovechar el doble de nodos sería crear una variable random entre 0 y 1, que haga que cada letra pueda ir a uno de 2 nodos que tenga asignados.

```python
def map(key, values, context):
    words = values.split()
    for w in words:
        context.write((Math.random(0,1),w[0]), 1)

def cmpShuffle(key, anotherKey):
  if key[0] == anotherKey[0]
    return 0
  elif key[0] < anotherKey[0]:
    return -1
  else:
    return 1
```
Aunque al hacer esto tendríamos que hacer un segundo Job o un programa externo para unir los resultados de los dos reducers ejecutándose en nodos distintos.

