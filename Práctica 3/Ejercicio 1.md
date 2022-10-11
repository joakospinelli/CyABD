# 1. ¿Qué imprime cada uno de los siguientes scripts (sin ejecutarlo)?
## a. 
```python
res = rdd.map(lambda t: t[0] + t[1] * 2)
print(res.first())
```
La función *map* va a transformar las tuplas a una nueva con la suma del primer valor con el segundo multiplicado por 2.

La función *first* devuelve la primera tupla, pero no puede garantizar que devuelva la primera en orden físico (va a devolver la primera en terminar de ejecutarse).

## b.
```python
res = rdd.filter(lambda t: t[0] >= t[1])
print(res.take(3))
```
La función *filter* va a filtrar sólo aquellas tuplas cuyo primer elemento sea mayor o igual al segundo

La función *take(3)* devuelve 3 tuplas aleatorias de aquellas que hayan pasado el filtro.

## c.
```python
res = rdd.map(lambda t: (t[0], t[1], t[0] / t[1]))

res = res.filter(lambda t: t[2] < 0.5)

res = res.reduce(lambda t1, t2:
    t1 if t1[2] < t2[2] else t2))
print(res)
```
La función *map* va a transformar las tuplas a una nueva 3-tupla con los siguientes elementos:
<ol>
<li> Primer elemento
<li> Segundo elemento
<li> Resultado de la división del primer elemento por el segundo
</ol>

La función *filter* va a filtrar sólo aquellas tuplas en las que el resultado de la división anterior sea inferior a 0.5.

La función *reduce* va a retornar la tupla con el resultado de la división más bajo.

## d.
```python
r1 = rdd.map(lambda t: t[0])
r2 = rdd.map(lambda t: t[1])

r1 = r1.distinct()
r2 = r2.distinct()
res = r2.union(r1)

print(res.collect())
```
La función *map* en `r1` va a transformar la tupla al valor de su primer elemento.

La función *map* en `r2` va a transformar la tupla al valor de su segundo elemento.

La función *distinct* va a eliminar las tuplas duplicadas de `r1` y `r2`.

La función *r2.union(r1)* va a unir todas las tuplas de `r2` con las de `r1`; la unión no elimina repetidos, por lo que si hay elementos que estén en ambos RDD entonces van a aparecer dos veces.

La función *collect* vuelca toda la información del resultado al nodo maestro o *"driver"*.


