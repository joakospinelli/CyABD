El error es que el script sobreescribe los valores del RDD dentro del for. Esto hace que en cada iteración no trabaje con los valores originales, sino que usa los valores ya modificados por la iteración anterior.

La solución correcta sería:
```python
rdd = sc.parallelize([1,2,3,4,5])
for i in range(2,6):
  acc = sc.broadcast(i)
  rddTemp = rdd.map(lambda v: v ** acc.value)
  r = rddTemp.reduce(lambda x,y : x + y)
  r = r / 5
  print(r)
```