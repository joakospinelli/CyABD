from pyspark import SparkContext
sc = SparkContext("local","Spark")

root_path = 'file:///content/drive/My Drive/Colab Notebooks/'

#  (ID_vehículo, Avenida, Calle, Timestamp, Destino)
viajes = sc.textFile(root_path + '/Viajes/input/')
viajes = viajes.map(lambda t: t.split("\t"))

# 1) Implemente un script de Spark que permita conocer cuántos viajes realizó cada vehículo.
# Recordar que un viaje es una serie de coordenadas que finalizan en un destino determinado.

viajesVehiculo = viajes.map(lambda t: (t[0], (t[1], t[2], t[3], t[4]) ) )

viajesVehiculo = viajesVehiculo.filter(lambda t: t[1][3] != '')

print('Viajes por vehículo:')
print(viajesVehiculo.countByKey())

# 2) Implemente un script de Spark que permita conocer cual es el top 3 de los "tipos" de destinos más visitados.
# Los "tipos" de destino válidos son "Hospital", "Escuela", "Plaza", "Ferretería", "Farmacia", "Supermercado", "Museo".
# NO interesa contar a los destinos "Otro".

lugaresBD = viajes.map(lambda t: (t[4], 1) ) 
lugaresBD = lugaresBD.filter(lambda t: t[0] != '' and t[0] != 'Otro')

lugaresBD = lugaresBD.reduceByKey(lambda t1, t2: t1 + t2)

lugaresBD = lugaresBD.map(lambda t: (t[1], t[0])) # Invierto clave-valor para que el sort interno de 'top' funcione con la cantidad

lugaresBD = lugaresBD.top(3)

print('Destinos más visitados:')
print(lugaresBD)

# 3) Implemente un script de Spark que permita conocer la cantidad de vehículos en movimiento por franja horaria.
# La duración de la franja horaria es un parámetro de la consulta.

# Hay que preguntar cuál es el valor máximo del timestamp para hacer el cálculo (en el dataset era 23997 pero capaz podría llegar a más)
while (True):
  duracion = int(input('ingrese la duración de la franja horaria: '))
  if (duracion > 24000):
    print('ingrese un número menor a 24000')
  else: break

duracion = sc.broadcast(duracion)

# Divide el tiempo total por la duración de la franja horaria y retorna la franja en la que estaría la tupla
def getFranjaHoraria(timestamp):
  return int(int(timestamp) / duracion.value)

franjas = viajes.map(lambda t: (getFranjaHoraria(t[3]), 1) )

franjas = franjas.reduceByKey(lambda t1, t2: t1 + t2)

conteoViajes = franjas.sortByKey().collect()

print('cantidad de vehículos por franja horaria')
for i in conteoViajes:
  print((i[0] * duracion.value),'-',((duracion.value * (i[0] + 1) - 1)),': ', i[1])

# 4) Implemente un script de Spark que permita conocer cuál es el top 10 de las esquinas (avenida, calle) más transitadas por vehículos diferentes.
# En esta consulta cada vehículo cuenta como paso de una esquina una única vez,
# independientemente de que el mismo vehículo haya pasado por la misma esquina varias veces en diferentes viajes.

esquinas = viajes.map(lambda t: (t[1] + ',' + t[2], t[0]) )
esquinas = esquinas.distinct()

esquinas = esquinas.map(lambda t: (t[0], 1) )

esquinas = esquinas.reduceByKey(lambda t1,t2: t1 + t2)

esquinas = esquinas.map(lambda t: (t[1], t[0]) )

esquinas = esquinas.top(10)

print('Esquinas más transitadas:')
print(esquinas)

# 5) Implemente un script de Spark que permita conocer la avenida y la calle más recorrida.
# La avenida (y también la calle) más recorrida es aquella por la que transitaron más vehículos en cualquiera de sus tramos.
# En esta consulta, cada vehículo puede sumar más de una vez si pasó por la misma cuadra varias veces.

avenidas = viajes.map(lambda t: (t[1], 1) )
calles = viajes.map(lambda t: (t[2], 1) )

avenidas = avenidas.reduceByKey(lambda t1,t2: t1 + t2)
avenidas = avenidas.reduce(lambda t1,t2: t1 if t1[1] > t2[1] else t2)

calles = calles.reduceByKey(lambda t1,t2: t1 + t2)
calles = calles.reduce(lambda t1,t2: t1 if t1[1] > t2[1] else t2)

print('Avenida más recorrida: ', avenidas)
print('Calle más recorrida: ', calles)