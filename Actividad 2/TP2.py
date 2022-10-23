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

print(viajesVehiculo.countByKey())

# 2) Implemente un script de Spark que permita conocer cual es el top 3 de los "tipos" de destinos más visitados.
# Los "tipos" de destino válidos son "Hospital", "Escuela", "Plaza", "Ferretería", "Farmacia", "Supermercado", "Museo".
# NO interesa contar a los destinos "Otro".

# --- Esta solución no es válida si los tipos de destino fuesen Big Data

viajes = sc.textFile(root_path + '/Viajes/input/')
viajes = viajes.map(lambda t: t.split("\t"))

lugares = viajes.map(lambda t: (t[4], 1) ) 
lugares = lugares.filter(lambda t: t[0] != '' and t[0] != 'Otro')

lugares = lugares.countByKey()

sorted(lugares.items(), key=lambda x: x[1], reverse=True)[0:3]

# --- Otra solución

lugaresBD = viajes.map(lambda t: (t[4], 1) ) 
lugaresBD = lugaresBD.filter(lambda t: t[0] != '' and t[0] != 'Otro')

lugaresBD = lugaresBD.reduceByKey(lambda t1, t2: t1 + t2)

lugaresBD = lugaresBD.map(lambda t: (t[1], t[0])) # Invierto clave-valor para que el sort interno de 'top' funcione con la cantidad

lugaresBD = lugaresBD.top(3)

print(lugaresBD)