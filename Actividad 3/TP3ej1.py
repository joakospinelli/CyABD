# 1. Implemente un script de Spark que permita conocer cuántos viajes realizó cada vehículo.
# Recordar que un viaje es una serie de coordenadas que finalizan en un destino determinado.

# Cada celda está separada por el comentario '------------------'.

from pyspark import SparkContext
sc = SparkContext("local","Spark")

from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc,15)

ssc.checkpoint("buffer")

# ------------------

root_path = 'file:///content/drive/MyDrive/Colab Notebooks/'

# Transformaciones
streamOriginal = ssc.textFileStream(root_path + 'Traffic/input')

stream = streamOriginal.map(lambda t: t.split("\t"))
stream = stream.map(lambda t: (t[0], t[4]) )

stream = stream.filter(lambda t: t[1] != '')

stream = stream.map(lambda t: (t[0], 1) )

stream = stream.reduceByKey(lambda t1, t2: t1 + t2)

# stream.pprint()

# Transformaciones
def fUpdate(newValues, history):
  if(history == None):
    history = 0
  if(newValues == None):
    newValues = 0
  else:
    newValues = sum(newValues)
  return newValues + history

history = stream.updateStateByKey(fUpdate)

history.pprint()

# ------------------

ssc.start()
ssc.awaitTermination()