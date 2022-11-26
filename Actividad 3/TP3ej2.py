# 2) Implemente un script de Spark que permita conocer cuál es la esquina “promedio” entre todas las esquinas cuyo destino es “Otro”.

# Cada celda está separada por el comentario '------------------'.

from pyspark import SparkContext
sc = SparkContext("local","Spark")

from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc,15)

ssc.checkpoint("buffer")

# ------------------

root_path = 'file:///content/drive/My Drive/Colab Notebooks/'

streamOriginal = ssc.textFileStream(root_path + 'Traffic/input')

stream = streamOriginal.map(lambda t: t.split("\t"))

# tomo el ID del vehiculo, la calle y la avenida de todos los destinos 'Otro'
stream = stream.map(lambda t: (t[0], t[1], t[2], t[4]) )
stream = stream.filter(lambda t: t[3] == 'Otro')

# mapeo todas las avenidas y calles a una misma clave, sumando en la función map el acumulador
stream = stream.map(lambda t: (1, (int(t[1]), int(t[2]), 1) ) )

# hago un reduce sumando las avenidas y calles
stream = stream.reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1], t1[2] + t2[2]) )

def fUpdate(newValues, history):
  if(history == None):
    history = (0,0,0)
  if(newValues == None or newValues == []):
    newValues = (0,0,0)
    return history
  else:
    newValues = ( sum(map(lambda t: t[0], newValues)) , sum(map(lambda t: t[1], newValues)), sum(map(lambda t: t[2], newValues)) )
  return (history[0] + newValues[0], history[1] + newValues[1], newValues[2] + history[2])

history = stream.updateStateByKey(fUpdate)

def fmap(t):
  return (t[1][0] / t[1][2], t[1][1] / t[1][2])

results = history.map(fmap)

results.pprint()

# ------------------

ssc.start()
ssc.awaitTermination()