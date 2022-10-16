from pyspark import SparkContext
sc = SparkContext("local","Spark")

root_path = 'file:///content/drive/My Drive/Colab Notebooks/'

# 2) Usando el dataset EstacionesMeteorologicas imprima el ID de la estación que tiene el máximo registro de humedad,
# el ID de la estación con máximo registro en temperatura y el ID de la estación con el máximo registro de precipitación usando solo seis
# transformaciones, incluyendo la transformación textFile.

# <ID_Estación, fecha_registro, temperatura, humedad, precipitación>

registros = sc.textFile(root_path + "EstacionesMeteorologicas/input/")

registros = registros.map(lambda t: t.split("\t"))

registros = registros.map(lambda t: (t[0], t[2], t[3], t[4]))

maxTemperatura = registros.reduce(lambda t1, t2: t1 if float(t1[1]) >= float(t2[1]) else t2)

print("ESTACIÓN MAYOR TEMPERATURA: " + maxTemperatura[0] + " (" + maxTemperatura[1] + ")")

maxHumedad = registros.reduce(lambda t1, t2: t1 if float(t1[2]) >= float(t2[2]) else t2)

print("ESTACIÓN MAYOR HUMEDAD: " + maxHumedad[0] + " (" + maxHumedad[2] + ")")

maxPrecipitaciones = registros.reduce(lambda t1,t2: t1 if float(t1[3]) >= float(t2[3]) else t2)

print("ESTACIÓN MAYOR PRECIPITACIÓN: " + maxPrecipitaciones[0] + " (" + maxPrecipitaciones[3] + ")")

# Debe haber alguna manera más efectiva de resolverlo pero zzz