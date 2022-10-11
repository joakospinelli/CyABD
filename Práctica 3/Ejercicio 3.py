from pyspark import SparkContext
sc = SparkContext("local","Spark")

root_path = 'file:///content/drive/My Drive/Colab Notebooks/'

# 3) Usando el dataset Banco, escriba un script en Python usando Spark para responder a las siguientes preguntas:

# a. Nombre y apellidos de los clientes capricornianos.
from datetime import date

def crearFecha(strDate):
  [ año, mes, dia ] = strDate.split("-")
  return date(int(año), int(mes), int(dia))

def esCapricorniano(t):
  return (t[2].month == 12 and t[2].day >= 22) or (t[2].month == 1 and t[2].day <= 20)

clientes = sc.textFile(root_path + "Banco/input/Clientes/")

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[1], t[2], crearFecha(t[4])))

clientes = clientes.filter(esCapricorniano)

clientes = clientes.map(lambda t: (t[0], t[1]))

print(clientes.collect())

# b. Nombre y apellido de los clientes de nacionalidad argentina.

clientes = sc.textFile(root_path + "Banco/input/Clientes/")

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[1], t[2], t[5]))

clientes = clientes.filter(lambda t: t[2] == 'ARG')
clientes = clientes.map(lambda t: (t[0], t[1]))

print(clientes.collect())

# c. Del resultado de a) cuántos nacieron en verano.

""" 
    NO ME VAN A ENGAÑAR YO SÉ QUE TODOS LOS DE CAPRICORNIO NACEN EN VERANO
"""

# d. Del resultado de b) quién es el cliente más joven y quién el más viejo.

from datetime import date

def crearFecha(strDate):
  [ año, mes, dia ] = strDate.split("-")
  return date(int(año), int(mes), int(dia))

clientes = sc.textFile(root_path + "Banco/input/Clientes/")

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[1], t[2], crearFecha(t[4]), t[5])) # Tengo que agregar el campo de fecha

clientes = clientes.filter(lambda t: t[3] == 'ARG')
clientes = clientes.map(lambda t: (t[0], t[1], t[2]))

print("CLIENTE MÁS JOVEN:")
print(clientes.reduce(lambda t1,t2: t1 if t1[2] >= t2[2] else t2))

print("---")

print("CLIENTE MÁS VIEJO:")
print(clientes.reduce(lambda t1,t2: t1 if t1[2] <= t2[2] else t2))

# e. El ID de la caja que tiene asociado el préstamo con mayor cantidad de cuotas y entre las que tienen la misma cantidad, el de mayor monto.

prestamos = sc.textFile(root_path + "Banco/input/Prestamos")
cajas = sc.textFile(root_path + "Banco/input/CajasDeAhorro")

prestamos = prestamos.map(lambda t: t.split("\t"))

# Se podría hacer un reduce o otra cosa que mantenga los que son iguales? Para no tener que hacer el filter después (PREGUNTAR)

prestamoMayor = prestamos.reduce(lambda t1,t2: t1 if int(t1[1]) >= int(t2[1]) else t2) # Obtengo la mayor cantidad de cuotas

prestamos = prestamos.filter(lambda t: t[1] == prestamoMayor[1]) # Obtengo los préstamos con la misma cantidad
print(prestamos.reduce(lambda t1,t2: t1 if float(t1[2]) >= float(t2[2]) else t2))

# f. Los ID de clientes (únicos) con al menos una caja de ahorro (en positivo) cuyo saldo es mayor a 300 U$S.

cajas = sc.textFile(root_path + "Banco/input/CajasDeAhorro/")

cajas = cajas.map(lambda t: t.split("\t"))
cajas = cajas.filter(lambda t: float(t[2]) > 300)

cajas = cajas.map(lambda t: t[1])
cajas = cajas.distinct()

print(cajas.collect())

# g. Del dataset Movimientos, el monto del mayor movimiento y el id de caja del último movimiento.

from datetime import datetime

movimientos = sc.textFile(root_path + "Banco/input/Movimientos/")

movimientos = movimientos.map(lambda t: t.split("\t"))

mayorMovimiento = movimientos.reduce(lambda t1,t2: t1 if t1[1] >= t2[1] else t2)

print("MAYOR MOVIMIENTO:")
print(mayorMovimiento)

def crearDatetime(strDate):
  [ fecha, hora ] = strDate.split(" ")
  [ año, mes, dia ] = fecha.split("-")
  [ hora, minutos, segundos ] = hora.split(":")

  return datetime(int(año),int(mes),int(dia),int(hora),int(minutos),int(segundos))

ultimoMovimiento = movimientos.map(lambda t: (t[0], t[1], crearDatetime(t[2])))

ultimoMovimiento = ultimoMovimiento.reduce(lambda t1,t2: t1 if t1[2] >= t2[2] else t2)

print("ÚLTIMO MOVIMIENTO:")
print(ultimoMovimiento)