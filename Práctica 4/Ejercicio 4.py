# 4) Es posible resolver los siguientes problemas (por separado) utilizando una única función *reduce*:
# a. El promedio de edades de los clientes

from datetime import date

def crearFecha(strDate):
  [ año, mes, dia ] = strDate.split("-")
  return date(int(año), int(mes), int(dia))

def getEdad(date):
  hoy = date.today()
  return hoy.year - date.year - ((hoy.month, hoy.day) < (date.month, date.day))

clientes = sc.textFile(root_path + "Banco/input/Clientes/")

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: getEdad(crearFecha(t[4])))

acum = clientes.reduce(lambda t1,t2: t1 + t2)
print(acum / clientes.count())

# b. Determinar la cantidad de cuentas con saldo positivo y la cantidad de cuentas con saldo negativo.

"""
    Yo creo que no se puede porque el Reduce compara de a pares y tiene que devolver una tupla; no sabría
    cómo hacer esa transformación para contar las cuentas.
"""

cajas = sc.textFile(root_path + "Banco/input/CajasDeAhorro/")

cajas = cajas.map(lambda t: t.split("\t"))
cajas = cajas.map(lambda t: (t[2], 0, 0))

cajasPositivo = cajas.filter(lambda t: float(t) >= 0)
cajasNegativo = cajas.filter(lambda t: float(t) < 0)

print(cajasPositivo.count())
print(cajasNegativo.count())