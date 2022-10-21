# 4) Utilizando el dataset Banco escriba un script que permita determinar si las siguientes afirmaciones son verdaderas:

# Cliente: <ID_Cliente, nombre, apellido, DNI, fecha de nacimiento, nacionalidad>
# CajaDeAhorro: <ID_Caja, ID_Cliente, saldo>
# Prestamos: <ID_Caja, cuotas, monto>

clientes = sc.textFile(root_path + "Banco/input/Clientes")
cajas = sc.textFile(root_path + "Banco/input/CajasDeAhorro")
prestamos = sc.textFile(root_path + "Banco/input/Prestamos")

europeos = [ "ITA", "ESP" ]
americanos = [ "BRA", "ARG", "VEN", "COL", "BOL", "ECU", "URU", "PAR", "CHI", "PER" ]

# a. El banco tiene más clientes europeos que americanos

clientes = clientes.map(lambda t: t.split("\t"))

clientes = clientes.map(lambda t: (t[0], t[5]))

clientesAmericanos = clientes.filter(lambda t: t[1] in americanos)
clientesEuropeos = clientes.filter(lambda t: t[1] in europeos)

print(clientesEuropeos.count() > clientesAmericanos.count())

# ------------------ OTRA SOLUCIÓN USANDO AGGREGATE ------------------

# en [0] cuenta por Europeos; en [1] cuenta por Americanos
def calcularClientes(res, og):

    if (og[1] in europeos):
        return (res[0] + 1, res[1])
    
    if (og[1] in americanos):
        return (res[0], res[1] + 1)


clientesCount = clientes.aggregate((0, 0), calcularClientes, (lambda t1,t2: (t1[0] + t2[0], t1[1] + t2[1])))

print(clientesCount[0] >= clientesCount[1])

#b. El promedio de edad de los clientes americanos es menor que el de los europeos
from datetime import date

def crearFecha(strDate):
  [ año, mes, dia ] = strDate.split("-")
  return date(int(año), int(mes), int(dia))

def getEdad(date):
  hoy = date.today()
  return hoy.year - date.year - ((hoy.month, hoy.day) < (date.month, date.day))

# En [0][1] escribe el conteo y el total de Europeos; en [2][3] escribe el conteo y total de Americanos
def calcularEdad(res, og):
    if (og[2] in europeos):
        return (res[0] + og[1], res[1] + 1, res[2], res[3])
    if (og[2] in americanos):
        return (res[0], res[1], res[2] + og[1], res[3] + 1)

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[0], getEdad(crearFecha(t[4])), t[5]))

edadesClientes = clientes.aggregate((0, 0, 0, 0),
                              calcularEdad,
                              (lambda r1, r2: (r1[0] + r2[0],
                                               r1[1] + r2[1],
                                               r1[2] + r2[2],
                                               r1[3] + r2[3])
                            ))

print((edadesClientes[0] / edadesClientes[1]) > (edadesClientes[2] / edadesClientes[3]))

# c. Los americanos deben más plata que los europeos (un cliente debe plata si la suma de montos de todas sus cajas de ahorro es negativa)

europeos = [ "ITA", "ESP" ]
americanos = [ "BRA", "ARG", "VEN", "COL", "BOL", "ECU", "URU", "PAR", "CHI", "PER" ]

# c. Los americanos deben más plata que los europeos (un cliente debe plata si la suma de montos de todas sus cajas de ahorro es negativa)

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[0], ("AMERICA" if t[5] in americanos else "EUROPA") ) )

cajas = cajas.map(lambda t: t.split("\t"))
cajas = cajas.map(lambda t: (t[1], (t[2] )) )

clientes = clientes.join(cajas)

clientes = clientes.reduceByKey(lambda t1,t2: (t1[0], float(t1[1]) + float(t2[1]) ) )

clientes = clientes.map(lambda t: (t[1][0], t[1][1]) )
clientes = clientes.reduceByKey(lambda t1, t2: (float(t1) + float(t2) ))

resultados = clientes.collect()

print(resultados)
# No sé si se puede garantizar que en [0] SIEMPRE va a estar Europa y en [1] SIEMPRE va a estar America
print((resultados[0][1] < 0) and (resultados[0][1] < resultados[1][1]))

# d. Los clientes americanos suelen sacar, en promedio, préstamos con mayor cantidad de cuotas que los europeos

"""
  ESTE NO LO VOY A HACER zzzz
"""
