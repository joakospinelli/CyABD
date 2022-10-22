# 5) Utilizando el dataset Banco escriba un script que permita calcular el factor de riesgo de todos sus clientes.

# A: saldo total entre todas las cajas de ahorro
# B: cantidad de cajas de ahorro
# C: cantidad de cajas de ahorro con saldo negativo
# D: monto total de todos los préstamos
# E: promedio de cuotas entre todos los préstamos
# F: cantidad de préstamos

# Cliente: <ID_Cliente, nombre, apellido, DNI, fecha de nacimiento, nacionalidad>
# CajaDeAhorro: <ID_Caja, ID_Cliente, saldo>
# Prestamos: <ID_Caja, cuotas, monto>

clientes = sc.textFile(root_path + "Banco/input/Clientes")
cajas = sc.textFile(root_path + "Banco/input/CajasDeAhorro")
prestamos = sc.textFile(root_path + "Banco/input/Prestamos")

clientes = clientes.map(lambda t : t.split("\t"))
cajas = cajas.map(lambda t: t.split("\t"))
prestamos = prestamos.map(lambda t: t.split("\t"))

clientes = clientes.map(lambda t: (int(t[0]), 0) )
cajas = cajas.map(lambda t: (t[0], (int(t[1]), float(t[2])) ))
prestamos = prestamos.map(lambda t: (t[0], (int(t[1]), float(t[2]) ) ))

cajasPrestamos = cajas.join(prestamos)
cajasPrestamos = cajasPrestamos.map(lambda t: (t[1][0][0], (t[1][0][1], t[1][1][0], t[1][1][1])) )

clientes = clientes.join(cajasPrestamos)
clientes = clientes.mapValues(lambda t: (t[1][0], t[1][1], t[1][2]) )

# En 0,1,2,3,4 cuento A, B, C, D, E, F. El "E" (promedio de préstamos) lo calculo al final con D / F
def contarFR(res,og):
  return (
    res[0] + og[0],
    res[1] + 1,
    res[2] + 1 if og[0] < 0 else res[2],
    res[3] + og[2],
    res[4] + og[1],
    res[5] + 1
  )

clientes = clientes.aggregateByKey( (0,0,0,0,0,0),
                                   contarFR,
                                   lambda r1, r2: (
                                       r1[0] + r2[0],
                                       r1[1] + r2[1],
                                       r1[2] + r2[2],
                                       r1[3] + r2[3],
                                       r1[4] + r2[4],
                                       r1[5] + r2[5]
                                   ))

def calcularFR(t):
  promPrestamos = t[4] / t[5]
  return (((t[3] / promPrestamos) + 0.001) ** t[5]) / ((t[0] / t[1]) ** (1 / (((t[1] - t[2]) + 1))))

clientes = clientes.mapValues(calcularFR)

# saveAsTextFile para usarlo en el prox. ejercicio
clientes.saveAsTextFile(root_path + "Banco/output/SparkOutput")
print(clientes.collect())