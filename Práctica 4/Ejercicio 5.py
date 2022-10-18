norte = sc.textFile(root_path + "EstacionesMeteorologicas/input/Norte/")
sur = sc.textFile(root_path + "EstacionesMeteorologicas/input/Sur")

def getCelsius(t):
  return (t - 32) / 1.8

norte = norte.map(lambda t: t.split("\t"))
norte = norte.map(lambda t: ( int(t[0]), t[1], getCelsius(float(t[2])), int(t[3]), float(t[4]) * 10 ) )

sur = sur.map(lambda t: t.split("\t"))
sur = sur.map(lambda t: ( int(t[0]), t[1], float(t[2]), int(t[3]), float(t[4]) ) )

registros = norte.union(sur)

acumulados = registros.reduce(lambda t1,t2: (0, 0, t1[2] + t2[2], t1[3] + t2[3], t1[4] + t2[4] ) )

# Promedios

print("TEMPERATURA PROMEDIO: ", acumulados[2] / registros.count())
print("HUMEDAD PROMEDIO: ", acumulados[3] / registros.count())
print("PRECIPITACIONES PROMEDIO:. ", acumulados[4] / registros.count())