norte = sc.textFile(root_path + "EstacionesMeteorologicas/input/Norte/")
sur = sc.textFile(root_path + "EstacionesMeteorologicas/input/Sur")

def getCelsius(t):
  return (t - 32) / 1.8

# <ID_Estación, fecha_registro, temperatura, humedad, precipitación>

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

# Máximo-mínimo TEMPERATURA

print("TEMPERATURA MÁS FRÍA: ", registros.reduce(lambda t1,t2: t1 if t1[2] <= t2[2] else t2))
print("TEMPERATURA MÁS CALUROSA: ", registros.reduce(lambda t1,t2: t1 if t1[2] >= t2[2] else t2))

# Máximo-mínimo HUMEDAD

print("MAYOR HUMEDAD: ", registros.reduce(lambda t1,t2: t1 if t1[3] <= t2[3] else t2))
print("MENOR HUMEDAD: ", registros.reduce(lambda t1,t2: t1 if t1[3] >= t2[3] else t2))

# Máximo-mínimo PRECIPITACIONES

print("MÁS PRECIPITACIONES: ", registros.reduce(lambda t1,t2: t1 if t1[4] <= t2[4] else t2))
print("MENOS PRECIPITACIONES: ", registros.reduce(lambda t1,t2: t1 if t1[4] >= t2[4] else t2))