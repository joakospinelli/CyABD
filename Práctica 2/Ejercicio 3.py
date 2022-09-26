#  3) Implemente un job MapReduce para calcular el máximo, mínimo, promedio y desvío estándar de las ocurrencias de todas las palabras del dataset Libros.

inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"
tempDir = root_path + "WordCount/temp/"

import math

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)

def fred(key, values, context):
  c=0
  for v in values:
    c=c+v
  context.write(key, c)

def fmap2(key, value, context):
  context.write(1, (key, value))

def fred2(key, values, context):
  total = 0
  cont = 0

  for v in values:
    valor = int(v[1])
    total += 1
    cont += valor

  context.write(1, cont / total)

def fred3(key, value, context):

  max = -1
  palabraMax = ''

  min = 99999
  palabraMin = ''

  acum = 0
  total = 0
  promedio = float(context["promedio"])

  for v in value:
    total += 1
    palabra = v[0]
    valor = int(v[1])

    if valor > max:
      max = valor
      palabraMax = palabra

    if valor < min:
      min = valor
      palabraMin = palabra

    acum += (valor - promedio)**2   

  desvioEstandar = math.sqrt(acum / total)

  context.write("MAXIMO",(palabraMax, max))
  context.write("MINIMO",(palabraMin, min))
  context.write("PROMEDIO",context["promedio"])
  context.write("DESVIO ESTANDAR",desvioEstandar)

job = Job(inputDir, tempDir, fmap, fred)
success = job.waitForCompletion()

job2 = Job(tempDir, outputDir, fmap2, fred2)
success2 = job2.waitForCompletion()

job3 = Job(tempDir, outputDir, fmap2, fred3)

with open(outputDir + "/output.txt") as f:
  job3.setParams({"promedio":(f.readline().split()[1])})

success3 = job3.waitForCompletion()