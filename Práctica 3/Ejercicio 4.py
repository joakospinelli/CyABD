# 4)  La mediana es el "número en el medio" de una lista ordenada de números.
# Implemente una solución MapReduce que permita calcular la mediana de una serie de valores.
# Use como prueba el dataset Website para calcular la mediana del tiempo de permanencia.

inputDir = root_path + "Website/input/"
outputDir = root_path + "Website/output/"
tempDir = root_path + "Website/temp/"

def fmap(key, value, context):
  context.write(1, value)

def cmpSort(key, anotherKey):
  if key == anotherKey:
    return 0
  elif key < anotherKey:
    return -1
  else:
    return 1

def fred(key, values, context):
  c = 0
  for v in values:
    c += 1
  context.write(1, c)

def fmap2(key, value, context):
  [ *_, tiempo ] = value.split()
  context.write(1, tiempo)

def fred2(key, values, context):
  mitad = int(int(context["total"]) / 2)
  c = 0

  for v in values:
    c += 1
    if (c == mitad):
      context.write(1, v)

job = Job(inputDir,tempDir,fmap,fred)

success = job.waitForCompletion()

job2 = Job(inputDir,outputDir,fmap2,fred2)

with open(tempDir + "output.txt") as f:
  job2.setParams({"total":f.readline().split()[1]})

job2.setSortCmp(cmpSort)
success = job2.waitForCompletion()