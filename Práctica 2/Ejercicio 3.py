#  3) Implemente un job MapReduce para calcular el máximo, mínimo, promedio y desvío stándard de las ocurrencias de todas las palabras del dataset Libros.

inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"
tempDir = root_path + "WordCount/temp/"

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)

def fred(key, values, context):
  c=0
  for v in values:
    c=c+1
  context.write(key, c)

def fmap2(key, value, context):
  context.write(1, (key, value))

def fred2(key, values, context):
  max = 0
  palabraMax = ''

  min = 999999
  palabraMin = ''

  total = 0
  cont = 0

  for v in values:
    [ palabra, valor ] = v.split()
    valor = int(valor)
    total += 1
    cont += valor

    if (valor > max):
      max = valor
      palabraMax = palabra
    
    if (valor < min):
      min = valor
      palabraMin = palabra

  context.write("MAYOR",(palabraMax, max))
  context.write("MENOR",(palabraMin, min))
  context.write("PROMEDIO", cont / total)

job = Job(inputDir, tempDir, fmap, fred)

success = job.waitForCompletion()

job2 = Job(tempDir, outputDir, fmap2, fred2)

success2 = job2.waitForCompletion()

# Me faltó hacer el desvío estándar pero creo que necesito un 3er job