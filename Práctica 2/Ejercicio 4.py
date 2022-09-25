# 4) Utilice el dataset Libros para implementar una aplicación MapReduce que devuelva como salida todos los párrafos que tienen una longitud mayor al promedio.

inputDir = root_path + "WordCount/input/"
outputDir = root_path + "WordCount/output/"
tempDir = root_path + "WordCount/temp/"

def fmap(key, value, context):
    words = value.split('. \n')
    for w in words:
      c = 0
      palabrasParrafo = len(w)
      context.write(1, (w, palabrasParrafo))
        
def fred(key, values, context):
    cont = 0
    total = 0
    for v in values:
      total = total + 1
      cont = cont + v[1]
    context.write(1, cont / total)

def fmap2(key, value, context):
    words = value.split('. \n')
    for w in words:
      c = 0
      palabrasParrafo = len(w)
      if (palabrasParrafo > float(context["promedio"])):
        context.write(1, w)

def fred2(key, values, context):
  for v in values:
    context.write(key, v)

job = Job(inputDir,tempDir,fmap,fred)

success = job.waitForCompletion()

job2 = Job(inputDir, outputDir, fmap2, fred2)

# No se me ocurrió otra manera para setear el parámetro del promedio (PREGUNTAR)
with open(tempDir + "/output.txt") as f:
  job2.setParams({"promedio":(f.readline().split()[1])})

success = job2.waitForCompletion()