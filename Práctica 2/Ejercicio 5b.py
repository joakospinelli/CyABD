# 5.b) El dataset 'Website' tiene información sobre el tiempo de permanencia de sus usuarios en cada una de las páginas del sitio.
# Implemente una aplicación MapReduce que calcule el usuario que más páginas distintas visitó

""" ESTA RESPUESTA CREO QUE ESTÁ MAL PORQUE SI UN USUARIO VISITA LA MISMA PÁGINA MÁS DE UNA VEZ LA CUENTA COMO UNA DISTINTA EN C/U
inputDir = root_path + "Website/input/"
outputDir = root_path + "Website/output/"
tempDir = root_path + "Website/temp/"

def fmap(key, value, context):
  [ pagina, *_ ] = value.split()
  context.write(key, pagina)

def fred(key,value,context):
  cont = 0

  for v in value:
    cont += 1
  context.write(key, cont)

def fmap2(key, value, context):
  context.write(1, (key, value))

def fred2(key, value, context):
  max = -1
  idUsuario = ''

  for v in value:
    if (int(v[1]) > max):
      max = int(v[1])
      idUsuario = v[0]
  context.write("MAXIMO", (idUsuario, max))

job = Job(inputDir,tempDir,fmap,fred)

success = job.waitForCompletion()

job2 = Job(tempDir,outputDir,fmap2,fred2)

success = job2.waitForCompletion() """

# Esta es correcta pero me parece un poco exagerado usar 3 jobs
inputDir = root_path + "Website/input/"
outputDir = root_path + "Website/output/"
tempDir = root_path + "Website/temp/"

def fmap(key, value, context):
  [ pagina, *_ ] = value.split()
  context.write((key, pagina), 1)

def fred(key,value,context):
  context.write(key[0],key[1]) # De esta manera me aseguro de no tener páginas repetidas por usuario

def fmap2(key, value, context):
  context.write(key, value)

def fred2(key, value, context):
  cont = 0

  for v in value:
    cont += 1
  context.write(key, cont)

def fmap3(key, value, context):
  context.write(1, (key, value))

def fred3(key, value, context):
  max = -1
  idUsuario = ''

  for v in value:
    if int(v[1]) > max:
      max = int(v[1])
      idUsuario = v[0]
  context.write("MAXIMO",(idUsuario, max))

job = Job(inputDir,tempDir,fmap,fred)
success = job.waitForCompletion()

job2 = Job(tempDir,tempDir,fmap2,fred2)
success = job2.waitForCompletion()

job3 = Job(tempDir,outputDir,fmap3,fred3)
success = job3.waitForCompletion()
