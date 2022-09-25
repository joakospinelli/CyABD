# 5.c) El dataset 'Website' tiene información sobre el tiempo de permanencia de sus usuarios en cada una de las páginas del sitio.
# La página más visitada (en cuanto a cantidad de visitas, sin importar el tiempo de permanencia) por todos los usuarios.

inputDir = root_path + "Website/input/"
outputDir = root_path + "Website/output/"
tempDir = root_path + "Website/temp/"

def fmap(key, value, context):
  [ pagina, *_ ] = value.split()
  context.write(pagina, 1)

def fcomb(key, value, context):
  cont = 0
  for v in value:
    cont += 1
  context.write(key, cont)

def fred(key, value, context):
  cont = 0
  for v in value:
    cont += int(v)
  context.write(key, cont)

def fmap2(key, value, context):
  context.write(1, (key, value))

def fred2(key, value, context):
  max = -1
  idPag = ''

  for v in value:
    if (int(v[1]) > max):
      max = int(v[1])
      idPag = v[0]
  context.write(idPag, max)

job = Job(inputDir,tempDir,fmap,fred)

job.setCombiner(fcomb)

success = job.waitForCompletion()

job2 = Job(tempDir,outputDir,fmap2,fred2)

success = job2.waitForCompletion()