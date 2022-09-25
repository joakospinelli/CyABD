# 5.a) El dataset 'Website' tiene información sobre el tiempo de permanencia de sus usuarios en cada una de las páginas del sitio.
# Implemente una aplicación MapReduce que calcule la página más visitada (la página en la que más tiempo permaneció) para cada usuario

inputDir = root_path + "Website/input/"
outputDir = root_path + "Website/output/"
tempDir = root_path + "Website/temp/"

def fmap(key, value, context):
  [ pag, tiempo ] = value.split()
  context.write((key, pag), tiempo) 

# Este anda pero el combiner no se ejecuta nunca (PREGUNTAR)
def fcomb(key, value, context):
  c = 0
  for v in value:
    c = c + int(v)
  context.write(key, c)

def fred(key, value, context):
  cont = 0
  for v in value:
    cont = cont + int(v)
  context.write(key[0],(key[1],cont))

def fmap2(key, value, context):
  [ pagina, tiempo ] = value.split()
  context.write(key, (pagina, tiempo))

def fred2(key, value, context):
  max = -1
  idPag = ''

  for v in value:
    if (int(v[1]) > max):
      idPag = v[0]
      max = int(v[1])
  context.write(key, (idPag, max))

job = Job(inputDir, tempDir, fmap, fred)

job.setCombiner(fcomb)

success = job.waitForCompletion()

job2 = Job(tempDir, outputDir, fmap2, fred2)

success = job2.waitForCompletion()