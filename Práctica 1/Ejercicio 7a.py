# 7.a) Se desea saber el nombre del inversionista más joven

from datetime import date

def fmap(key, value, context):
  nombre, dia, mes, año, importe = value.split()
  # Almaceno como valor una tupla (fecha nac,nombre)
  context.write(1, (date(int(año), int(mes), int(dia)), nombre))
        
def fred(key, values, context):
    max = date(1, 1, 1)
    maxName = ''
    for v in values:
      if (v[0] > max):
        max = v[0]
        maxName = v[1]
    context.write(key, maxName)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()