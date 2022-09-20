# 7.b) Se desea saber el total del importe invertido por todos los inversionistas

def fmap(key, value, context):
  nombre, dia, mes, año, importe = value.split()
  context.write(1, int(importe))
        
def fred(key, values, context):
    total = 0
    for v in values:
      total = total + v
    context.write(key, total)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()