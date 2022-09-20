# 6) Una empresa proveedora de internet realizó una encuesta para conocer el grado de satisfacción de sus clientes,
# en un formulario web los clientes debían completar un campo con los textos "Muy satisfecho", "Algo satisfecho", "Poco satisfecho",
# “Disconforme” o "Muy disconforme". Utilice el dataset Encuesta para saber cuántos clientes están en cada una de las cinco categorías.

import difflib

# Tengo que estandarizar la información en el map
def fmap(key, value, context):
  options = ["MUY SATISFECHO","ALGO SATISFECHO","POCO SATISFECHO","DISCONFORME","MUY DISCONFORME"]
  v = value.upper()
  similar = difflib.get_close_matches(v, options) # Los retorna en orden de similitud (de menor a mayor)
  context.write(similar[0], 1)
        
def fred(key, values, context):
    total = 0
    for v in values:
      total = total + 1
    context.write(key, total)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()