# 7.c) Se desea saber el promedio de edad de los inversionistas

from datetime import date
 
def age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

def fmap(key, value, context):
  nombre, dia, mes, año, importe = value.split()
  context.write(1, age(date(int(año), int(mes), int(dia))))
        
def fred(key, values, context):
    total = 0
    cont = 0
    for v in values:
      cont = cont + v
      total = total + 1
    context.write(key, cont / total)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()