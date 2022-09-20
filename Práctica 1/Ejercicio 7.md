# 7) El dataset Inversionistas posee los nombres, dni, fecha de nacimiento (día, mes y año como campos separados) e importe invertido por diferentes personas en la apertura de un nuevo negocio en la ciudad (LOS 3 PROBLEMAS A RESOLVER SON 7.A, 7.B Y 7.C). ¿Se puede resolver los tres problemas en un único job?

Si bien podrían juntarse toda la información en una única función `map` (y almacenarla en claves distintas), para cada uno de los problemas se necesita que la función `reduce` trabaje de manera distinta.

Esto podría hacerse en un único Job con un condicional en el `reduce` para que haga algo distinto según la clave con la que está trabajando, pero no es lo más óptimo porque tendría que agregar un _if_ nuevo por cada clave con la que quiera trabajar.

Igualmente si lo quisiera hacer con un _if_ quedaría así:
```python
from datetime import date

def age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

def fmap(key, value, context):
  value = value.split('\n')
  for v in value:
    nombre, dia, mes, año, importe = v.split()

    context.write("BUSCAR MENOR", (date(int(año), int(mes), int(dia)), nombre))
    context.write("TOTAL IMPORTE", int(importe))
    context.write("PROMEDIO EDAD", age(date(int(año), int(mes), int(dia))))
        
def fred(key, values, context):
    if key == "BUSCAR MENOR":
        max = date(1, 1, 1)
        maxName = ''
        for v in values:
            if (v[0] > max):
                max = v[0]
                maxName = v[1]
            context.write(key, maxName)
    elif key == "TOTAL IMPORTE":
      total = 0
      for v in values:
        total = total + v
      context.write(key, total)
    else:
      total = 0
      cont = 0
      for v in values:
        cont = cont + v
        total = total + 1
      context.write(key, cont / total)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
```