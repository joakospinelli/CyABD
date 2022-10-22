# 6) Realice un script que permita imprimir, por país, los nombres de los clientes cuyo factor de riesgo es menor a 2.

clientes = sc.textFile(root_path + "Banco/input/Clientes")
factorRiesgo = sc.textFile(root_path + "Banco/output/SparkOutput") # Output del ejercicio anterior

clientes = clientes.map(lambda t: t.split("\t"))
clientes = clientes.map(lambda t: (t[0], (t[1] + ' ' + t[2], t[5])))

factorRiesgo = factorRiesgo.map(lambda t: t.replace('(','').replace(')','') )
factorRiesgo = factorRiesgo.map(lambda t: t.split(', '))
factorRiesgo = factorRiesgo.map(lambda t: (t[0], t[1]))

clientes = clientes.join(factorRiesgo)
clientes = clientes.filter(lambda t: complex(t[1][1]).real < 2)

clientes = clientes.map(lambda t: (t[1][0][1], t[1][0][0]) )

def agregarLista(res,og):
  res.append(og)
  return res

def extenderLista(r1,r2):
  r1.extend(r2)
  return r1

clientes = clientes.aggregateByKey(list(),
                                   agregarLista,
                                   extenderLista
                                  )

print(clientes.collect())