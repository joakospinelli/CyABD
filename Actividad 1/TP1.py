#Codigo
#Conexión con drive
root_path = 'drive/My Drive/Colab Notebooks/'

from google.colab import drive
drive.mount('/content/drive/', force_remount=True)

#Importar el emulador
import sys
sys.path.append('/content' + root_path)

#Codigo
inputDir1 = root_path + "Dataset/input/Enero/"
inputDir2 = root_path + "Dataset/input/Febrero/"
tempDir = root_path + "Dataset/temp/"
outputDir = root_path + "Dataset/output/"

# ACTIVIDAD 1
# Implemente un job MapReduce que resuelva la unión de dos datasets (meses).

def fmap(key, value, context):
  context.write(value, 1)

def fred(key, values, context):
  context.write(1, key)

job = Job(inputDir1,outputDir,fmap,fred)
job.addInputPath(inputDir2, fmap)

success = job.waitForCompletion()

# ACTIVIDAD 2
# Implemente un job MapReduce que resuelva la intersección de dos datasets (meses). 

def fmap(key, value, context):
  context.write(value, "1")

def fmap2(key, value, context):
  context.write(value, "2")

def fred(key, values, context):
  esta1 = False
  esta2 = False

  for v in values:
    if (v == "1"):
      esta1 = True
    elif (v == "2"):
      esta2 = True
  
  if (esta1 and esta2):
    context.write(key, 1)

job = Job(inputDir1,outputDir,fmap,fred)
job.addInputPath(inputDir2, fmap2)

success = job.waitForCompletion()

# ACTIVIDAD 3
# Implemente un job MapReduce que permita determinar si dos datasets (meses) son 
# iguales. Dos conjuntos son iguales si ambos tienen los mismos elementos sin importar
# cuántas veces se repitan dentro de cada conjunto.
import os

def fmap(key, value, context):
  context.write(value, "1")

def fmap2(key, value, context):
  context.write(value, "2")

# Probar anotando los que no están en ambos

def fred(key, value, context):
  esta1 = False
  esta2 = False

  for v in value:
    if (v == "1"):
      esta1 = True
    else:
      esta2 = True
  
  if not (esta1 and esta2):
    context.write(key, 1)

job = Job(inputDir1,tempDir,fmap,fred)
job.addInputPath(inputDir2, fmap2)

success = job.waitForCompletion()

f = os.stat(tempDir + "output.txt")

if (f.st_size > 0):
  print("NO son iguales")
else:
  print("Los datasets son iguales")

# ACTIVIDAD 4

#i
inputDir1 = root_path + "Dataset/input/Enero/"
inputDir2 = root_path + "Dataset/input/Febrero/"
inputDir3 = root_path + "Dataset/input/Marzo/"
inputDir4 = root_path + "Dataset/input/Abril/"
inputDir5 = root_path + "Dataset/input/Mayo/"
outputDir = root_path + "Dataset/output/"

def fmap(key, value, context):
  context.write(value, 1)

def fred(key, values, context):
  context.write(1, key)

job = Job(inputDir1,outputDir,fmap,fred)
job.addInputPath(inputDir2, fmap)
job.addInputPath(inputDir3, fmap)
job.addInputPath(inputDir4, fmap)
job.addInputPath(inputDir5, fmap)
success = job.waitForCompletion()

#ii

def fmap(key, value, context):
  context.write(value, "0")

def fmap2(key, value, context):
  context.write(value, "1")

def fmap3(key, value, context):
  context.write(value, "2")

def fmap4(key, value, context):
  context.write(value, "3")

def fmap5(key, value, context):
  context.write(value, "4")

def fred(key, values, context):
  esta = []

  for i in range(context["n"]): # Inicializo lista en False
    esta.append(False)

  for v in values:
    esta[int(v)] = True
  
  if (all(esta)):
    context.write(key, 1)

job = Job(inputDir1,outputDir,fmap,fred)

job.setParams({"n": 5}) # Cantidad de datasets a probar

job.addInputPath(inputDir2, fmap2)
job.addInputPath(inputDir3, fmap3)
job.addInputPath(inputDir4, fmap4)
job.addInputPath(inputDir5, fmap5)
success = job.waitForCompletion()

# iii

def fmap(key, value, context, num):
  context.write(value, "0")

def fmap2(key, value, context):
  context.write(value, "1")

def fmap3(key, value, context):
  context.write(value, "2")

def fmap4(key, value, context):
  context.write(value, "3")

def fmap5(key, value, context):
  context.write(value, "4")

def fred(key, values, context):
  esta = []

  for i in range(context["n"]): # Inicializo lista en False
    esta.append(False)

  for v in values:
    esta[int(v)] = True
  
  if not (all(esta)):
    context.write(key, 1)

job = Job(inputDir1,outputDir,fmap,fred)

job.setParams({"n": 5}) # Cantidad de datasets a probar

job.addInputPath(inputDir2, fmap2)
job.addInputPath(inputDir3, fmap3)
job.addInputPath(inputDir4, fmap4)
job.addInputPath(inputDir5, fmap5)

success = job.waitForCompletion()

f = os.stat(tempDir + "output.txt")

if (f.st_size > 0):
  print("NO son iguales")
else:
  print("Los datasets son iguales")