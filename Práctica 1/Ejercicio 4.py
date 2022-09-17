#  4) Modifique el proyecto WordCount para contar cuántas vocales, consonantes, dígitos, espacios y otros caracteres posee el data set Libros.

def fmap(key, value, context):
    digitos = '0123456789'
    vocales = 'aeiouAEIOUÂÃáéíóúüï'
    consonantes = 'bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ'
    
    for c in value:
        if c in digitos:
            context.write('Digitos', c)
        elif c in vocales:
          context.write('Vocales', c)
        elif c in consonantes:
          context.write('Consonantes', c)
        elif c == ' ':
          context.write('Espacios', c)
        else:
          context.write('Otros', c)

def fred(key, values, context):
    c = 0
    for v in values:
      c=c+1
    context.write(key, c)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()