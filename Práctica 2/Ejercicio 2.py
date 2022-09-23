# 2) Implemente una función Combiner para el problema del WordCount

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)

def fcomb(key, value, context):
    c=0
    for v in value:
        c=c+1
    context.write(key, c)
        
def fred(key, values, context):
    c=0
    for v in values:
        c=c+v
    context.write(key, c)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()