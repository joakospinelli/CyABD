# 3) ¿La operación de Inner Join mejora su performance con la función *combiner*?

La performance de la operación no va a mejorar porque en el _reduce_ va a tener que comparar ambas tablas, que provienen de archivos diferentes. Sin embargo, usar un *combiner* sí puede mejorar la performance si necesito realizar alguna operación intermedia sobre las tuplas antes de realizar el Inner Join.