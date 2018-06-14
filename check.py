import os
import time
import argparse
from pyspark import SparkConf
from pyspark import SparkContext

# OUTPUTS
s1 = "El tiempo fue menor o igual al esperado"
s2 = "El tiempo no fue menor o igual al esperado"
s3 = "La cantidad de lineas de su respuesta es correcta"
s4 = "La cantidad de lineas de su respuesta no es correcta"
s5 = "Su respuesta hace match con la nuestra (es igual)"
s6 = "Su respuesta no hace match con la nuestra (no es igual)"


def check(size):
    # Al principio tienen todas las pruebas correctas
    BD1 = True
    BD2 = True
    BD3 = True
    # comenzamos a contar (TIC)
    S = time.time()
    # Corremos su implementacion con el make
    os.system('make run-'+size)
    # terminamos de contar (TOC)
    E = time.time()
    total = E-S
    # Si se tardo mas que el benchmark NO pasa la prueba 1
    if (total > TIMES[size]):
        BD1 = False
    # Ahora a comparar los resultados
    # Verificamos la cantidad de lineas esperadas
    output = SC.textFile(size+'-puzzle-out.txt')
    # Si no tienen el grafo completo NO pasa la prueba 2
    if output.count() != LINEAS[size]:
        BD2 = False
    # Cargamos expected
    expected = SC.textFile('./expected/'+size+'-puzzle-expected.txt')
    # Miramos que los resultados sean iguales a los de expected
    # para eso miramos que le falta a su output para que se parezca al
    # de nosotros
    # Si su output tiene algo de mas, estaria mal
    if  expected.subtract(output).count() != 0:
        BD3 = False
    # BD3 puede ser true si las lineas son diferentes
    BD3 = BD2 and BD3

    return (BD1, BD2, BD3, total)

def preprocess(r, t):

    a = s1 if t[0] else s2
    b = s3 if t[1] else s4
    c = s5 if t[2] else s6
    d = "All Ok" if t[0] and t[1] and t[2] else "BAD :("

    r = """
        Tablero de: {0}
        -----------------
        Tiempo: {1:.2f} segundos
        {2}
        {3}
        {4}
    """.format(r, t[3], b, c, d)
    return r


def main(args):
    global TIMES, MASTER, LINEAS, SC
    conf = SparkConf().setAppName('Check').setMaster(args.master)
    SC = SparkContext(conf=conf)
    TIMES = {
        'small':args.time1+5,
        'medium':args.time2+5,
        'large':args.time3+5
    }
    LINEAS = {
        'small':12,
        'medium':181440,
        'large':1814400
    }
    r1 = preprocess('2x2', check('small'))
    r2 = preprocess('3x3', check('medium'))
    r3 = preprocess('5x2', check('large'))
    with open('resultados.txt', 'w') as f:
        f.write(r1+'\n')
        f.write(r2+'\n')
        f.write(r3+'\n')
        f.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verifica el output')
    parser.add_argument("-M", "--master", type=str, default="local[8]",help="url del master para este trabajo")
    parser.add_argument("-t1", "--time1", type=int, default=500, help="tiempo prueba 1")
    parser.add_argument("-t2", "--time2", type=int, default=500, help="tiempo prueba 2")
    parser.add_argument("-t3", "--time3", type=int, default=500, help="tiempo prueba 3")
    args = parser.parse_args()
    main(args)
