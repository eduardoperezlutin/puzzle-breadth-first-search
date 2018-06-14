#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  _         _    _  __
 | |   __ _| |__/ |/  \.
 | |__/ _` | '_ \ | () |
 |____\__,_|_.__/_|\__/

  ___ ___ ___   ___                _
 | _ ) __/ __| / __|_ __  __ _ _ _| |__
 | _ \ _|\__ \ \__ \ '_ \/ _` | '_| / /
 |___/_| |___/ |___/ .__/\__,_|_| |_\_\.
                   |_|
"""

import Sliding
import argparse

from pyspark import SparkConf
from pyspark import SparkContext

import time

import math

"""
    AQUI DEFINAN SUS FUNCIONES MAP Y REDUCE, HELPER FUNCTIONS ETC

    NOTA: NO TRATEN DE HACER TODO EN UNA SOLA FUNCION, ESO ES UNA BAD IDEA.
"""


def bfs_reduce(graph):
    """
    Debe recibir el grafo de la forma:
        [((key), value)]
        ..
        [((posicion), nivel)]
        ..
        [(("A", "B", "C", "-"), 1)]

    y devolver las tuplas que generan el grafo con los nodos
    mas cercanos y sin repetirse.

    Arguments:

    graph : RDD
        Objeto RDD con los niveles y posiciones que representan el grafo.
    
    Returns:

    graph : RDD
        El grafo reducido.
    """
    # aplicar reduceByKey() para dejar en el grafo, 
    # la posicion con el nivel mas bajo (de 0 a n) y asi evitar que se repitan posiciones en el grafo.
    reduced_graph = graph.reduceByKey(lambda a, b: a if a < b else b)

    # max reduce reached
    max_reduce_reached = False

    if ( len(reduced_graph.collect()) == len(graph.collect()) ):
        max_reduce_reached = True

    return reduced_graph, max_reduce_reached

def bfs_map(_sc, position, graph=[], level=1):
    """
    Function to create the graph.

    Arguments:

    _sc : SparkContext()
        Spark Context.

    position : list
        A position. Example: ('A', 'B', 'C', '-')

    graph : RDD
        RDD Graph.
    
    level : int
        Actual level.
    """
    children = get_children(_sc, position, graph)
    graph = add_child_to_graph(_sc, children.flatMap(lambda child: (child,)), graph, level)

    return graph

def get_children(_sc, position, graph):
    """
    Returns the children of a position.

    Arguments:

    _sc : SparkContext
        The Spark Context configurations.

    position : list
        A position. Example: ('A', 'B', 'C', '-')

    graph : RDD
        RDD Object wich represents the graph.
    """  
    # print type(position)
    
    if "tuple" not in str(type(position)):
        print "not tuple"
        children = [Sliding.children(w, h, x) for x in position.collect()]
        print "start parallelizing"
        children = _sc.parallelize(sum(children, []))
        print "children parallelized"
        return children

    children = _sc.parallelize(Sliding.children(w, h, position))   # obtain the children from position obtained
    return children

def add_child_to_graph(_sc, child, graph, level):
    """
    Append childs to the graph.

    Arguments:

    _sc : SparkContext
        The Spark Context configurations.

    child : RDD
        RDD Object with the children to append.

    graph : RDD
        RDD Object wich represents the graph.

    level : int
        Level on wich append the children.
    """

    children = _sc.parallelize([(child,level) for child in child.collect()])    # join the position with the respective level
    children, max_reduce_reached = bfs_reduce(children)                                             # apply reduce function to children to be added to the graph

    graph += children                                                           # add the children found to the graph
    reduced_graph, mrr = bfs_reduce(graph)                                           # reduce graph to leave only the positions in lower levels

    children_of_pos = _sc.parallelize([])                                       # clean the children of the positions in a level
    
    # print "children length %s" % len(children.collect())
    print "reduced length : %s" % len(reduced_graph.collect())

    # if max_reduce_reached == True:
    #     return reduced_graph

    # print "the children sent"
    # print children.collect()

    children_of_pos = get_children(_sc, children.keys().flatMap(lambda x: (x,)), reduced_graph)

    print "children length %s" % len(children_of_pos.collect())

    # for position in children.keys().collect():
    #     children_of_pos += get_children(_sc, position, reduced_graph)           # children of both positions in level

    # stops when the positions in a level dont have more children
    if ( len(reduced_graph.collect()) < graph_length ):
        return add_child_to_graph(_sc, children_of_pos, reduced_graph, level+1) # recursively add children to graph ;)
    else:
        print reduced_graph.collect()
        return reduced_graph

    # time.sleep(5)
    # return add_child_to_graph(_sc, children_of_pos, reduced_graph, level+1) # recursively add children to graph ;)

def solve_sliding_puzzle(master, output, height, width):
    """
        Aqui tienen que construir el RDD Spark y todo lo demas

        @param master: el master que tienen que utilizar
        @param output: la funcion que espera un string para escribir en archivo
        @param height: el height
        @param width: el width
    """
    global w,h,level,graph_length

    level = 0
    w = width
    h = height
    graph_length = ( math.factorial(w * h) / 2 )
    print "graph length %s" % graph_length

    #solucion
    solution = Sliding.solution(w,h)
    # Conf
    conf = SparkConf().setAppName('SlidingBFS').setMaster(master)
    # Spark Context
    sc = SparkContext(conf=conf)

    #######################################
    #  AQUI TIENEN QUE PONER SU SOLUCIÓN  #
    #######################################

    graph = sc.parallelize([(solution, 0)])
    out = sorted(bfs_map(sc, solution, graph).collect(), key=lambda tup: tup[1])

    for line in out:
        output(str(line[1]) + " " + str(line[0]))
    
    sc.stop()

"""
    ---------------------------------------------------------------------
    NO MODIFIQUEN NADA A PARTIR DE AQUI

    Pueden leer el codigo y tratar de entenderlo, pero no es necesario
    que se preocupen de entenderlo.
"""


def main():
    """
        Parsea los argumentos del command line y corre el solver.
        Si ningun argumento es pasado se corre utilizando los valores
        por defecto.
    """
    parser = argparse.ArgumentParser(
            description="Regresa la solucion entera del grafo.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
                        help="url del master para este trabajo")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
                        help="nnombre del output file")
    parser.add_argument("-H", "--height", type=int, default=2,
                        help="height del puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
                        help="width del puzzle")
    args = parser.parse_args()

    # Abre el archivo donde van a escribir la solucion y define
    # Una funcion para escribir en el
    output_file = open(args.output, "w")

    def myWriter(line):
        output_file.write(line + '\n')  # output_file es un upvalue

    writer = myWriter

    # Llama a su solver
    solve_sliding_puzzle(
        args.master,
        writer,
        args.height,
        args.width,
    )

    # cierra el output file
    output_file.close()

# Ejecuta este archivo si se corre directamente
if __name__ == "__main__":
    main()
