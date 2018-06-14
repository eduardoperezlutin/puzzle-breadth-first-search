

"""
  _         _    _  __
 | |   __ _| |__/ |/  \.
 | |__/ _` | '_ \ | () |
 |____\__,_|_.__/_|\__/


Este archivo es 'guia', ustedes tienen que ver la manera de convertir
este algoritmo en un formato compatible con MapReduce: una funcion map y
una funcion reduce.

NOTA: Esta es una manera muy ineficiente de resolver el problema, por tal
motivo no es una buena idea de que usen este archivo para probar tableros
de width y height grandes, bueno a menos de que tengan tiempo y espacio de
disco.

Un poco de info:

ustedes van a probar en este laboratorio resolver tableros de 2x2, 3x3 y 5x2

La cantidad de posiciones unicas que hay en un tablero se representa por la
siguiente ecuacion:

POS = (width * height)!
      -----------------
             2

Tablero de 2x2:

para un tablero de 2x2 hay 4!/2 = 12 posiciones unicas que pueden llegar
a la solucion del puzzle.

Tablero de 3x3:

para un tablero de 3x3 hay 9!/2 = 1814400 posiciones unicas, bastante pero
un poco aceptable digamos ...

Tablero de 5x2:

para un tablero de 5x2 hay 10!/2 = 1814400, POSICIONES! unicas, aqui es
donde las cosas se ponen un poco grandes y esto no es nada ...

Tablero de 4x3: (Solo en amazon lo podrian resolver :( )

239500800 posicones unicas -> ¿alguien sabe como decir ese numero xD?

y el 15-puzzle que conocen (este tambien necesitaria amazon y bastante tiempo):

1.046139494×10^13 posiciones unicas D:

FIN INFORMACION
"""

import argparse
import Sliding

from pprint import pprint

level_to_pos = {}  # map de level a posicion
pos_to_level = {}  # map de posicion a level


def slidingBfsSolver(puzzle, width, height, max_level=-1):
    """
        BF visita todo el grafo del puzzle, construye las estructuras:
        * level_to_pos
        * pos_to_level
    """
    solution = puzzle  # Solucion del puzzle
    level = 0  # La solucion es level 0
    level_to_pos[level] = [solution]  # el level 0 solo consiste de la solucion
    pos_to_level[solution] = level

    #  Mientras existan posiciones en el level de la frontera
    while level_to_pos[level] and (max_level == -1 or level < max_level):
        level += 1  # Incrementamos el level
        level_to_pos[level] = []  # Creamos una lista vacia en el nuevo level
        # Para cada posicion en el ultimo level (antes de aumentarlo)
        for position in level_to_pos[level-1]:
            # Para cada hijo de cada posicion del for de arriba
            for child in Sliding.children(width, height, position):
                # Si es primera vez que miramos al child
                if child not in pos_to_level:
                    # Actualizamos los mappings para recordarlo, y que van a
                    # ser parte de la nueva frontera
                    pos_to_level[child] = level
                    level_to_pos[level].append(child)

    del level_to_pos[level]  # El ultimo level siempre esta vacio, lo eliminan
    pprint(level_to_pos)


# Main
def main(args):
    p = Sliding.solution(args.width, args.height)
    slidingBfsSolver(p, args.width, args.height)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Retorna la solucion completa del grafo")
    parser.add_argument("-H", "--height", type=int, default=2,
                        help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
                        help="width of the puzzle")
    args = parser.parse_args()
    main()
