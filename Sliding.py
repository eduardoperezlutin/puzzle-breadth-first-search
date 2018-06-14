"""
  ___ _ _    _ _
 / __| (_)__| (_)_ _  __ _
 \__ \ | / _` | | ' \/ _` |
 |___/_|_\__,_|_|_||_\__, |
                     |___/
"""

import string

def solution(W, H):
    """
        Retorna la solucion del sliding puzzle para las dimensiones dadas
    """
    return tuple(string.ascii_uppercase[0:(W*H) - 1] + "-")


def swap(board, i, j):
    """
        Simula lo de mover las piezas del tablero
    """
    boardL = list(board)
    boardL[i], boardL[j] = boardL[j], boardL[i]
    return tuple(boardL)


def children(W, H, board):
    """
        Retorna una lista de todos los tableros hijos de una configuracion
        de tablero dada.
    """
    i = board.index("-")
    children = []
    if i % W != 0:      # no en el borde izquierdo
        children.append(swap(board, i, i-1))
    if i % W != (W-1):  # no en el borde derecho
        children.append(swap(board, i, i+1))
    if i >= W:          # no en el borde superior
        children.append(swap(board, i, i-W))
    if i < W*(H-1):     # no en el borde inferior
        children.append(swap(board, i, i+W))
    return children
