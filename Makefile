#------------------------------------------------------#
#  ___  ___      _         __ _ _                      #
#  |  \/  |     | |       / _(_) |                     #
#  | .  . | __ _| | _____| |_ _| | ___                 #
#  | |\/| |/ _` | |/ / _ \  _| | |/ _ \                #
#  | |  | | (_| |   <  __/ | | | |  __/                #
#  \_|  |_/\__,_|_|\_\___|_| |_|_|\___|                #
#                                                      #
#  Si quieren probar esto en sus compus recuerden      #
#	 modificar el MASTER, poniendo a la cantidad de    #
#	 threads que tiene su maquina. En linux para saber #
#	 eso de manera facil es con la herramienta htop:   #
#	 para instalarla: sudo apt-get install htop        #
#	 y para probarla en el terminal escriban htop      #
#	 para salirse de alli presionen Q. En esa pantalla #
#	 que sale dice la cantidad que tienen.             #
#                                                      #
#	 Si tienen 4 threads por ejemplo:                  #
#	 MASTER="local[4]"                                 #
#------------------------------------------------------#

MASTER="local[4]"
SOLVER=SlidingBfsSpark.py
CHECK=check.py

.PHONY: clean

default: run-small

run-small:
	rm -rf small-puzzle-out.txt
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) --master=$(MASTER) --output="small-puzzle-out.txt" --height=2 --width=2

run-medium:
	rm -rf medium-puzzle-out.txt
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) --master=$(MASTER) --output="medium-puzzle-out.txt" --height=3 --width=3

run-large:
	rm -rf large-puzzle-out.txt
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) --master=$(MASTER) --output="large-puzzle-out.txt" --height=5 --width=2

check:
	rm -rf resultados.txt
	PYTHONWARNINGS="ignore" spark-submit $(CHECK) --master=$(MASTER)

clean:
	rm -rf resultados.txt
	rm -rf *-puzzle-out*
	rm -rf *.pyc
