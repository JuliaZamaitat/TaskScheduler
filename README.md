# TaskScheduler

This is the labwork done by Julia Zamaitat for the course Resource 
Management for Embedded System by Prof. Pierson at University Paul Sabatier 3.
The goal of this lab is to implement different scheduling algorithms.

The algorithms can be run with these commands on the command line:
  - python3 main.py fifo_basic src/inputs/test1.txt
  - python3 main.py fifo src/inputs/test1.txt  (for energy/power caps)
  - python3 main.py edf_basic src/inputs/test1.txt
  - python3 main.py edf src/inputs/test1.txt (for energy/power caps)
  - python3 main.py round src/inputs/test1.txt
  - python3 main.py rms src/inputs/test1.txt

Different test file for Wavefront and CPM using arrival time 0 for all tasks:
  - python3 main.py wavefront src/inputs/test2.txt
  - python3 main.py cpm src/inputs/test2.txt
