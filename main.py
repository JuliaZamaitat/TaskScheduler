# use like python3 main.py fifo src/inputs/test1.txt

from src.classes.scheduler import Scheduler
from src.file_reader import *

scheduler = Scheduler(parse_servers(), metrics['power_cap'], metrics['energy_cap'], metrics['repeat'])

# run the scheduler with the first argument given on the command line
# shall be fifo, edf etc.
scheduler.start(sys.argv[1], parse_jobs())
