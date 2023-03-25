# use like python3 main.py fifo_basic src/inputs/test1.txt
# use like python3 main.py fifo_basic src/inputs/test1.txt for energy/power caps
# use like python3 main.py edf src/inputs/test1.txt
# use like python3 main.py edf_basic src/inputs/test1.txt
# use like python3 main.py round src/inputs/test1.txt
# use like python3 main.py rms src/inputs/test1.txt

from src.results import *
from src.classes.scheduler import Scheduler
from src.file_reader import *
import subprocess

scheduler = Scheduler(parse_servers(), metrics['power_cap'], metrics['energy_cap'], metrics['repeat'])

prepare_result_file()
# run the scheduler with the first argument given on the command line
# shall be fifo, edf etc.
scheduler.start(sys.argv[1], parse_jobs())

sort_result_file()
power_results(scheduler.total_power_used)
#tested on mac
subprocess.run(["python3", "src/plotter/plotter.py", "results.txt"])


