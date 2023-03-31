from src.results import *
from src.classes.scheduler import Scheduler
from src.file_reader import *
import subprocess

scheduler = Scheduler(parse_servers(), metrics['power_cap'], metrics['energy_cap'], metrics['repeat'], parse_dependencies())

prepare_result_file()

scheduler.start(sys.argv[1], parse_jobs())

sort_result_file()
power_results(scheduler.total_power_used)

#tested on mac
subprocess.run(["python3", "src/plotter/plotter.py", "results.txt"])


