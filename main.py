import sys
from classes.job import Job


split_path = sys.argv[1].split("/")
input_path = "/".join(split_path[:-1])

# use as python main.py inputs/test1.txt
 # store values into a Map like {job_file': 'jobs.txt', 'server_file': 'servers.txt',...}
def parse_test_file():
    values = {}
    with open(sys.argv[1], 'r') as file:
        for line in file:
            if "=" in line:
                split_line = line.replace("\"", "").split(" ")
                values[split_line[0]] = split_line[-1].strip()
    return values


data = parse_test_file()

#reads the given job_file and returns a list of jobs 
def parse_jobs(job_file=None):
    jobs = []
    if job_file is None:
        print(input_path)
        job_file = input_path + "/" + data["job_file"]
    with open(job_file, 'r') as file:
        for line in file:
            if line.startswith("#"):
                continue
            attributes = [int(x) for x in line.strip().split(" ") if x != ""]
            jobs.append(Job(attributes[0], attributes[1], attributes[2], attributes[3],attributes[4]))
    return jobs


print(parse_jobs())





