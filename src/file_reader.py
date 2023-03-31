import sys
from src.classes.job import Job
from src.classes.server import Server

"""
Helper file to parse input of test files.
"""

split_path = sys.argv[2].split("/")
file_path = "/".join(split_path[:-1])

 # store values into a Map like {job_file': 'jobs.txt', 'server_file': 'servers.txt',...}
def parse_test_file():
    values = {}
    with open(sys.argv[2], 'r') as file:
        for line in file:
            if "=" in line:
                split_line = line.replace("\"", "").split(" ")
                values[split_line[0]] = split_line[-1].strip()
    return values

metrics = parse_test_file()

def parse_servers(server_file=None):
    servers = []
    if server_file is None:
        server_file = file_path + "/" + metrics["server_file"]
    with open(server_file, 'r') as file:
        for line in file:
            if line.startswith('#'):
                continue
            if "(" in line and ")" in line:
             frequencies = list(map(int, line[line.index("(")+1:line.index(")")].split(" "))) #-> [1,2,3] etc.
             elements = line.strip().split(" ")
            servers.append(Server(elements[0], frequencies))
    return servers

def parse_dependencies():
    dependencies = []
    dependencies_file = file_path + "/" + metrics["dependency_file"]
    with open(dependencies_file, 'r') as file:
        for line in file:
            if line.startswith('#') or ' - ' not in line:
                continue
            dependency = list(map(int, line.strip().split(' - ')))
            dependencies.append(dependency)
    return dependencies

def parse_jobs(job_file=None):
    jobs = []
    if job_file is None:
        job_file = file_path + "/" + metrics["job_file"]
    with open(job_file, 'r') as file:
        for line in file:
            if line.startswith("#"):
                continue
            attributes = [int(x) for x in line.strip().split(" ") if x != ""]
            jobs.append(Job(attributes[0], attributes[1], attributes[2], attributes[3],attributes[4]))
    return jobs   





