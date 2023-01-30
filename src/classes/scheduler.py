class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat):
     self.servers = servers
     self.power_cap = power_cap
     self.energy_cap = energy_cap
     self.nbrepeat = nbrepeat
     self.job_queue = []
     self.current_time = 0
     self.missed_deadline_count = 0

def printInfo(algorithm):
  if algorithm == 'fifo':
    print("Running FIFO")
  elif algorithm == 'edf':
    print("Running EDF")   
  else:
    print("Not supported algorithm")   


def jobs_arrive(self, jobs):
  unscheduled = [],
  for job in jobs:
    if job.arrival == self.current_time:
      job.deadline += self.current_time
      self.job_queue.append(job)
      unscheduled.append(job)
  return unscheduled

def clock(self, jobs):
  unscheduled = jobs_arrive(jobs)
  if len(unscheduled) is not 0:
    print("Time" + self.current_time)
    for job in unscheduled:
      print(job)
    print("Waiting jobs: " + len(self.job_queue))
    for job in self.job_queue:
      if job.deadline > self.current_time:
        self.missed_deadline_count += 1
        print("Job with id: " + job.id + " missed its deadline")


def fifo(self):
  job = self.job_queue[0]
  server = self.servers[0] #TODO: change later to select one avaiable server
  if not server.job:
    server.call(self.current_time, job)
    self.job_queue.remove(job)


def schedule_tasks(self, algorithm):
  if algorithm == 'fifo':
    fifo()

def init(self, algorithm, jobs):
 printInfo(algorithm)

 server_empty = self.servers[0].job == None
 job_left = any(job.end < 0 for job in jobs)
 if server_empty and job_left:
   #create a queue of jobs
   #set deadlines and times for jobs
   clock(jobs)
   #start scheduling the tasks for the amount of jobs in queue
   for _ in range(self.job_queue):
     schedule_tasks(algorithm)
     self.current_time += 1
   