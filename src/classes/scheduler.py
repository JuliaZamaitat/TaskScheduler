class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat):
     self.servers = servers
     self.power_cap = power_cap
     self.energy_cap = energy_cap
     self.nbrepeat = nbrepeat
     self.job_queue = []
     self.current_time = 0
     self.missed_deadline_count = 0
     self.quantum = 4

  def printInfo(self, algorithm):
    if algorithm == 'fifo':
      print("Running FIFO")
    elif algorithm == 'edf':
      print("Running EDF")
    elif algorithm == 'rms':
      print("Running Rate Monotonic") 
    elif algorithm == 'round':
      print("Running Round Robin")      
    else:
      print("Not supported algorithm")   

#puts unscheduled jobs in queue and returns the ones that just have been scheduled
  def jobs_arrive(self, jobs):
    unscheduled = []
    for job in jobs:
      if job.arrival == self.current_time:
        job.deadline += self.current_time
        self.job_queue.append(job)
        unscheduled.append(job)
    return unscheduled

  #checks whether a job has finished and removes it from the server
  def finish_jobs(self):
    nb_running_jobs = 0
    for server in self.servers:
        if server.job:
            #server.job.relative_duration -= (server.performance * max(server.frequencies))
            server.job.relative_duration -= 1 * 1
            server.job.quantum += 1
            if server.job.relative_duration <= 0:
                server.shutdown(self.current_time)
            nb_running_jobs += 1 
    return nb_running_jobs

  def clock(self, jobs):
    unscheduled = self.jobs_arrive(jobs) 
    nb_running_jobs = self.finish_jobs()
   
    if len(unscheduled) != 0 or nb_running_jobs != 0:
      print("----Time " + str(self.current_time) + "-----")
      print("Waiting jobs: " + str(len(self.job_queue)))
      print("Running jobs: " + str(nb_running_jobs))
    if len(unscheduled) != 0:
      for job in unscheduled:
        print("Job with ID " + str(job.id) + ", duration: " + str(job.duration) + " arrives with deadline " + str(job.deadline))
    for job in self.job_queue:
      if job.deadline == (self.current_time):
        self.missed_deadline_count += 1
        print("Job with id: " + str(job.id) + " missed its deadline")


  def fifo(self):  #not preemptive
    job = self.job_queue[0]
    server = self.servers[0] #TODO: change later to select one avaiable server
    if not server.job:
      server.call(self.current_time, job)
      self.job_queue.remove(job)

  def round_robin(self): #not preemptive
    server =  self.servers[0] #TODO: change later to select one avaiable server
    if server.job is not None and server.job.quantum == self.quantum:
      old_job = server.job
      server.job.end = self.current_time
      duration_left = old_job.duration - (self.current_time - old_job.start) 
      dup_job = server.shutdown(self.current_time)
      dup_job.duration = duration_left
      print("Duration of job " + str(dup_job.id) + " left: " + str(dup_job.duration))
      self.job_queue.append(dup_job)
    if not server.job:
      server.call(self.current_time, self.job_queue[0])
      self.job_queue.remove(self.job_queue[0])

  def edf(self): #preemptive
    deadlines = []
    #sort deadlines of jobs in queue
    job = sorted(self.job_queue, key=lambda j: j.deadline)[0]
    if self.servers[0].job: #TODO: change to iterate through all servers
      deadlines.append(self.servers[0].job.deadline)
    
    #TODO later: get server with highest deadline
    #if all servers are full TODO: change later to  if len(deadlines) == len(servers)
    if self.servers[0].job:
      #if the new job candidate has earlier deadline than the current job
      if job.relative_deadline < self.servers[0].job.relative_deadline:
        #interrupt executed job
        self.servers[0].job.end = self.current_time
        self.job_queue.append(self.servers[0].job)
        self.servers[0].shutdown(self.current_time)
        self.servers[0].call(self.current_time, job)
        self.job_queue.remove(job)
    else:
      self.servers[0].call(self.current_time, job)
      self.job_queue.remove(job)

  def rms(self): #preemptive
    print("edf")


  def schedule_tasks(self, algorithm):
    if algorithm == 'fifo':
      self.fifo()
    elif algorithm == 'edf':
      self.edf()
    elif algorithm == 'round':
      self.round_robin()    
    elif algorithm == 'rms':
      self.rms()  


  def jobs_left(self, jobs):
    server_empty = True
    server = self.servers[0]
    if server.job:
        server_empty = False
    if not server_empty or any(job.end < 0 for job in jobs):
        return True
    else:
      return False    

  def start(self, algorithm, jobs):
    self.printInfo(algorithm)
    while(self.jobs_left(jobs)):
      #create a queue of jobs
      #set deadlines and times for jobs
      self.clock(jobs)
      #start scheduling the tasks for the amount of jobs in queue
      for _ in range (len(self.job_queue)):
        self.schedule_tasks(algorithm)
      self.current_time += 1  
    
    print("\n--------------STATS----------")
    print("Deadlines fulfilled in total " + str(len(jobs) - self.missed_deadline_count)) 
    print("Deadlines missed in total: " + str(self.missed_deadline_count)) 
      