from src.classes.job import Job

class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat):
     self.servers = servers
     self.power_cap = power_cap
     self.energy_cap = energy_cap
     self.nbrepeat = int(nbrepeat)
     self.job_queue = []
     self.current_time = 0
     self.missed_deadline_count = 0
     self.quantum = 4
     self.number_of_total_jobs = 0

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
        job.nbPeriod += 1
        self.job_queue.append(job)
        self.number_of_total_jobs += 1
        unscheduled.append(job)
      elif job.period > 0 and job.nbPeriod < self.nbrepeat:
        if ((self.current_time - job.arrival) % job.period) == 0:
          job.nbPeriod += 1
          dup_job = Job(job.id, job.arrival + self.current_time, job.duration, job.deadline, job.period)#TODO: duration hier plus self.currentTime and arrivelDate oder nicht?
          self.job_queue.append(dup_job)
          self.number_of_total_jobs += 1
          unscheduled.append(dup_job)
    return unscheduled

  def choose_server(self, job):
    # for server in self.servers:
    #     if not server.job:
    #         server.call(self.current_time, job)
    #         self.job_queue.remove(job)
    #         break
    available_servers = [server for server in self.servers if not server.job]
    if not available_servers:
        print("No available servers to choose from.")
        return
    server = min(available_servers, key=lambda s: s.frequencies[0])
    for s in available_servers:
        if s.frequencies[0] < server.frequencies[0]:
            server = s
    server.call(self.current_time, job)
    self.job_queue.remove(job)


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
            else: 
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
        print("Job with ID " + str(job.id) + ", duration: " + str(job.duration) + ", period: " + str(job.period) + " arrives with deadline " + str(job.deadline))
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
    for server in self.servers:
        if server.job:
          deadlines.append(server.job.deadline)
    #no empty server available
    if len(deadlines) == len(self.servers):
      #choose which server has the deadline that is the longest away
      server_with_longest_deadline = self.servers[max(range(len(deadlines)), key=deadlines.__getitem__)] if deadlines else None
      #if the new job candidate has earlier deadline than the current job
      if job.relative_deadline < server_with_longest_deadline.job.relative_deadline:
        #interrupt executed job
        server_with_longest_deadline.job.end = self.current_time
        self.job_queue.append(server_with_longest_deadline.job)
        server_with_longest_deadline.shutdown(self.current_time)
        server_with_longest_deadline.call(self.current_time, job)
        self.job_queue.remove(job)
    else:
      self.choose_server(job)

  def rms(self): #preemptive, fixex-priority based on periods
    job = sorted(self.job_queue, key=lambda j: 1/j.period if j.period > 0 else 0, reverse=True)[0]
    busy_servers = [server for server in self.servers if server.job]
    if len(busy_servers) == len(self.servers):
      # print("All servers busy")  
      server_with_lowest_priority = min(self.servers, key=lambda s: s.job.period)
      if (1/job.period  if job.period > 0 else 0) > (1/server_with_lowest_priority.job.period if server_with_lowest_priority.job.period > 0 else 0):
        server_with_lowest_priority.job.end = self.current_time
        self.job_queue.append(server_with_lowest_priority.job)
        server_with_lowest_priority.shutdown(self.current_time)
        server_with_lowest_priority.call(self.current_time, job)
        self.job_queue.remove(job)
    else:
        self.choose_server(job)

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
    if not server_empty or any(job.end < 0 for job in jobs) or any((job.nbPeriod < self.nbrepeat) and (job.period > 0) for job in jobs):
        return True
    else:
      return False    

  def start(self, algorithm, jobs):
    self.printInfo(algorithm)
    while(self.jobs_left(jobs)) :
      #create a queue of jobs
      #set deadlines and times for jobs
      self.clock(jobs)
      #start scheduling the tasks for the amount of jobs in queue
      for _ in range (len(self.job_queue)):
        self.schedule_tasks(algorithm)
      self.current_time += 1  
    
    print("\n--------------STATS----------")
    print("Deadlines fulfilled in total " + str(self.number_of_total_jobs - self.missed_deadline_count)) 
    print("Deadlines missed in total: " + str(self.missed_deadline_count)) 
      