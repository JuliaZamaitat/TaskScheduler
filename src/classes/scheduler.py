from src.classes.job import Job

class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat):
     self.servers = servers
     self.power_cap = int(power_cap)
     self.energy_cap = int(energy_cap)
     self.nbrepeat = int(nbrepeat)
     self.job_queue = []
     self.current_time = 0
     self.missed_deadline_count = 0
     self.quantum = 4
     self.number_of_total_jobs = 0
     self.energy_consumption = 0
     self.energy_cap_exceeded = False
     self.power_cap_exceeded = False
     self.total_power_used = []


  def printInfo(self, algorithm):
    if algorithm == 'fifo_basic':
      print("Running FIFO without energy/power caps, on one server")
    if algorithm == 'fifo':
      print("Running FIFO with energy/power caps")
    elif algorithm == 'edf':
      print("Running EDF")
    elif algorithm == 'edf_basic':
      print("Running EDF without energy/power caps")  
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

    #calculate power consumption
    consumption = 0
    for server in self.servers:
      consumption += server.power
    self.total_power_used.append(consumption)   
    
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


  def fifo_basic(self):  #not preemptive
    job = self.job_queue[0]
    server = self.servers[0] 
    if not server.job:
      server.call(self.current_time, job)
      self.job_queue.remove(job)

  def is_energy_cap_exceeded(self, job, server):
    # Energy cap gives the amount of total energy all servers together can consume over the WHOLE time
    job_duration = server.calculate_power_consumption(job)
    # total_power = sum([s.power for s in self.servers if s.job]) + server.power
    job_energy = server.power * job_duration
    if self.energy_cap > 0 and self.energy_consumption + job_energy > self.energy_cap:
      print(f"Job with ID {job.id} cannot be scheduled due to energy constraint")
      return True
    return False
  
  def is_power_cap_exceeded(self, job, server):
    # Power cap gives the amount of total power all servers together can consume at the SAME time
    server.calculate_power_consumption(job)
    total_power = sum([s.power for s in self.servers if s.job]) + server.max_power
    #if self.total_power_used[-1]+ server.power >= self.power_cap:
    if total_power  > self.power_cap:
      print(f"Job with ID {job.id} cannot be scheduled to server {server.id} due to power constraint")
      return True
    return False

  def fifo(self):  #not preemptive, several servers, energy/power caps
    job = self.job_queue[0]
    for server in self.servers:
      if not server.job:
        if self.is_energy_cap_exceeded(job, server):
          self.energy_cap_exceeded = True
          break
        if self.is_power_cap_exceeded(job, server):
          continue  # Gehe zum nächsten Server
        server.call_energy_aware(self.current_time, job)
        self.job_queue.remove(self.job_queue[0])
        self.energy_consumption += server.power
        break

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

  def edf_basic(self): #preemptive
    #sort deadlines of jobs in queue
    job = sorted(self.job_queue, key=lambda j: j.deadline)[0]
    deadlines = [server.job.deadline for server in self.servers if server.job]

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

  def choose_server_with_most_power(self, job):
    # Calculate the speeds of the servers using Constant Speed Scaling
    available_servers = [server for server in self.servers if not server.job]
    speeds = [1 * (server.max_power / 2500) ** (1 / 3) for server in available_servers] #1 = server.performance, 2.500 =max_energy over time
    # Getting the server with the highest speed
    for server, speed in zip(available_servers, speeds):
        if self.is_energy_cap_exceeded(job, server):
            self.energy_cap_exceeded = True
            break
        if self.is_power_cap_exceeded(job, server):
            continue
        # Find the server with the highest speed
        max_speed_server = available_servers[speeds.index(max(speeds))]
        max_speed_server.call_energy_aware(self.current_time, job)
        self.job_queue.remove(job)
        break
    
  def edf(self):
    job = sorted(self.job_queue, key=lambda j: j.deadline)[0]
    deadlines = [server.job.deadline for server in self.servers if server.job]
    #no empty server available
    if len(deadlines) == len(self.servers):
    # Get the server with the highest deadline
      server_with_longest_deadline = self.servers[max(range(len(deadlines)), key=deadlines.__getitem__)] if deadlines else None
      if job.relative_deadline < server_with_longest_deadline.job.relative_deadline:
          # End the currently executing job
          server_with_longest_deadline.job.end = self.current_time
          self.job_queue.append(server_with_longest_deadline.job)
          server_with_longest_deadline.shutdown(self.current_time)
          server_with_longest_deadline.call_energy_aware(self.current_time, job) #normally check here as well if energy caps are kept, but as speeds are the same here, we simply replace the one by the other and assume it's the same
          self.job_queue.remove(job)
    else:
      self.choose_server_with_most_power(job)

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
    if algorithm == 'fifo_basic':
      self.fifo_basic() 
    elif algorithm == 'edf':
      self.edf()
    elif algorithm == 'edf_basic':
      self.edf_basic()  
    elif algorithm == 'round':
      self.round_robin()    
    elif algorithm == 'rms':
      self.rms()  


  # def jobs_left(self, jobs):
  #   server_empty = True
  #   server = self.servers[0]
  #   if server.job:
  #       server_empty = False
  #   if not server_empty or any(job.end < 0 for job in jobs) or any((job.nbPeriod < self.nbrepeat) and (job.period > 0) for job in jobs):
  #       return True
  #   else:
  #     return False    
  def jobs_left(self, jobs):
    server_empty = True
    for server in self.servers:
      if server.job:
        server_empty = False
        break
    if not server_empty or any(job.end < 0 for job in jobs) or any((job.nbPeriod < self.nbrepeat) and (job.period > 0) for job in jobs):
        return True
    else:
        return False

  def start(self, algorithm, jobs):
    self.printInfo(algorithm)
    while(self.jobs_left(jobs) and not self.energy_cap_exceeded) :
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
      