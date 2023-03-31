from src.classes.job import Job
import networkx as nx

class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat, dependencies):
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
     self.dependencies = dependencies
     self.waves = None
     self.scheduled_jobs = []
     self.sorted_graphs = None


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
    elif algorithm == 'wavefront':
      print("Running Wavefront (as early as possible)")
    elif algorithm == 'cpm':
      print("Running Cluster Path Merge")                
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
        self.energy_consumption += max_speed_server.power
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
          self.energy_consumption += server_with_longest_deadline.power

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

  def calculate_waves(self):
    print("len", len(self.job_queue))
    independent_tasks = [task.id for task in self.job_queue if not any(task.id == dep[1] for dep in self.dependencies)]
    waves = [[self.dependencies[0][0]]] if self.dependencies else []
    while True:
        current_wave = []
        for wave_item in waves[-1]:
            for dependency in self.dependencies:
                if dependency[0] == wave_item:
                    current_wave.append(dependency[1])
        if not current_wave:
            break
        waves.append(current_wave)
    waves[0].pop(0)
    for t in independent_tasks:
      waves[0].insert(len(waves)-1,t)
    self.waves = waves


  def wavefront(self):
    if self.waves is None:
        self.calculate_waves()
    if len(self.waves) != 0:  
        wave = self.waves[0]
        for job in self.job_queue:
          if job.id in wave:
            if len(self.get_dependencies(job.id)) > 0:
              dependent_job_ids = self.get_dependencies(job.id)
              dependent_jobs = []
              for j in dependent_job_ids:
                for sj in self.scheduled_jobs:
                  if j == sj.id:
                    dependent_jobs.append(sj)
              if all(self.current_time >= j.start + j.duration for j in dependent_jobs): 
                server = self.get_available_server()
                if server is not None:
                  server.call(self.current_time, job)
                  self.scheduled_jobs.append(job)
                  self.job_queue.remove(job)
                  wave.remove(job.id)
                  if len(wave) == 0:
                    self.waves.pop(0)
                else:
                  print("No available server.")
                  break
            else:
              server = self.get_available_server()
              if server is not None:
                server.call(self.current_time, job)
                self.scheduled_jobs.append(job)
                self.job_queue.remove(job)
                wave.remove(job.id)
                if len(wave) == 0:
                  self.waves.pop(0)
              else:
                print("No available server.")
                break        


  def get_dependencies(self, job_id):
    dependent_tasks = []
    for dep in self.dependencies:
        if dep[1] == job_id and dep[0] not in dependent_tasks:
            dependent_tasks.append(dep[0])
    return dependent_tasks


  def get_available_server(self):
    # Sort servers by ascending order of the number of running jobs
    available_servers = self.servers
    # Iterate through the sorted list of servers
    for server in available_servers:
        # Check if the server has any available resources
        if server.job is None:
            return server
    # If no available server is found, return None
    return None


  def get_sorted_graphs(self):
    sub_paths = self.create_longest_subpaths()
    independent_tasks = [task.id for task in self.job_queue if not any(task.id == dep[1] for dep in self.dependencies)]
    for t in independent_tasks:
       sub_paths.append([t])
    return sub_paths   

  def create_longest_subpaths(self):
    # Erstellen eines gerichteten Graphen
    G = nx.DiGraph()
    # Füge Kanten basierend auf den gegebenen Abhängigkeiten hinzu
    for edge in self.dependencies:
        G.add_edge(edge[0], edge[1])
    # Sammle alle längsten Sub-Paths von jedem Startknoten aus
    longest_subpaths = []
    for source in G.nodes():
        if G.in_degree(source) == 0:  # Nur für Startknoten
            for target in G.nodes():
                if G.out_degree(target) == 0:  # Nur für Endknoten
                    if nx.has_path(G, source, target):
                        path = nx.shortest_path(G, source, target)
                        longest_subpaths.append(path)

    #remove nodes that already appear in the longer graph
    for i, l in enumerate(longest_subpaths):
      if(i + 1 != len(longest_subpaths)):
        for e in list(l):  # Create a copy of the list 'l' to iterate over
          if e in longest_subpaths[i + 1]:
            l.remove(e)            
    longest_subpaths.sort(key=len, reverse=True)
    return longest_subpaths

  
  def cpm(self):
    if self.sorted_graphs is None: 
      self.sorted_graphs = self.get_sorted_graphs()
    if len(self.sorted_graphs) != 0:
      for graph in self.sorted_graphs:
        for job in self.job_queue:
          if job.id in graph:
            if len(self.get_dependencies(job.id)) > 0:
              dependent_job_ids = self.get_dependencies(job.id)
              dependent_jobs = []
              for j in dependent_job_ids:
                if any(obj.id == j for obj in self.scheduled_jobs): #check if dependent job was already scheduled
                  for sj in self.scheduled_jobs:
                    if j == sj.id:
                      dependent_jobs.append(sj)
                else: #check if dependent job is still in queue
                  for qj in self.job_queue:
                    if j == qj.id:
                      dependent_jobs.append(qj)
              if all(self.current_time >= j.start + j.duration for j in dependent_jobs): #all dependent jobs are finished
                server_index = self.sorted_graphs.index(graph) % len(self.servers)
                server = self.servers[server_index]
                if server is not None and server.job is None:
                  server.call(self.current_time, job)
                  self.scheduled_jobs.append(job)
                  self.job_queue.remove(job) 
            else:
              server_index = self.sorted_graphs.index(graph) % len(self.servers)
              server = self.servers[server_index]
              if server is not None and server.job is None:
                server.call(self.current_time, job)
                self.scheduled_jobs.append(job)
                self.job_queue.remove(job)         

          
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
    elif algorithm == 'wavefront':
      self.wavefront()
    elif algorithm == 'cpm':
      self.cpm()         
 
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
    print("Energy used in total: " + str(self.energy_consumption)) 
    print("Makespan: " + str(self.current_time -1 )) 


      