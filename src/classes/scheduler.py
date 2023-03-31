from src.classes.job import Job
import networkx as nx

class Scheduler:
  def __init__(self, servers, power_cap, energy_cap, nbrepeat, dependencies):
     """
        Constructor for Scheduler
        
        Args:
            servers (List[Server]): A list of Server objects
            power_cap (str): A String giving the maximal power consumption at a given moment.
            energy_cap (str): A String giving the maximal energy consumption over time.
            energy_cap (str): A String giving the maximal energy consumption over time.
            nbrepeat (str): A String giving the maximal iteration count for periodic tasks.
            dependencies (List[List[number]]): A list consiting of lists with pairs of dependent tasks.
        """
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

  """
  Prints information about the currently used algorithm.
 
  Args:
      algorithm (String): The name of the algorithm.
  """
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
  
  """
  Puts unscheduled jobs in queue and calculates their stats.
 
  Args:
      jobs (List[Job]): A list of jobs that arrive at that time tick.
 
  Returns:
      List[Job]: A list of jobs that just arrived and are not scheduled yet.
  """
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
          dup_job = Job(job.id, job.arrival + self.current_time, job.duration, job.deadline, job.period)
          self.job_queue.append(dup_job)
          self.number_of_total_jobs += 1
          unscheduled.append(dup_job)
    return unscheduled

  """
  Simulates one clock tick.
  On each tick, it:
    - checks for arriving jobs
    - finishes jobs
    - calculates the current power consumption
    - checks for missed deadlines
 
  Args:
      jobs (List[Job]): A list of jobs that were parsed by the input parser.
  """
  def time_tick(self, jobs):
    unscheduled = self.jobs_arrive(jobs) 
    nb_running_jobs = self.finish_jobs()
    #calculate power consumption for each time tick
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

  """
  Checks whether there are still jobs that are not finished yet.
 
  Args:
      Jobs (List[Job]): List of jobs that were parsed by the Scheduler. 
  
  Returns:
      Boolean: A boolean value indicating whether there are unfinished jobs or not.
  """
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
  
  """
  Checks if the accumulated energy consumption over the duration of the program exceeds the 
  energy cap specified if the given job would be scheduled.
 
  Args:
      job (Job): The new Job that is about to be scheduled.
      server (Server): The chosen Server for that Job.
 
  Returns:
      Boolean: A boolean value indicating whether the energy cap would be exceeded or not.
  """
  def is_energy_cap_exceeded(self, job, server):
    job_duration = server.calculate_power_consumption(job)
    job_energy = server.power * job_duration
    if self.energy_cap > 0 and self.energy_consumption + job_energy > self.energy_cap:
      print(f"Job with ID {job.id} cannot be scheduled due to energy constraint")
      return True
    return False

  """
  Checks if the accumulated power consumption at a given moment exceeds the
  power cap specified if the given job would be scheduled.
 
  Args:
      job (Job): The new Job that is about to be scheduled.
      server (Server): The chosen Server for that Job.
 
  Returns:
      Boolean: A boolean value indicating whether the power cap would be exceeded or not.
  """
  def is_power_cap_exceeded(self, job, server):
    server.calculate_power_consumption(job)
    total_power = sum([s.power for s in self.servers if s.job]) + server.max_power
    if total_power  > self.power_cap:
      print(f"Job with ID {job.id} cannot be scheduled to server {server.id} due to power constraint")
      return True
    return False
  
  """
  Checks if there are available servers. 
  If so, schedules the Job on this server and removes job from queue.
 
  Args:
      job (Job): The new Job for which a server must be chosen.
  """
  def choose_server_and_schedule(self, job):
    available_servers = [server for server in self.servers if not server.job]
    if not available_servers:
        print("No available servers to choose from.")
        return
    server = min(available_servers, key=lambda s: s.frequencies[0])
    for s in available_servers:
        if s.frequencies[0] < server.frequencies[0]:
            server = s
    server.schedule(self.current_time, job)
    self.job_queue.remove(job)

  """
  Checks for all servers whether the currently running job on them is finished.
 
  Returns:
      Number: The number of currently running jobs on every server.
  """
  def finish_jobs(self):
    nb_running_jobs = 0
    for server in self.servers:
        if server.job:
            server.job.relative_duration -= 1 * 1
            server.job.quantum += 1
            if server.job.relative_duration <= 0:
                server.shutdown(self.current_time)
            else: 
              nb_running_jobs += 1 
    return nb_running_jobs
  
  """
  Basic implementation of the FIFO algorithm, on one server with one frequency, without energy or power caps.
  """
  def fifo_basic(self):  #not preemptive
    job = self.job_queue[0]
    server = self.servers[0] 
    if not server.job:
      server.schedule(self.current_time, job)
      self.job_queue.remove(job)

  """
  Enhanced implementation of the FIFO algorithm, on multiple server, choosing the biggest frequency,
  with energy and power caps considered.
  """
  def fifo(self):  #not preemptive
    job = self.job_queue[0]
    for server in self.servers:
      if not server.job:
        if self.is_energy_cap_exceeded(job, server):
          self.energy_cap_exceeded = True
          break
        if self.is_power_cap_exceeded(job, server):
          continue  # choose the next server
        server.schedule_energy_aware(self.current_time, job)
        self.job_queue.remove(self.job_queue[0])
        self.energy_consumption += server.power
        break

  """
  Basic implementation of the Round Robin algorithm, on one server with one frequency, without energy or power caps.
  """
  def round_robin(self): #not preemptive
    server =  self.servers[0]
    if server.job is not None and server.job.quantum == self.quantum:
      old_job = server.job
      server.job.end = self.current_time
      duration_left = old_job.duration - (self.current_time - old_job.start) 
      dup_job = server.shutdown(self.current_time)
      dup_job.duration = duration_left
      print("Duration of job " + str(dup_job.id) + " left: " + str(dup_job.duration))
      self.job_queue.append(dup_job)
    if not server.job:
      server.schedule(self.current_time, self.job_queue[0])
      self.job_queue.remove(self.job_queue[0])

  """
  Basic implementation of the EDF algorithm, on one server with one frequency, without energy or power caps.
  """
  def edf_basic(self): #preemptive
    #sort deadlines of jobs in queue
    job = sorted(self.job_queue, key=lambda j: j.deadline)[0]
    deadlines = [server.job.deadline for server in self.servers if server.job]
    #no empty server available
    if len(deadlines) == len(self.servers):
      server_with_longest_deadline = self.servers[max(range(len(deadlines)), key=deadlines.__getitem__)] if deadlines else None
      #if the new job candidate has earlier deadline than the current job
      if job.relative_deadline < server_with_longest_deadline.job.relative_deadline:
        #interrupt executed job
        server_with_longest_deadline.job.end = self.current_time
        self.job_queue.append(server_with_longest_deadline.job)
        server_with_longest_deadline.shutdown(self.current_time)
        server_with_longest_deadline.schedule(self.current_time, job)
        self.job_queue.remove(job)
    else:
      self.choose_server_and_schedule(job)
  
  """
  Chooses an available server with the highest power based on the Constant Speed Scaling Formula.
 
  Args:
      job (Job): The new Job for which a server must be chosen.
  """
  def choose_server_with_most_power(self, job):
    available_servers = [server for server in self.servers if not server.job]
    speeds = [1 * (server.max_power / self.power_cap) ** (1 / 3) for server in available_servers] 
    # Getting the server with the highest speed
    for server in zip(available_servers, speeds): #producec (server_id, speed)
        # Check if selecting this server would exceed the energy cap
        if self.is_energy_cap_exceeded(job, server):
            self.energy_cap_exceeded = True
            break
        # Check if selecting this server would exceed the power cap    
        if self.is_power_cap_exceeded(job, server):
            continue
        # Find the server with the highest speed
        max_speed_server = available_servers[speeds.index(max(speeds))]
        max_speed_server.schedule_energy_aware(self.current_time, job)
        self.job_queue.remove(job)
        self.energy_consumption += max_speed_server.power
        break
    
  """
  Enhanced implementation of the EDF algorithm, on multiple server, choosing the biggest frequency,
  with energy and power caps considered.
  """
  def edf(self):
    job = sorted(self.job_queue, key=lambda j: j.deadline)[0]
    deadlines = [server.job.deadline for server in self.servers if server.job]
    #no empty server available
    if len(deadlines) == len(self.servers):
      server_with_longest_deadline = self.servers[max(range(len(deadlines)), key=deadlines.__getitem__)] if deadlines else None
      if job.relative_deadline < server_with_longest_deadline.job.relative_deadline:
          # End the currently executing job
          server_with_longest_deadline.job.end = self.current_time
          self.job_queue.append(server_with_longest_deadline.job)
          server_with_longest_deadline.shutdown(self.current_time)
          server_with_longest_deadline.schedule_energy_aware(self.current_time, job) #normally check here as well if energy caps are kept, but as speeds are the same here, we simply replace the one by the other and assume it's the same
          self.job_queue.remove(job)
          self.energy_consumption += server_with_longest_deadline.power
    else:
      self.choose_server_with_most_power(job)

  """
  Implementation of the RMS algorithm, on multiple server, with one frequency, without energy or power caps considered.
  """
  def rms(self): #preemptive, fixex-priority based on periods
    #job with shorter period has higher priority
    job = sorted(self.job_queue, key=lambda j: 1/j.period if j.period > 0 else 0, reverse=True)[0]
    busy_servers = [server for server in self.servers if server.job]
    #no server available
    if len(busy_servers) == len(self.servers):
      # print("All servers busy")  
      server_with_lowest_priority = min(self.servers, key=lambda s: s.job.period)
      #compare priorities of current job on server and job to be scheduled
      if (1/job.period if job.period > 0 else 0) > (1/server_with_lowest_priority.job.period if server_with_lowest_priority.job.period > 0 else 0):
        server_with_lowest_priority.job.end = self.current_time
        self.job_queue.append(server_with_lowest_priority.job)
        server_with_lowest_priority.shutdown(self.current_time)
        server_with_lowest_priority.schedule(self.current_time, job)
        self.job_queue.remove(job)
    else:
        self.choose_server_and_schedule(job)

  """
  Computes waves based on the dependencies between tasks parsed by the scheduler. 
  Adds independent tasks to the first wave.
  """
  def calculate_waves(self):
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

  """
  Checks whether a job has dependencies and returns them.
 
  Args:
      job_id (number): The ID of the job to be scheduled.
 
  Returns:
      List[Number]: The IDs of all jobs that have a dependency with the current job.
  """
  def get_dependencies(self, job_id):
    dependent_tasks = []
    for dep in self.dependencies:
      if dep[1] == job_id and dep[0] not in dependent_tasks:
        dependent_tasks.append(dep[0])
    return dependent_tasks
  
  """
  Chooses the first available server that exists.

  Returns:
    Server | None: An available server or None.
  """
  def get_available_server(self):
    for server in self.servers:
        if server.job is None:
            return server
    return None
  
  """
  Implementation fo the Wavefront algorithm, without power and energy caps, with one frequency, 
  all tasks arriving at time 0.
  """
  def wavefront(self):
    if self.waves is None:
        self.calculate_waves()
    if len(self.waves) != 0:  
        wave = self.waves[0]
        for job in self.job_queue:
          if job.id in wave:
            #if job has dependencies
            if len(self.get_dependencies(job.id)) > 0:
              dependent_job_ids = self.get_dependencies(job.id)
              dependent_jobs = []
              for j in dependent_job_ids:
                for sj in self.scheduled_jobs:
                  if j == sj.id:
                    dependent_jobs.append(sj)
              #check whether the preceding depending jobs have finished yet
              if all(self.current_time >= j.start + j.duration for j in dependent_jobs): 
                #if so, choose a server
                server = self.get_available_server()
                if server is not None:
                  server.schedule(self.current_time, job)
                  self.scheduled_jobs.append(job)
                  self.job_queue.remove(job)
                  #remove job from wave to not iterate over it again
                  wave.remove(job.id)
                  #remove wave if we completed all jobs
                  if len(wave) == 0:
                    self.waves.pop(0)
                else:
                  print("No available server.")
                  break
            #if job has no dependencies
            else:
              server = self.get_available_server()
              if server is not None:
                server.schedule(self.current_time, job)
                self.scheduled_jobs.append(job)
                self.job_queue.remove(job)
                wave.remove(job.id)
                if len(wave) == 0:
                  self.waves.pop(0)
              else:
                print("No available server.")
                break        

  """
  Adds independent tasks to the list of ordered graphs.

  Returns:
    Server | None: An available server or None.
  """
  def get_sorted_graphs(self):
    sub_paths = self.create_longest_subpaths()
    independent_tasks = [task.id for task in self.job_queue if not any(task.id == dep[1] for dep in self.dependencies)]
    for t in independent_tasks:
       sub_paths.append([t])
    return sub_paths   

  """
  Creates a list of subpaths from dependencies.
  When an job id appears multiple times in a subpath, keeps only in the longest path.
  Orders the list from longest to shortest paths.

  Returns:
    List[[Number]]: A lis containing lists of job ids that form a graph.
  """
  def create_longest_subpaths(self):
    # Creating a directed graph
    G = nx.DiGraph()
    #"dding edges based on the given dependencies
    for edge in self.dependencies:
      G.add_edge(edge[0], edge[1])
    # Collecting all longest sub-paths from each starting node
    longest_subpaths = []
    for source in G.nodes(): #for each node in graph
      if G.in_degree(source) == 0: # Check if it is a starting node (in-degree of 0)
        for target in G.nodes(): #for each node in graph
          if G.out_degree(target) == 0: #Check if it is an end node (out-degree of 0)
            if nx.has_path(G, source, target): #Check if a path exists between the starting and end nodes
              path = nx.shortest_path(G, source, target) # If a path exists, get the shortest path between the starting and end nodes
              longest_subpaths.append(path)

    #remove nodes that already appear in the longer graph
    for i, l in enumerate(longest_subpaths):
      if(i + 1 != len(longest_subpaths)):
        for e in list(l):  # Create a copy of the list 'l' to iterate over
          if e in longest_subpaths[i + 1]:
            l.remove(e)            
    
    longest_subpaths.sort(key=len, reverse=True)
    return longest_subpaths

  """
  Implementation fo the CPM algorithm, without power and energy caps, with one frequency, 
  all tasks arriving at time 0.
  """
  def cpm(self):
    if self.sorted_graphs is None: 
      self.sorted_graphs = self.get_sorted_graphs()
    if len(self.sorted_graphs) != 0:
      for graph in self.sorted_graphs:
        for job in self.job_queue:
          if job.id in graph:
            #if job has dependencies
            if len(self.get_dependencies(job.id)) > 0:
              dependent_job_ids = self.get_dependencies(job.id)
              dependent_jobs = []
              for j in dependent_job_ids:
                #check if dependent job was already scheduled
                if any(obj.id == j for obj in self.scheduled_jobs):
                  for sj in self.scheduled_jobs:
                    if j == sj.id:
                      dependent_jobs.append(sj)
                else: #check if dependent job is still in queue
                  for qj in self.job_queue:
                    if j == qj.id:
                      dependent_jobs.append(qj)
              #all dependent jobs are finished
              if all(self.current_time >= j.start + j.duration for j in dependent_jobs): 
                server_index = self.sorted_graphs.index(graph) % len(self.servers)
                server = self.servers[server_index]
                if server is not None and server.job is None:
                  server.schedule(self.current_time, job)
                  self.scheduled_jobs.append(job)
                  self.job_queue.remove(job) 
            #job has no dependencies
            else:
              server_index = self.sorted_graphs.index(graph) % len(self.servers)
              server = self.servers[server_index]
              if server is not None and server.job is None:
                server.schedule(self.current_time, job)
                self.scheduled_jobs.append(job)
                self.job_queue.remove(job)         
          
  """
  Chooses the algorithm to run based on the string input from the user.
 
  Args:
      algorithm (String): The algorithm to perform.
  """
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
  
  """
  Starting point of the scheduling. Performs the following tasks:
    - Prints information of the chosen algorithm.
    - Checks whether there are jobs left to schedule.
    - Checks whether the energy cap is exceeded.
    - Runs the internal clock
    - Schedules the tasks.
    - Increases the current time.
    - Prints stats after all tasks are finished.
 
  Args:
      algorithm (String): The algorithm to perform.
      jobs (List[Job]): List of jobs parsed by the Scheduler.
  """
  def start(self, algorithm, jobs):
    self.printInfo(algorithm)
    while(self.jobs_left(jobs) and not self.energy_cap_exceeded) :
      self.time_tick(jobs)
      for _ in range (len(self.job_queue)):
        self.schedule_tasks(algorithm)
      self.current_time += 1  
    
    print("\n--------------STATS----------")
    print("Deadlines fulfilled in total " + str(self.number_of_total_jobs - self.missed_deadline_count)) 
    print("Deadlines missed in total: " + str(self.missed_deadline_count)) 
    print("Energy used in total: " + str(self.energy_consumption)) 
    print("Makespan: " + str(self.current_time -1 )) 


      