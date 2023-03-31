class Server:
  def __init__(self, id, frequencies):
    """
        Constructor for Server
        
        Args:
            id (int): An integer giving the id of the server
            frequencies (List[int]): A List with integers indicating the available frequencies of the Server.
      """
    self.id = id
    self.frequencies = frequencies
    self.job = None
    self.queue = []
    self.power = 0
    self.speed = 1
    self.max_power = 200
    
  """
  Calculates the power consumption of a job
 
  Args:
      job (Job): The Job that is about to be scheduled
 
  Returns:
      Float: A Float value indicating the duration of the job
  """
  def calculate_power_consumption(self, job):
    frequency = self.frequencies[-1]
    job_duration = job.relative_duration / frequency
    return round(job_duration, 1)
  
  """
  Schedules a job taking power consumption into consideration
 
  Args:
      current_time (int): The current time step.
      job (Job): The Job that is about to be scheduled
  """
  def schedule_energy_aware(self, current_time, job):
    self.queue.append(job)
    if not self.job:
      self.job = job
      self.job.start = current_time
      job_duration = self.calculate_power_consumption(self.job)
      max_frequency = max(self.frequencies)
      frequency = self.frequencies[-1]
      self.speed = frequency / max_frequency #all will have the same speed in my example here
      self.power = self.max_power * self.speed**2 #Das Quadrat der Slowdown-Rate wird verwendet, da die Leistungsaufnahme quadratisch mit der Taktfrequenz abnimmt
      self.job.end = int(round(current_time + job_duration,0))
      print("Job with id: " + str(self.job.id) + " has been scheduled to server " + self.id)
 
  """
  Schedules a job without taking power consumption or frequency into consideration.
 
  Args:
      current_time (int): The current time step.
      job (Job): The Job that is about to be scheduled
  """
  def schedule(self, current_time, job):
    self.queue.append(job)
    if not self.job:
      self.job = job
      self.job.start = current_time
      self.job.end = current_time + job.relative_duration 
      print("Job with id: " + str(self.job.id) + " has been scheduled to server " + self.id)      

  """
  Interrupts or finishes a job on a server. 
  Switches off the server.
  Writes stat of job to result file in order to also keep track of job interrupts.
 
  Args:
      current_time (int): The current time step.
  Returns:
       Job: A duplicate of the job which might be needed to save an interrupted job.
  """
  def shutdown(self, current_time=None):
    dup_job = None
    if self.job and current_time is not None:
      self.job.quantum = 0
      dup_job = self.job
      if (self.job.relative_duration + self.job.start) > current_time:
        print("Job with id: " + str(self.job.id) + " has been interrupted")
      else:
        print("Job with id: " + str(self.job.id) + " has been finished")    
        
      #write job already to results file in order to track when job was interrupted       
      filename = "results.txt"
      with open(filename, "a") as file:
        line = (f"{self.job.id} {self.id} {self.job.start} {self.job.end} {self.frequencies[-1]}")
        file.write(line + "\n")
      self.job = None
      self.power = 0  
    return dup_job
