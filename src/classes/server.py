class Server:
  def __init__(self, id, frequencies):
    self.id = id
    self.frequencies = frequencies
    self.job = None
    self.queue = []
    
  def call(self, current_time, job):
      self.queue.append(job)
      if not self.job:
        self.job = job
        self.job.start = current_time #start beim ERSTEN start
        self.job.end = current_time + job.relative_duration #finish beim LETZTEN finish
        print("Job with id: " + str(self.job.id) + " has been scheduled to server " + self.id)

  def shutdown(self, current_time=None):
    dup_job = None
    if self.job and current_time is not None:
        self.job.quantum = 0
        dup_job = self.job
        print(self.job.id, self.job.relative_duration,self.job.duration, self.job.start, self.job.end)
        if (self.job.relative_duration + self.job.start) > current_time:
          print("Job with id: " + str(self.job.id) + " has been interrupted")
        else:
          print("Job with id: " + str(self.job.id) + " has been finished")
        
        #write job already to results file in order to track when job was interrupted       
        filename = "results.txt"
        with open(filename, "a") as file:
          line = (f"{self.job.id} {self.id} {self.job.start} {self.job.end} {self.job.period}")
          file.write(line + "\n")
        
    self.job = None
    #power = 0
    
    return dup_job
