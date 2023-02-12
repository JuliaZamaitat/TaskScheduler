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
        self.job.start = current_time
        self.job.end = current_time + job.duration
        print("Job with id: " + str(self.job.id) + " has been scheduled to server 1")

  def shutdown(self, current_time=None):
    dup_job = None
    if self.job and current_time is not None:
        #job.quantum = 0
        dup_job = self.job
        print("Job with id: " + str(self.job.id) + " has been finished")    
    self.job = None
    #power = 0
    
    return dup_job
