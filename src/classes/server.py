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
    job.end = current_time + job.duration
    print("Job with id: " + self.job.id + "has been scheduled to server 1")
