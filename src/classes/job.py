class Job:
    def __init__(self, id, arrival, duration, deadline, period):
        """
        Constructor for Job
        
        Args:
            id (int): An integer giving the id of the server
            arrival (int): An integer giving the arrival time of a job
            duration (int): An integer giving the duration of a job
            deadline (int): An integer giving the deadline time of a job
            period (int): An integer giving the period of a job
      """
        self.id = id
        self.arrival = arrival
        self.duration = duration
        self.relative_duration = duration
        self.relative_deadline = arrival + deadline
        self.deadline = deadline
        self.period = period
        self.start = -1
        self.end = -1
        self.quantum = 0
        self.nbPeriod = 0
