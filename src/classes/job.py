class Job:
    def __init__(self, id, arrival, duration, deadline, period):
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
