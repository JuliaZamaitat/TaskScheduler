def show_scheduling_results(servers):
    filename = "results.txt"
    with open(filename, "w") as file:
        file.write("#jobID serverID starting_time ending_time frequency_used\n")
        file_content = []
        for server in servers:
            for job in server.queue:
                file_content.append(f"{job.id} {server.id} {job.start} {job.end} {job.period}")
        file_content.sort(key=lambda line: int(line.split(' ')[0]))
        for line in file_content:
            file.write(line + "\n")
            # if float(line.split(' ')[3]) < 20:
            #     file.write(line + "\n")
            # else:
            #     # to keep the graph compact
            #     # if the periodicity is too high, we comment it for python
            #     file.write("#arrival > 20, exclude from graph plot\n#" + line + "\n")
