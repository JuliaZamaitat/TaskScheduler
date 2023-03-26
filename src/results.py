import matplotlib.pyplot as plt

def prepare_result_file():
    filename = "results.txt"
    with open(filename, "w") as file:
        file.write("#jobID serverID starting_time ending_time frequency_used\n")
       
def sort_result_file():
    filename = "results.txt"
    with open(filename, "r+") as file:
        lines = file.readlines()
        comment_lines = [line for line in lines if line.startswith('#')]
        non_comment_lines = [line for line in lines if not line.startswith('#')]
        sorted_lines = sorted(non_comment_lines, key=lambda line: int(line.split(' ')[0]))
        file.seek(0)
        file.truncate()
        file.writelines(comment_lines)
        file.writelines(sorted_lines)

def power_results(power_stats):
    with open("power_stats.txt", "w") as file:
        file.write(str(power_stats))
    plot_power_consumption(power_stats)    

def plot_power_consumption(power_stats):
    time = range(len(power_stats))
    plt.plot(time, power_stats)
    plt.xlabel('Time')
    plt.ylabel('Power Consumption')
    plt.savefig("power_consumption.png")