import argparse
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import colorsys as cs

def get_new_color(feed=None):
    color = "#"
    if feed:

        r, g, b = cs.hls_to_rgb(((feed * 3.34876) % 16)/16, 0.7, 0.4)

        color += str(hex(int(r*256)))[2:]
        color += str(hex(int(g*256)))[2:]
        color += str(hex(int(b*256)))[2:]

    else:
        import random
        for i in range(0, 6):
            color += str(hex(random.randint(5, 15)))[2:]
    return color


# Create the argument parser
parser = argparse.ArgumentParser(description='Display a schedule.')

# Add input file argument
parser.add_argument('file', metavar='file', type=str,
                   help='the path of the results file')

# Parse the arguments
args = parser.parse_args()

# Check whether file exists
file = None
try:
    file = open(args.file)
    file.close()
except FileNotFoundError:
    print('Error : File ' + args.file + ' not found.')
    exit(1)


# Start parsing the file
print("Parsing file...")

# Keep track of the server count
server_count = -1
# Keep track of the simulation time
simulation_time = -1
# Create the Job list
jobs = []
# Open the file
file = open(args.file)
# For each line
for line in file:
    # If not a comment
    if not line.startswith('#'):
        # Split words
        values = line.split(' ')
        try:
            # Convert the words to integers
            job = [float(v) for v in values]
            # Keep track of the server count
            server_count = max(server_count, job[1])
            # Keep track of the simulation time
            simulation_time = max(simulation_time, job[3])
            # Add the parsed job to the job list
            jobs.append(job)

        except:
            print("Error : Wrong file format")
            exit(1)
# Close the file
file.close()
print("Done parsing file.")

# Create Matplotlib figure
fig1 = plt.figure()

print("Creating plots...")

# Create subplot
ax1 = fig1.add_subplot(111)

# Keep the scale on integers
ax1.set_yticks([i for i in range(int(server_count) + 1)])
ax1.set_xticks([i for i in range(int(simulation_time) + 1)])

# Set labels
ax1.set_ylabel("Server ID")
ax1.set_xlabel("Timestep")

# For all jobs
for j in jobs:
    # Add a matplotlib patch
    ax1.add_patch(
        # As a rectangle
        patches.Rectangle(
            # X is the start time, Y is the Server ID
            (j[2], j[1]-0.5),  # (x,y)
            # Width is the difference between end time and start time
            abs(j[3]-j[2]),  # width
            # Height is always 1
            1,  # height
            # We assign it a color
            facecolor=get_new_color(j[0])
        )
    )
    # Add text for the ID of the job at (almost) the same position
    ax1.text(j[2]+(abs(j[3]-j[2]))/4, j[1]-0.06, str(int(j[0]))+'('+str(int(j[4]))+')')
print("Done...")
plt.plot()
plt.show()
