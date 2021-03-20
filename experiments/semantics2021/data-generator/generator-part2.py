# Streaming data is sampled such that ~2% of the reported means
# violate the scenario thresholds.

from mako.template import Template
import datetime
import re
import random
import numpy as np
import matplotlib.pyplot as plt
#from scipy import stats


def get_randints(lower, upper, exclude):
    """Generate a set of unique random numbers in a range (inclusive)."""
    numbers = [i for i in range(lower, upper, 1)]
    numbers.remove(exclude)
    random.shuffle(numbers)
    return numbers 

def generate_streams(event_per_second, duration, directory):
    with open(f"resources/prefixes.trigstar", "r") as f:
        prefixes = re.sub("\s+", " ", f.read()) + "\n"

    f1 = open(f"{directory}/events1.trigstar", "w")
    f1.write(prefixes)
    f2 = open(f"{directory}/events2.trigstar", "w")
    f2.write(prefixes)

    stream1 = []
    stream2 = []
    
    t0 = datetime.datetime.fromisoformat("2021-03-01T10:00:00.000000")
    template = Template(filename='resources/stream-part2.template')
    
    for i in range(duration):
        time = t0 + datetime.timedelta(seconds=i)
        
        values1 = np.random.uniform(0, 1, event_per_second)
        values2 = np.random.uniform(0, 1, event_per_second)
            
        for j in range(event_per_second):            
            # ids
            id1 = f"1_{j}_{i}"
            id2 = f"2_{j}_{i}"

            # sample around standard normal (mu=0, sigma=1)
            mu = 0
            sigma = 1
            value1 = values1[j]
            value2 = values2[j]
            #value1 = np.random.normal(mu, sigma)
            #value2 = np.random.normal(mu, sigma)

            # get random numbers
            random_joins = get_randints(0, event_per_second, j)
            joins1 = {
                "join_1": f"2_{random_joins[0]}_{i}",
                "join_10":  [f"2_{x}_{i}" for x in random_joins[:10]],
                "join_100": [f"2_{x}_{i}" for x in random_joins[:100]],
                "join_01":  f"2_{random_joins[0]}_{i}" if j % 10 == 0 else None,
                "join_001": f"2_{random_joins[0]}_{i}" if j % 100 == 0 else None
            }

            # get new list of random numbers
            random_joins = get_randints(0, event_per_second, j)
            joins2 = {
                "join_1": f"1_{random_joins[0]}_{i}",
                "join_10": [f"1_{x}_{i}" for x in random_joins[:10]],
                "join_100": [f"1_{x}_{i}" for x in random_joins[:100]],
                "join_01":  f"1_{random_joins[0]}_{i}" if j % 10 == 0 else None,
                "join_001": f"1_{random_joins[0]}_{i}" if j % 100 == 0 else None
            }

            e1 = template.render(data={"id": id1, "value": value1, "joins": joins1, "time": time.isoformat() })
            e2 = template.render(data={"id": id2, "value": value2, "joins": joins2, "time": time.isoformat() })
            f1.write(re.sub("\s+", " ", e1) + "\n")
            f2.write(re.sub("\s+", " ", e2) + "\n")
    f1.close()
    f2.close()


if __name__ == '__main__':
    generate_streams(100, (50+5)*10+5, "../../../resources/data/")
    #v1 = np.random.uniform(0, 1, 500)
    #v2 = np.random.uniform(0, 1, 500)
    
    #import matplotlib.pyplot as plt
    #count, bins, ignored = plt.hist(v1, 10, density=True)
    #plt.plot(bins, np.ones_like(bins), linewidth=2, color='r')
    #plt.show()

    #count, bins, ignored = plt.hist(v2, 10, density=True)
    #plt.plot(bins, np.ones_like(bins), linewidth=2, color='r')
    #plt.show()

