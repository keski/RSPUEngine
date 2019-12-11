"""
Author: Robin Keskisärkkä

This script is used to generate multiple heart rate streams, annoatated with uncertainty, of various stream rates.
The values are randomized from a range and do not conform to any specific scenario.

TODO: Update template/generation according to use-case.
"""

import re
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import skfuzzy as fuzz

from mako.template import Template

np.random.seed(0)



def main():
    normal_sample.__name__ = "Normal"
    uniform_sample.__name__ = "Uniform"

    with open("prefixes.ttl", "r") as f:
        prefixes = f.read().replace("\n", " ")
    # Set time: 2019-11-27T18:59 CET
    reference_unix_time = 1574881152.0 + 3600

    # duration of each stream (6 minutes)
    duration = 4 * 60
    percentage = 0
    for rate in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]: #[2100, 2200, 2300, 2400, 2600, 2700, 2800, 2900, 3100, 3200, 3300, 3400]:
        # [100, 200, 300, 400, 500, 500, 600, 700, 800, 900, 1000,
        #  1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
        #  2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000,
        #  3100, 3200, 3300, 3400, 3500]:
        heart_f = open(f"../data/performance-eval/streams/heart-{rate}.trigs", "w")
        heart_f.write(prefixes + "\n")
        unix_time = reference_unix_time
        counter = 0
        step = 1.0 / rate
        print(f"\nGenerating stream: heart-{rate}.trigs)")
        for i in range(duration):
            unix_time += 1.0
            # many events per timestamp
            for j in range(rate):
                time = unix_time + j * step
                timestamp = f"\"{datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-6]}\"^^xsd:dateTime"
                counter += 1
                event = heart(counter, timestamp, uniform_sample(40, 120), 5)
                heart_f.write(re.sub("\\s+", " ", event) + "\n")

            p = int((100*i)/duration)
            if p != percentage:
                percentage = p
                if p % 5 == 0:
                    print(f"{percentage}%")

        heart_f.close()


def eval_fuzzy(fuzzy_list, sample):
    x = {}
    event_types = {}
    p_max = 0
    for fuzzy in fuzzy_list:
        p = fuzzy["f"][sample]
        event_types[fuzzy["event_type"]] = p
        if p > p_max:
            x["state"] = fuzzy["state"]
            p_max = p

    x["event_types"] = event_types
    return x


def heart(i, timestamp, mean, stddev, person="person1", sensor="hr/sensor1"):
    sample = int(normal_sample(mean, stddev, decimals=0))
    x = eval_fuzzy(fuzzy_heart_rate[1], sample)
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/HeartRate>",
        "value": f"\"Normal({sample},{stddev*stddev})\"^^rspu:distribution",
        "unc_type": x["event_types"],
        "time": timestamp,
        "state_type": ":HeartRate",
        "state": x["state"]
    }
    return template.render(data=data)


def get_fuzzy_heart_rate():
    """
     Returns a fuzzy representation of  heart rate.
    :return:
    """
    x = np.arange(0, 300, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 0, 60, 80])
    medium = fuzz.trapmf(x, [60, 80, 100, 120])
    high = fuzz.trapmf(x, [100, 120, 160, 180])
    very_high = fuzz.trapmf(x, [160, 180, 300, 300])

    return (x, [{"state": ":Low", "event_type": ":LowHeartRateEvent", "f": low},
                {"state": ":Normal", "event_type": ":NormalHeartRateEvent", "f": medium},
                {"state": ":High", "event_type": ":HighHeartRateEvent", "f": high},
                {"state": ":VeryHigh", "event_type": ":VeryHighHeartRateEvent", "f": very_high}])


def normal_sample(mu, sigma, decimals=2):
    return np.around(np.random.normal(mu, sigma), decimals)


def uniform_sample(lower, upper, decimals=2):
    return np.around(np.random.uniform(lower, upper), decimals)

template = Template(filename='templates/heart_performance.template')
fuzzy_heart_rate = get_fuzzy_heart_rate()

if __name__ == '__main__':
    main()

