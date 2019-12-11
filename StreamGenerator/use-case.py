"""
Author: Robin Keskisärkkä

This script can be used to generate uncertain data streams. The generated streams are aligned with respect to time, and
change with respect to "triggers" on a timeline. Each trigger results in a change in the mean values for the generated
events, allowing specific scenarios to be used to as a reference for the generated streams.

Each stream is written to a separate file, and each streamed element is written to a single line in trigs syntax.
"""

import re
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import skfuzzy as fuzz

from mako.template import Template

np.random.seed(0)


# Sensors:
# - activity: reported by system, no uncertainty
# - heart rate: 1 sensor on body
# - breathing rate: 1 sensor on body
# - temperature: 1 sensor on body
# - oxygen saturation: 1 sensor on body


def plot_memberships():
    # Visualize the membership functions
    memberships = [
        ("Breathing rate", get_fuzzy_breathing()),
        ("Heart rate", get_fuzzy_heart_rate()),
        ("Oxygen saturation", get_fuzzy_oxygen()),
        ("Temperature", get_fuzzy_temperature())
    ]

    fig, plots = plt.subplots(nrows=2, ncols=2, figsize=(10, 6))

    i = 0
    colors = ["r", "b", "g", "y"]
    for i in [0,1]:
        for j in [0, 1]:
            name = memberships[i*2+j][0]
            x, fuzz_dict = memberships[i*2+j][1]
            c = 0
            for d in fuzz_dict:
                plots[i][j].plot(x, d["f"], colors[c], linewidth=1.5, label=d["event_type"])
                c += 1
            plots[i][j].set_title(name)
            plots[i][j].legend()

            # Turn off top/right axes
            plots[i][j].spines['top'].set_visible(False)
            plots[i][j].spines['right'].set_visible(False)
            plots[i][j].get_xaxis().tick_bottom()
            plots[i][j].get_yaxis().tick_left()

    plt.tight_layout()
    plt.show()


def main():
    # uncomment to visualize membership functions
    # plot_memberships()

    normal_sample.__name__ = "Normal"
    uniform_sample.__name__ = "Uniform"

    heart_f = open("../data/use-case/streams/heart.trigs", "w")
    oxygen_f = open("../data/use-case/streams/oxygen.trigs", "w")
    breathing_f = open("../data/use-case/streams/breathing.trigs", "w")
    temperature_f = open("../data/use-case/streams/temperature.trigs", "w")
    activity_f = open("../data/use-case/streams/activity.trigs", "w")

    # write prefixes
    with open("prefixes.ttl", "r") as f:
        prefixes = f.read().replace("\n", " ")
    heart_f.write(prefixes + "\n")
    oxygen_f.write(prefixes + "\n")
    breathing_f.write(prefixes + "\n")
    temperature_f.write(prefixes + "\n")
    activity_f.write(prefixes + "\n")

    scenario = [
        {
            "duration": 30,
            "heart_rate": 60,
            "oxygen": 95,
            "breathing": 14,
            "temperature": 37,
            "activity": "Resting"
        },
        {
            "duration": 30,
            "heart_rate": 80,
            "oxygen": 95,
            "breathing": 14,
            "temperature": 37,
            "activity": "Resting"
        },
        {
            "duration": 30,
            "heart_rate": 100,
            "oxygen": 94,
            "breathing": 14,
            "temperature": 38,
            "activity": "Sleeping"
        },
        {
            "duration": 30,
            "heart_rate": 120,
            "oxygen": 90,
            "breathing": 20,
            "temperature": 38.5,
            "activity": "Sleeping"
        },
        {
            "duration": 30,
            "heart_rate": 130,
            "oxygen": 89,
            "breathing": 30,
            "temperature": 38.8,
            "activity": "Sleeping"
        }, {
            "duration": 30,
            "heart_rate": 165,
            "oxygen": 89,
            "breathing": 25,
            "temperature": 38.8,
            "activity": "Sleeping"
        }, {
            "duration": 30,
            "heart_rate": 140,
            "oxygen": 85,
            "breathing": 24,
            "temperature": 38.0,
            "activity": "Sleeping"
        }
    ]

    # Set time: 2019-11-27T19:59
    unix_time = 1574881152

    counter = 0
    for x in scenario:
        for i in range(x["duration"]):
            counter += 1
            unix_time += 1
            timestamp = f"\"{datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%dT%H:%M:%SZ')}\"^^xsd:dateTime"

            e1 = heart(counter, timestamp, x["heart_rate"], 5)
            heart_f.write(re.sub("\\s+", " ", e1) + "\n")

            e2 = oxygen(counter, timestamp, x["oxygen"], 2)
            oxygen_f.write(re.sub("\\s+", " ", e2) + "\n")

            e3 = breathing(counter, timestamp, x["breathing"], 2)
            breathing_f.write(re.sub("\\s+", " ", e3) + "\n")

            e4 = temperature(counter, timestamp, x["temperature"], 0.5)
            temperature_f.write(re.sub("\\s+", " ", e4) + "\n")

            e5 = activity(counter, timestamp, x["activity"])
            activity_f.write(re.sub("\\s+", " ", e5) + "\n")

    heart_f.close()
    oxygen_f.close()
    breathing_f.close()
    temperature_f.close()
    activity_f.close()

    for e in [e1, e2, e3, e4, e5]:
        print(e)


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
    x = eval_fuzzy(get_fuzzy_heart_rate()[1], sample)
    if x['state'] == "VeryHigh":
        x['state'] = "High"
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/HeartRate>",
        "value": sample,
        "value_error": f"\"Normal(0,{stddev*stddev})\"^^rspu:distribution",
        "unc_type": x["event_types"],
        "time": timestamp,
        "state_type": "ecare:HeartRate",
        "state": f"{x['state']}"
    }
    template = Template(filename='templates/heart.template')
    return template.render(data=data)


def oxygen(i, timestamp, mean, error, person="person1", sensor="oxygen/sensor1"):
    sample = int(uniform_sample(mean - error, mean + error))
    x = eval_fuzzy(get_fuzzy_oxygen()[1], sample)
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/OxygenSaturation>",
        "value": sample,
        "value_error": f"\"Uniform({-error},{error})\"^^rspu:distribution",
        "unc_type": x["event_types"],
        "time": timestamp,
        "state_type": "ecare:OxygenSaturation",
        "state": f"{x['state']}"
    }
    template = Template(filename='templates/oxygen.template')
    return template.render(data=data)


def breathing(i, timestamp, mean, error, person="person1", sensor="breathing/sensor1"):
    sample = int(uniform_sample(mean - error, mean + error))

    x = eval_fuzzy(get_fuzzy_breathing()[1], sample)
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/BreathingRate>",
        "value": sample,
        "value_error": f"\"Uniform({-error},{error})\"^^rspu:distribution",
        "unc_type": x["event_types"],
        "time": timestamp,
        "state_type": "ecare:BreathingRate",
        "state": f"{x['state']}"
    }
    template = Template(filename='templates/breathing.template')
    return template.render(data=data)


def temperature(i, timestamp, mean, stddev, person="person1", sensor="temperature/sensor1"):
    sample = normal_sample(mean, stddev, decimals=1)

    x = eval_fuzzy(get_fuzzy_temperature()[1], int(sample*10))
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/Temperature>",
        "value": sample,
        "value_error": f"\"Normal(0,{stddev**stddev})\"^^rspu:distribution",
        "unc_type": x["event_types"],
        "time": timestamp,
        "state_type": "ecare:Temperature",
        "state": f"{x['state']}"
    }
    template = Template(filename='templates/temperature.template')
    return template.render(data=data)


def activity(i, timestamp, current_activity, person="person1", sensor="activity/sensor1"):
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/Activity>",
        "value": current_activity,
        "time": timestamp,
        "state_type": "ecare:Activity",
        "state": f"{current_activity}"
    }
    template = Template(filename='templates/activity.template')
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

    return (x, [{"state": "Low", "event_type": "LowHeartRateEvent", "f": low},
                {"state": "Normal", "event_type": "NormalHeartRateEvent", "f": medium},
                {"state": "High", "event_type": "HighHeartRateEvent", "f": high},
                {"state": "VeryHigh", "event_type": "VeryHighHeartRateEvent", "f": very_high}])


def get_fuzzy_breathing():
    """
     Returns a fuzzy representation of  heart rate.
    :return:
    """
    x = np.arange(0, 60, 1)
    # Fuzzy membership functions
    low = fuzz.trimf(x, [0, 7, 9])
    normal = fuzz.trapmf(x, [8, 12, 16, 20])
    high = fuzz.trapmf(x, [18, 60, 100, 100])

    return (x, [{"state": "Low", "event_type": "SlowBreathingRateEvent", "f": low},
                {"state": "Normal", "event_type": "NormalBreathingRateEvent", "f": normal},
                {"state": "High", "event_type": "ElevatedBreathingRateEvent", "f": high}])


def get_fuzzy_oxygen():
    """
    Returns a fuzzy representation of oxygen saturation.
    :return:
    """
    x = np.arange(0, 100, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 0, 88, 90])
    normal = fuzz.trapmf(x, [88, 92, 100, 100])

    return (x, [{"state": "Low", "event_type": "LowOxygenSaturationEvent", "f": low},
                {"state": "Normal", "event_type": "NormalOxygenSaturationEvent", "f": normal}])


def get_fuzzy_temperature():
    """
    Returns a fuzzy representation of body temperature.
    :return:
    """
    x = np.arange(0, 450, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [300, 300, 360, 365])
    normal = fuzz.trapmf(x, [360, 365, 375, 385])
    high = fuzz.trapmf(x, [375, 385, 420, 450])

    return (x, [{"state": "Low", "event_type": "LowTemperatureEvent", "f": low},
                {"state": "Normal", "event_type": "NormalTemperatureEvent", "f": normal},
                {"state": "High", "event_type": "HighTemperatureEvent", "f": high}])


def normal_sample(mu, sigma, decimals=2):
    return np.around(np.random.normal(mu, sigma), decimals)


def uniform_sample(lower, upper, decimals=2):
    return np.around(np.random.uniform(lower, upper), decimals)


if __name__ == '__main__':
    main()
