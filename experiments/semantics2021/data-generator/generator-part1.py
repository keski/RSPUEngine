# Streaming data is sampled such that ~2% of the reported means
# violate the scenario thresholds.

from mako.template import Template
import datetime
import re
import random
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


def ox_sensor_uniform():
    return f"U(-0.01,0.01)"
    
def ox_sensor_normal():
    return f"N(0,0.005)"

def temp_sensor_uniform():
    return f"U(-2,2)"
    
def temp_sensor_normal():
    return f"N(0,0.5)"

def generate_static(number_of_locations, filename=None):
    # Generate static data
    data = {
        "number_of_locations": number_of_locations,
        "ox_sensor_mu": ox_sensor_uniform,
        "temp_sensor_mu": temp_sensor_uniform
    }

    template = Template(filename=f'resources/static-part1.template')
    if filename:
        f = open(filename, "w")
        f.write(template.render(data=data))
        f.close()
    else:
        print(template.render(data=data))

def generate_streams(number_of_locations, duration, directory):
    stream1 = []
    stream2 = []
    stream3 = []
    stream4 = []
    t0 = datetime.datetime.fromisoformat("2021-03-01T10:00:00.000000")
    template = Template(filename='resources/stream-part1.template')
    for obs_count in range(duration):
        time = t0 + datetime.timedelta(seconds=obs_count)
        for loc_count in range(number_of_locations):
            common = { "obs_count": obs_count, "loc_count": loc_count, "time": time.isoformat() }

            # sample oxygen around "normal"
            mu, sigma, error = 0.21, 0.015, 0.04
            ox_1 = np.random.uniform(mu-error, mu+error)
            ox_2 = np.random.normal(mu, sigma)

            # sample temperature around "normal"
            mu, sigma, error = 26, 2, 5
            temp_1 = np.random.uniform(mu-error, mu+error)
            temp_2 = np.random.normal(mu, sigma)

            obs1 = { "type": "OxygenSensorType1", "prop": "oxygen", "result": ox_1 }
            obs2 = { "type": "OxygenSensorType2", "prop": "oxygen", "result": ox_2, "mu": ox_sensor_normal() }
            obs3 = { "type": "TemperatureSensorType1", "prop": "temperature", "result": temp_1 }
            obs4 = { "type": "TemperatureSensorType2", "prop": "temperature", "result": temp_2, "mu": temp_sensor_normal() }
            stream1.append(template.render(data={**common, **obs1}))
            stream2.append(template.render(data={**common, **obs2}))
            # Generate temperature stream only for even numbered locations
            if loc_count % 2 == 0:
                stream3.append(template.render(data={**common, **obs3}))
                stream4.append(template.render(data={**common, **obs4}))
    

    if directory:
        with open(f"resources/prefixes-part1.trigstar", "r") as f:
            prefixes = re.sub("\s+", " ", f.read()) + "\n"
        
        with open(f"{directory}/ox1.trigstar", "w") as f:
            f.write(prefixes)
            for e in stream1: f.write(re.sub("\s+", " ", e) + "\n")
        with open(f"{directory}/ox2.trigstar", "w") as f:
            f.write(prefixes)
            for e in stream2: f.write(re.sub("\s+", " ", e) + "\n")
        with open(f"{directory}/temp1.trigstar", "w") as f:
            f.write(prefixes)
            for e in stream3: f.write(re.sub("\s+", " ", e) + "\n")
        with open(f"{directory}/temp2.trigstar", "w") as f:
            f.write(prefixes)
            for e in stream4: f.write(re.sub("\s+", " ", e) + "\n")
        f.close()
    else:
        print(stream1)
        print(stream2)
        print(stream2)
        print(stream4)

def test_plots():
    #mu, sigma = 0.21, 0.015
    #s = np.random.normal(mu, sigma, 10000)

    # Create the bins and histogram
    #count, bins, ignored = plt.hist(s, 100)
    #plt.axvline(x=0.18, color='k', linestyle='--')

    # Plot the distribution curve
    #plt.show()

    #mu, sigma = 26, 2
    #s = np.random.normal(mu, sigma, 10000)

    # Create the bins and histogram
    #count, bins, ignored = plt.hist(s, 100)
    #plt.axvline(x=30, color='k', linestyle='--')
    #plt.show()

    mu, scale = 0, 1
    s1 = np.random.uniform(mu, scale, 10000)


    s2 = np.random.uniform(mu, scale, 10000)

    # Create the bins and histogram
    count, bins, ignored = plt.hist(s1+s2, 200)
    plt.axvline(x=0.18, color='k', linestyle='--')

    # Plot the distribution curve
    plt.show()

def generate_selectivity_measures():
    total = 100

    calc_filter1 = False
    calc_filter2 = True
    calc_filter3 = False
    calc_filter4 = False

    # distributions used for sampling
    mu, sigma, error = 0.21, 0.015, 0.04
    ox_samples1 = np.random.uniform(mu-error, mu+error, total)
    ox_samples2 = np.random.normal(mu, sigma, total)

    # distributions used for sampling
    mu, sigma, error = 26, 2, 5
    temp_samples1 = np.random.uniform(mu-error, mu+error, total)
    temp_samples2 = np.random.normal(mu, sigma, total)

    # error and sigma
    ox_error = 0.01
    ox_sigma = 0.005

    # error and sigma
    temp_error = 2
    temp_sigma = 0.5

    
    if calc_filter1:
        print("Filter 1")
        selectivities = []
        thresholds = []
        temp_threshold = 30
        # THRESHOLDS:    [0, 0.01, 0.1, 0.5, 0.9]
        # SELECTIVITIES: [0.251, 0.248, 0.226, 0.121, 0.026]
        # Regular-filter selectivity: 0.8988
        for P in [0, .01, .1, .5, .9]:
            regular_filter = 0
            unc_filter = 0
            
            for ox, temp  in zip(ox_samples1, temp_samples1):
                if temp < temp_threshold:
                    regular_filter += 1
                
                rv = stats.uniform(loc=ox-ox_error, scale=2*ox_error)
                if less_than(rv, 0.18) > P:
                    unc_filter += 1
            selectivities.append(round(unc_filter/total, 3))
            thresholds.append(round(P, 3))
            
        print("THRESHOLDS:   ", thresholds)
        print("SELECTIVITIES:", selectivities)
        print("Regular-filter selectivity:", regular_filter/total)
        print("-----------")

    if calc_filter2:
        print("Filter 2")
        selectivities = []
        thresholds = []
        temp_threshold = 30
        # THRESHOLDS:    [0, 0.01, 0.1, 0.5, 0.9]
        # SELECTIVITIES: [0.98, 0.68, 0.55, 0.42, 0.29]
        # Regular-filter selectivity: 0.51
        for P in [0, .01, .1, .5, .9]:
            regular_filter = 0
            unc_filter = 0
            
            for i in range(total):
                if i == 0 or i == total-1: continue

                if (temp_samples2[i] > temp_samples2[i+1] or temp_samples2[i] > temp_samples2[i-1]):
                    if (ox_samples2[i] > ox_samples2[i+1] or ox_samples1[i] > ox_samples1[i-1]):
                        regular_filter += 1
                
                rv1 = stats.norm(loc=temp_samples2[i], scale=temp_sigma)
                rv2 = stats.norm(loc=temp_samples2[i+1], scale=temp_sigma)
                rv3 = stats.norm(loc=temp_samples2[i-1], scale=temp_sigma)
                
                if prob_greater_than(rv1, rv2) < P and prob_greater_than(rv1, rv3) < P:
                    continue

                rv1 = stats.norm(loc=ox_samples1[i], scale=ox_sigma)
                rv2 = stats.norm(loc=ox_samples1[i+1], scale=ox_sigma)
                rv3 = stats.norm(loc=ox_samples1[i-1], scale=ox_sigma)
                
                if prob_greater_than(rv1, rv2) < P and prob_greater_than(rv1, rv3) < P:
                    continue

                unc_filter += 1
            selectivities.append(round(unc_filter/total, 3))
            thresholds.append(round(P, 3))
            
        print("THRESHOLDS:   ", thresholds)
        print("SELECTIVITIES:", selectivities)
        print("Regular-filter selectivity:", regular_filter/total)
        print("-----------")


def less_than(rv, upper):
    return rv.cdf(upper)

def greater_than(rv, lower):
    return 1- rv.cdf(lower)

def between(rv, lower, upper):
    return less_than(rv, upper) - less_than(rv, lower)

def prob_greater_than(rv1, rv2):
    count = 10000
    gt_count  = 0
    values1 = rv1.rvs(count)
    values2 = rv2.rvs(count)

    for v1, v2 in zip(values1, values2):
        if v1 > v2:
            gt_count += 1

    return gt_count/count

if __name__ == '__main__':
    generate_static(1000, "../../../resources/data/static.trigstar")
    generate_streams(1000, 300, "../../../resources/data/")
    #generate_selectivity_measures()
    #test_plots()

