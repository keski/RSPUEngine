from mako.template import Template
import datetime
import re
import random

def uniform_sample(lower, upper):
    return random.uniform(lower, upper)

def ox_sensor_uniform():
    return f"U(0,0.03)"
    
def ox_sensor_normal():
    return f"N(0,0.01)"

def temp_sensor_uniform():
    return f"U(0,3)"
    
def temp_sensor_normal():
    return f"N(0,1)"

def generate_static(number_of_locations, filename=None):
    # Generate static data
    data = {
        "number_of_locations": number_of_locations,
        "ox_sensor_mu": ox_sensor_uniform,
        "temp_sensor_mu": temp_sensor_uniform
    }

    template = Template(filename=f'resources/data.template')
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
    template = Template(filename='resources/stream.template')
    for obs_count in range(duration):
        time = t0 + datetime.timedelta(seconds=obs_count)
        for loc_count in range(number_of_locations):
            common = { "obs_count": obs_count, "loc_count": loc_count, "time": time.isoformat()
            }
            obs1 = { "type": "OxygenSensorType1", "prop": "oxygen", "result": uniform_sample(0.18-0.01,0.18+0.09) }
            obs2 = { "type": "OxygenSensorType2", "prop": "oxygen", "result": uniform_sample(0.18-0.01,0.18+0.09), "mu": ox_sensor_normal() }
            obs3 = { "type": "TemperatureSensorType1", "prop": "temperature", "result": uniform_sample(30-9,30+1) }
            obs4 = { "type": "TemperatureSensorType2", "prop": "temperature", "result": uniform_sample(30-9,30+1), "mu": temp_sensor_normal() }
            stream1.append(template.render(data={**common, **obs1}))
            stream2.append(template.render(data={**common, **obs2}))
            stream3.append(template.render(data={**common, **obs3}))
            stream4.append(template.render(data={**common, **obs4}))
    

    if directory:
        with open(f"resources/prefixes.trigstar", "r") as f:
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

if __name__ == '__main__':
    generate_static(10, "../../../resources/data/static.trigstar")
    generate_streams(10, 60, "../../../resources/data/")