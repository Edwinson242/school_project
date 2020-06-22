# -*- coding: utf-8 -*-

# Import modules
import requests  # to make Internet requests
import json as JSON  # to manage data
import datetime


def next_datetime():
    """Return next monday at 9am datetime string
        Needs the `python-dateutil` module: pip install python-dateutil

    Returns:
        [str] -- next monday date: YYYY-MM-DDThh:mm:ss
    """

    now = datetime.datetime.now()
    day = 0  # next monday
    now_9am = now.replace(hour=9, minute=0, second=0, microsecond=0)  # at 9am
    next_monday = now_9am + datetime.timedelta(days=(day - now_9am.weekday() + 7) % 7)
    return next_monday.replace(microsecond=0).isoformat()


# function will take an address as parameter and return coordinates

def geocoding(address1):
    headerDict = {"Authorization": 'f84788b8-abf9-4d74-bd46-f33016febc56'}
    url_geocoding = 'http://api.navitia.io/v1/places?q={}&type[]=address'.format(address1)

    r = requests.get(url_geocoding, headers=headerDict)

    returned_data = r.text

    data = JSON.loads(returned_data)

    return data['places'][0]["address"]["coord"]

def best_journey(start, end):
        headerDict = {"Authorization": 'f84788b8-abf9-4d74-bd46-f33016febc56'}
        next_monday = next_datetime()
        url_geocoding = 'http://api.navitia.io/v1/journeys?' \
                        'from={};{}&to={};{}&datetime={}&datetime_represents=' \
                        'arrival'.format(start['lon'], start['lat'], end['lon'], end['lat'], next_monday)
        r = requests.get(url_geocoding, headers=headerDict)
        returned_data = r.text
        data = JSON.loads(returned_data)

        return data["journeys"][0]["duration"]




# ________________________________________________________________________________________



start = {'lat': '48.841076', 'lon': '2.586311'}
end = {'lat': '48.8573352', 'lon': '2.347556'}
journey = best_journey(start, end)

print(journey)


