# -*- coding: utf-8 -*-
from urllib.parse import urlencode
#import urllib2
from urllib.request import urlopen
import requests

from lxml import etree
from lxml import objectify

#FREE_API_KEY = ""
PREMIUM_API_KEY = "3f2087f542844b76837175557182407"

_keytype = "premium"
_key = PREMIUM_API_KEY



def internet_on():
    """fast test by trying one of google IPs"""
    try:
        #unfortunately sometimes google is unstable in China
        requests.get('http://www.google.com',timeout=3)
        return True
    except urllib.request.URLError:
        return False

def setKeyType(keytype="free"):
    """ keytype either "free" or "premium", set the key if it exists"""
    global _key, _keytype, FREE_API_KEY, PREMIUM_API_KEY

    keytype = keytype.lower()
    if (keytype == 'free'):
        _keytype = "free"
        _key = FREE_API_KEY
        return True
    elif keytype.startswith("prem") or keytype in ("nonfree", "non-free"):
        _keytype = "premium"
        _key = PREMIUM_API_KEY
        return True
    else:
        print ("invalid keytype", keytype)
        return False

def setKey(key, keytype):
    """ if keytype is valid, save a copy of key accordingly and check if the key is valid """
    global _key, _keytype, FREE_API_KEY, PREMIUM_API_KEY

    keytype = keytype.lower()
    if (keytype == 'free'):
        FREE_API_KEY = key
    elif keytype.startswith("prem") or keytype in ("nonfree", "non-free"):
        keytype = "premium"
        PREMIUM_API_KEY = key
    else:
        print ("invalid keytype", keytype)
        return

    oldkey = _key
    oldkeytype = _keytype
    _key = key
    _keytype = keytype

    w = LocalWeather("Dublin")
    if w is not None and hasattr(w, 'data') and w.data != False:
        return True
    else:
        print ("The key is not valid.")
        _key = oldkey
        _keytype = oldkeytype
        return False

    
class WWOAPI(object):
    """ The generic API interface """
    def __init__(self, q, **keywords):
        """ query keyword is always required for all APIs """
        if _key == "":
            print ("Please set key using setKey(key, keytype)")
        else:
            if internet_on():
                self.setApiEndPoint(_keytype == "free")
                self._callAPI(q=q, key=_key, **keywords)
            else:
                print ("Internet connection not available.")

    def setApiEndPoint(self, freeAPI):
        if freeAPI:
            self.apiEndPoint = self.FREE_API_ENDPOINT
        else:
            self.apiEndPoint = self.PREMIUM_API_ENDPOINT

    def _callAPI(self, **keywords):

        url = self.apiEndPoint + "?" + urlencode(keywords)
        try:
            response = requests.get(url).content
        except urllib.request.URLError:
            print ("something wrong with the API server")
            return

        # if the key is invalid it redirects to another web page
    
        if response.startswith(b'<?xml '):
            self.data = objectify.fromstring(response)
            if self.data is not None and hasattr(self.data, 'error') and self.data.error != False:
                print (self.data.error.msg)
                self.data = False
        else:
            self.data = False

class LocalWeather(WWOAPI):
    FREE_API_ENDPOINT = "http://api.worldweatheronline.com/premium/v1/weather.ashx"
    PREMIUM_API_ENDPOINT = "http://api.worldweatheronline.com/premium/v1/weather.ashx"
                            
    def __init__(self, q, num_of_days=1, **keywords):
        """ q and num_of_days are required. max 7 days for free and 15 days for premium """
        super(LocalWeather, self).__init__(
            q, num_of_days=num_of_days, **keywords)


def get_weather():
    
    setKey("3f2087f542844b76837175557182407", "premium")
    weather = LocalWeather("Dublin")

    Temp = weather.data.current_condition.temp_C
    print (Temp)
    Cloud = (weather.data.current_condition.cloudcover)
    print (Cloud)
    Results = [Temp, Cloud]
    return Results
