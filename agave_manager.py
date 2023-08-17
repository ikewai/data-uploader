from requests.exceptions import HTTPError
import requests
import datetime
import uuid
import json
from threading import Lock
from agavepy import Agave
import random

requests.packages.urllib3.disable_warnings() 

class AgaveManager:
    def __init__(self, config_file: str, retries: int = 0, max_backoff: float = None):
        self.__config_file = config_file
        self.__max_backoff = max_backoff
        self.__retries = retries
        with open(config_file, "r") as f:
            self.__config = json.load(f)
        self.__create_con()
        self.__ag_create_lock = Lock()

    def __create_con(self):
        try:
            self.__ag = Agave(**self.__config)
            self.__ag.token.create()
        #if issue creating token attempt to create new client and try again
        except HTTPError:
            self.__create_new_client()
            #if fail again just let it happen
            self.__ag = Agave(**self.__config)

    def __create_new_client(self):
        url = f"{self.__config['api_server']}/clients/v2/"
        client_name = f"loggernet_client_{str(uuid.uuid4())}"
        body = {
            "clientName": client_name,
            "tier": "Unlimited",
            "description": f"Created: {datetime.datetime.now().isoformat()}",
            "callbackUrl": ""
        }
        res = post(url, json = body, auth = (self.__config.username, self.__config.password), verify = False)
        key = res.json()["result"]["consumerKey"]
        secret = res.json()["result"]["consumerSecret"]
        self.__config["api_key"] = key
        self.__config["api_secret"] = secret
        #write new config info to config file
        with open(self.__config_file, "w") as f:
            json.dump(self.__config, f)

    def __get_backoff(self, delay: float):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 1-2x current backoff
        else:
            backoff = delay + random.uniform(0, delay)
        return backoff

    def __retry_wrapper(self, funct, args: dict, exceptions: tuple, retries: int, delay: float = 0):
        retry = False
        res = None
        #check if connection trying to be recreated, if it is wait for that to finish
        with self.__ag_create_lock:
            pass
        try:
            res = funct(**args)
        except exceptions as e:
            retries -= 1
            if retries < 0:
                raise e
            #try to acquire the creation lock, if already acquired another thread is handling the creation so just skip
            if self.__ag_create_lock.acquire(blocking = False):
                err = None
                try:
                    #recreate the connection
                    self.__create_con()
                except Exception as e:
                    err = e
                #make sure to release the lock even if something goes wrong creating the new connection
                finally:
                    self.__ag_create_lock.release()
                    #reraise the error now that the lock is released
                    if err is not None:
                        raise err
            #indicate need to retry
            retry = True

        if retry:
            backoff = self.__get_backoff(delay)
            if self.__max_backoff is not None:
                backoff = min(self.__max_backoff, backoff)
            res = self.__retry_wrapper(funct, args, exceptions, retries, backoff)

        return res

    def exec(self, api: str, f: str, args: dict, exceptions: tuple):
        print(self.__ag.files.manage)
        api_o = getattr(self.__ag, api)
        f_o = getattr(api_o, f)
        self.__retry_wrapper(f_o, args, exceptions, self.__retries)