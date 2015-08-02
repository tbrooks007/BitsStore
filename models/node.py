from utils import date_utils

MAX_SECONDS = 2592000 #30 DAYS

class Node(dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, args)

        self.update(*args, **kwargs)

        self.__expire_keys_dict = {}
        self.name = kwargs.get('name', None)

    def __getitem__(self, key):

        if not self.__is_expired(key):
            val = dict.__getitem__(self, key)
            return val

        return None

    def stats(self):
        """
            Return nodes stats (memory usage, etc)
        """
        raise NotImplementedError("Not implemented here")

    def set_with_expire(self, key, value, expiration_secs):

        #if the expiration is 0 the key will live forever so don't waste time storing the key
        if expiration_secs == 0:
            return

        if expiration_secs > MAX_SECONDS:
            raise Exception('Expiration time can not exceed 30 days (%s seconds)' % MAX_SECONDS)

        #calculate expiration utc datetime
        current = date_utils.get_current_utc_datetime()
        expiration_dt_utc = date_utils.addSeconds(expiration_secs, current)

        self.__expire_keys_dict[key] = expiration_dt_utc
        dict.__setitem__(self, key, value)

    def __is_expired(self, key):

        expire_dt = self.__expire_keys_dict.get(key, None)

        if expire_dt:
            current = date_utils.get_current_utc_datetime()
            return expire_dt > current

        return False



