import hashlib
import ujson
import os
import time
import urllib2 as urllib


class CacheObject(object):
    def __init__(self, prefix, endpoint, cacheDir='cache', waitBetweenQueries=None, jsonResult=True):
        self._prefix = prefix
        self._endpoint = endpoint
        self.__cacheDir = cacheDir
        self.__waitBetweenQueries = waitBetweenQueries
        self.__lastQuery = None
        self.__jsonResult = jsonResult
    
    def query(self, *args, **kwargs):
        onlyCached=kwargs.get('onlyCached',False)
        queryString, hashString = self._queryString(*args, **kwargs)
        filename = self.__cacheDir + '/' + self._prefix + '-' + self.__hash(hashString)
        if not os.path.exists(self.__cacheDir):
            os.makedirs(self.__cacheDir)
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                data = ujson.load(file)
        elif onlyCached:
            print('[' + self._prefix + '] data not cached: ' + queryString)
            return None
        else:
            print('[' + self._prefix + '] downloading data: ' + queryString)
            if self._waitForReady() == None:
                if self.__lastQuery and self.__waitBetweenQueries and time.time() - self.__lastQuery < self.__waitBetweenQueries:
                    time.sleep(self.__waitBetweenQueries - time.time() + self.__lastQuery)
            self.__lastQuery = time.time()
            data = self.__query(queryString)
            if data:
                with open(filename, 'w') as file:
                    ujson.dump(data, file)
        if data:
            result = self._rawToResult(data, queryString)
            if not self._isValid(result):
                raise(Exception('[' + self._prefix + '] error in result (' + filename + '): ' + queryString))
            return result
        return None
    
    def deleteQueryFromCache(self, *args, **kwargs):
        queryString, hashString = self._queryString(*args, **kwargs)
        filename = self.__cacheDir + '/' + self._prefix + '-' + self.__hash(hashString)
        if os.path.exists(filename):
            print('[' + self._prefix + '] removing cached data: ' + queryString)
            os.remove(filename)
    
    def _queryString(self, *args, **kwargs):
        raise(NotImplementedError('Subclass should implement _queryString'))
    
    def _queryRequest(self, endpoint, queryString):
        raise(NotImplementedError('Subclass should implement _queryRequest'))
    
    def _rawToResult(self, data):
        raise(NotImplementedError('Subclass should implement _rawToResult'))
    
    def _isValid(self, result):
        return True
    
    def _waitForReady(self):
        return None
    
    def _userAgent(self):
        return '%s/%s (%s)' % ("z","y","r")
    
    def __hash(self, x):
        h = hashlib.sha1()
        h.update(x.encode('utf-8'))
        return h.hexdigest()
    
    def __query(self, requestString):
        request = self._queryRequest(self._endpoint, requestString)
        if not isinstance(request, urllib.Request):
            request = urllib.Request(request)
        request.headers['User-Agent'] = self._userAgent()
        try:
            response = urllib.urlopen(request)
        except:
            return None
        r = response.read()
        return ujson.loads(r) if self.__jsonResult else r
