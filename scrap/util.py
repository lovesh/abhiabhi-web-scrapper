import re

def replaceKey(dictionary,findkey,replacekey):
    if findkey in dictionary:
        dictionary[replacekey]=dictionary[findkey]
        del(dictionary[findkey])
        return True
    return False
