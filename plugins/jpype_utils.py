import jpype
import jpype.imports
from jpype.types import *




def startJVM(*args, **kwargs):
    """
    Starts a jpype JVM if it isn't already started.
    If it is started, args and kwargs will be ignored.
    """
    try:
        jpype.startJVM(*args, **kwargs)
    except OSError as e:
        if str(e) != 'JVM is already started':
            raise e

def list_to_arraylist(l):
    """
    Converts a Python list to a jpype java ArrayList
    """
    startJVM()
    from java.util import ArrayList

    if isinstance(l, ArrayList):
        return l
    elif l is None:
        return JObject(None, ArrayList)
    else:
        al = ArrayList(len(l))
        al.extend(l)
        return al

def dict_to_hashmap(d):
    """
    Converts a python dict to a jpyp Java HashMap.
    """
    startJVM()
    from java.util import HashMap
    if isinstance(d, HashMap):
        return d
    elif d is None:
        return JObject(None, HashMap)
    else:
        hm = HashMap()
        for k, v in d.items():
            hm.put(k, v)
        return hm
