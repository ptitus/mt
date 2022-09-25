from enum import Enum

class SessionState(Enum):
    Closed = 0 # initialized
    Connected = 1
    Failed = 2
 
class SessionError(Exception):
    """Raised when session could not be created or closed"""
    def __init__(self, message):
        print(message)

class RequestState(Enum):
    Idle = 0 #initialized
    Called = 1
    Accepted = 2
    NotFound = 3
    FloodWait = 4
    TakeoutWait = 5
    Failed = 9

class RequestError(Exception):
    """Raised when Requests get blocked or failed"""
    def __init__(self, message):
        print(message)

class ScrapeState(Enum):
    Init = 0 # initialized
    AnalyzingEntities = 1
    AnalyzingMessages = 2
    NoFreeSessions = 8
    Failed = 9

class ScrapeError(Exception):
    """Raised when Scraping failes"""
    def __init__(self, message):
        print(message)

