import threading

class RWLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.reading_count = 0
        self.writing = False

    def acquire_shared_lock(self):
        self.lock.acquire()
        if self.writing==False:
            self.reading_count +=1
            self.lock.release()
            return True
        else:
            self.lock.release()
            return False

    def acquire_exclusive_lock(self):
        self.lock.acquire()
        if self.reading_count == 0 and self.writing==False:
            self.writing = True
            self.lock.release()
            return True
        else:   
            self.lock.release()
            return False

    def release_shared_lock(self):
        self.lock.acquire()
        self.reading_count -=1
        self.lock.release()

    def release_exclusive_lock(self):
        self.lock.acquire()
        self.writing = False
        self.lock.release()