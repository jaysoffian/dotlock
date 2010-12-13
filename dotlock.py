#!/usr/bin/env python
# -*- mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
# ex: set tabstop=4 expandtab:
# Please do not change the two lines above. See PEP 8, PEP 263.
#
# $Id: dotlock.py 3511 2008-02-08 03:37:32Z jay $

'''
DotLock - A class to allow files to be locked over NFS reliably.

NFS historically did not provide locking. NFS itself is a stateless protocol on
the server side, and locking is inheritently stateful. RPC lockd/statd were
added in NFSv3 to provide locking via lockf/flocl/fnctl. However, they are
generally considered to be unreliable, and may not be available in any case.

So-called dot locking is a mechanism which uses an auxilary lock file as a
locking semaphore. Implemented properly it is reliable over NFS. Its primary
weakness is that there is no race-free way to resolve a stale-lock situation.
However, because stale locks are uncommon and there is a race-resistent way to
clear stale locks, this is not too bad.

Example usage:

    from dotlock import DotLock

    lock = DotLock(path)
    lock.acquire()
    # do stuff with path
    lock.release()
  
Example usage:

    def check_func(lock):
        return lock_is_valid(lock.path)

    lock = DotLock(path, check_func=check_func)
    if lock.acquire(max_attempts=5):
        try:
            # do stuff
        finally:
            lock.release()
'''

__all__ = ["DotLock"]

import os
import time
import string
import socket
from stat import ST_NLINK, ST_INO, ST_MTIME

try:
    import thread
except ImportError:
    thread = None
    
try:
    True, False
except NameError:
    True, False = 1, 0
    

class DotLockError(Exception): pass

class DotLock:
    
    # See the paragraph on O_EXCL in the Linux open(2) man page for
    # background on the algorithm in use here. (In theory, O_EXCL works with
    # NFSv3, but this will work w/NFSv2.)
    
    valid_lock_age = 60 # N.B. should be at least 2 * hijack_delay
    poll_interval  = 15
    hijack_delay   = 15
    
    def __init__(self, path, check_func=None):
        '''Instantiate a DotLock
        
        The <path> argument is the file to be "locked" (the lock file itself is
        named "<path>.lock"). The optional argument "check_func" is called
        during acquire() verify whether a lock that appears stale is still
        valid. It is passed the DotLock instance as an argument. It should
        return True if the lock is still valid, otherwise False.
        '''
        self.path = path
        self.lockpath = path + ".lock"
        self.check_func = check_func
        self._skew = 0    # clock skew
        self._lock = None # (st_ino, lockdata)
    
    def acquire(self, max_attempts=None):
        '''Acquire lock. Return True if successful, otherwise False.
        
        acquire() will poll forever until it can acquire the lock. For a highly
        contested lock it is possible for acquire() to starve indefinitely. In
        such situations, make use of the optional <max_attemmpts> argument and
        check the return value of acquire().
        '''
        # If we wanted to prevent starvation of highly contested locks,
        # there's a couple options: 1) implement a queue, possibly as a
        # directory where waiting processes deposit a file containing their
        # name. the process which holds the lock could check this directory
        # and then pass the lock off to the next in-line (e.g, the waiting
        # process writes its locktemp into the directory. the process with
        # the lock releases by copying the contents of that file into the
        # extant lock and then deleting the locktemp from the directory. a
        # polling process would then need to check whether it now has the
        # lock. 2) The DotLock instance could keep track of how frequently it
        # is acquiring the lock. If the acquisitation rate exceeds a
        # threshold, back-off to give other processes a chance to acquire the
        # lock.
        if self.is_locked():
            debug("* i already have the lock!")
            return
        i = 0
        while True:
            if self._trylock():
                debug("> lock acquired")
                return True
            if self.is_stale():
                if self._hijacklock():
                    debug("> lock acquired (by hijacking)")
                    return True
            if max_attempts:
                i = i + 1
                if i >= max_attempts:
                    return False
            time.sleep(self.poll_interval)
    
    def release(self):
        '''Release the lock.
        
        It safe to call this even if the lock has not been acquired, in which
        case it is a no-op.
        '''
        if self._lock is None:
            debug("* i don't have the lock to release!")
            return
        if not self.is_locked():
            return
        unlink(self.lockpath)
        self._lock = None
        debug("< lock released")
    
    def refresh(self):
        '''Refresh the lock.
        
        If the lock will be held for longer than DotLock.valid_lock_age,
        refresh() should be called periodically to keep the lock from becoming
        stale. DotLockError will be raised if the lock has been hijacked due to
        not calling refresh() in time.
        '''
        # Update times on lock file so that other clients know it is valid.
        if not self.is_locked():
            raise DotLockError("Not locked")
        time_t = time.time() - self._skew
        utime(self.lockpath, (time_t, time_t))
    
    def is_locked(self):
        '''Return True if the lock is (still) acquired, False otherwise.'''
        if self._lock is None:
            return False
        # check if it was hijacked
        st_ino, lockdata = self._lock
        st = stat(self.lockpath)
        if st and st[ST_INO] == st_ino and readfile(self.lockpath) == lockdata:
            return True
        debug("* lock was hijacked")
        self._lock = None
        return False
    
    def is_stale(self):
        '''Return True if the lock appears stale, False otherwise.
        
        is_stale() will first check the lock's age. If it is less than
        DotLock.valid_lock_age, the lock is not stale. Otherwise call
        check_lock() to see if the lock is valid but for some reason the lock
        holder is not refreshing it.
        '''
        st = stat(self.lockpath)
        if not st:
            return False # we'll try again...
        age = time.time() - st[ST_MTIME] - self._skew
        debug("* lock age %d (skew %d)" % (age, self._skew))
        if age < max(2 * self.hijack_delay, self.valid_lock_age):
            return False
        if self.check_lock():
            return False
        return True
    
    def check_lock(self):
        '''Return whether check_func indicates lock is valid (not stale).
        
        check_lock() is called when a lock is stale (exceeds
        DotLock.valid_lock_age). Normally it should not be called as a proper
        lock holder should call refresh() periodically.
        '''
        return self.check_func and self.check_func(self)
    
    def _trylock(self):
        '''Try dot-locking protocol. Update clock skew.'''
        hostname = socket.gethostname()
        pid = os.getpid()
        tid = thread and thread.get_ident() or 0
        lockdata = "%s %s %s %s" % (hostname, pid, tid, time.time())
        locktemp = "%stmp-%s-%s-%s" % (self.lockpath, hostname, pid, tid)
        writefile(locktemp, lockdata)
        try:
            link(locktemp, self.lockpath)
            st = stat(locktemp)
            if not st:
                return
            if st[ST_NLINK] == 2:
                self._lock = (st[ST_INO], lockdata)
            skew = time.time() - st[ST_MTIME]
            if abs(skew) > 1:
                self._skew = skew
        finally:
            unlink(locktemp)
        return self._lock is not None
    
    def _hijacklock(self):
        '''Try hijacking a lock.'''
        # hijacking a lock is perilous. nothing prevents multiple processes
        # from doing it at once. to ameliorate the situation, we sleep awhile
        # after stealing the lock then check to see if any other clients have
        # stolen it. to hijack the lock, we just overwrite the existing lock.
        # this is safer than unlinking which has a race condition where two
        # processes can simultaneously unlink() what they think is a stale
        # lock, while a third process acquires the lock file in-between the
        # two unlinks()
        debug("* attempting to hijack the lock")
        hostname = socket.gethostname()
        pid = os.getpid()
        tid = thread and thread.get_ident() or 0
        lockdata = "%s %s %s %s" % (hostname, pid, tid, time.time())
        writefile(self.lockpath, lockdata)
        time.sleep(self.hijack_delay)
        if readfile(self.lockpath) != lockdata:
            return False
        st = stat(self.lockpath)
        if not st:
            return False
        self._lock = (st[ST_INO], lockdata)
        return True
    

    def __del__(self):
        if self._lock is not None:
            self.release()

def writefile(path, data):
    f = open(path, 'w')
    try:
        if data:
            f.write(data)
    finally:
        f.close()
        
def readfile(path):
    try:
        f = open(path)
        try:
            return f.read()
        finally:
            f.close()
    except (OSError, IOError):
        pass

def utime(path, t):
    try:
        os.utime(path, t)
    except (OSError, IOError):
        pass

def link(src, dst):
    try:
        os.link(src, dst)
    except (OSError, IOError):
        pass

def unlink(path):
    try:
        os.unlink(path)
    except (OSError, IOError):
        pass

def stat(path):
    try:
        return os.stat(path)
    except (OSError, IOError):
        return None

def debug(s):
    pass

def debug_test(s):
    import sys
    sys.stderr.write("%s [%s]: %s\n" % (
        time.asctime(time.gmtime(time.time())), os.getpid(), s))

def test(path, run_time=300, num_procs=1):
    import random
    
    global debug
    debug = debug_test
    
    run_time  = int(run_time)
    num_procs = int(num_procs)
    
    if num_procs > 1:
        # fork a bunch of children, then release them all at once
        read_end, write_end = os.pipe()
        for i in xrange(min(num_procs, 50)):
            start_delay = random.random()
            pid = os.fork()
            if not pid:
                break
        if pid:
            time.sleep(2)
            debug("release the hounds")
            os.close(read_end)
            os.close(write_end)
            return
        else:
            debug("waiting")
            os.close(write_end)
            os.fdopen(read_end).read() # block till parent is ready
            time.sleep(start_delay) # give them all a different start time
    
    end_time = time.time() + run_time
    lock = DotLock(path)
    # speed up testing a bit by turning down some of times
    lock.hijack_delay = 8
    lock.valid_lock_age = 16
    
    id_ = "%s-%s" % (socket.gethostname(), os.getpid())
    while time.time() < end_time:
        if not lock.acquire(1):
            time.sleep(1)
            continue
        if random.random() < .05:
            # simulate a hung process which doesn't release the lock
            debug("* hanging the lock")
            time.sleep(lock.valid_lock_age * 1.5)
            continue
        if os.path.exists(path):
            f = open(path)
            n = int(string.split(f.readlines()[-1])[0]) + 1
            f.close()
        else:
            n = 1
        # provide time for any race condition to show itself
        time.sleep(1) 
        open(path, 'a').write("%s %s\n" % (n, id_))
        debug("* wrote %s" % n)
        lock.release()
        # give someone else time to acquire the lock
        time.sleep(random.randint(1,5))
    del lock
    debug("* exiting")

if __name__ == '__main__':
    # dot_lock.py <path> <run_time> <num_procs>
    import sys
    apply(test, sys.argv[1:])

