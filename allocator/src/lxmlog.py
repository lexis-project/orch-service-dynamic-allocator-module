# import libraries and modules
import os
import time
from datetime import datetime


# define a logging class
class LXMlog:
    # define a constructir of the class
    def __init__(self):
        self.logger = None


    # open the logger for the activity of the web app
    def openLog(self, log_path):
        log_fullpath = os.path.join(os.getcwd(), log_path)
        print (" (dbg): opening log file (%s)" % (log_fullpath))
        try:
            self.logger = open(log_fullpath, "w+")
        except IOError:
            print (" (err): unable to open logger")
            return (1)    
        self.logger.write("(%s): logger opened and active\n" % (datetime.now()))
        self.logger.flush()
        return (0)


    # log a message
    def doLog(self, msg):
        if (self.logger != None):
            self.logger.write("(%s): %s\n" % (datetime.now(), str(msg)))
            self.logger.flush()


    # close the logger
    def closeLog(self):
        if (self.logger != None):
            self.logger.write("(%s): web application backend is going to stop -- goodbye\n\n" % (datetime.now()))
            self.logger.flush()
            self.logger.close()


    # force flushing the current buffer
    def forceFlush(self):
        self.logger.flush()
        return (0)
