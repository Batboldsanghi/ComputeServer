import threading
import time, traceback
from models.base import session, engine
from models.Models import User, Info
from libs.computes import compute1


class Compute1(threading.Thread):
    """Real time computing"""
    def __init__(self):
        super(Compute1, self).__init__()        

    def run(self):
        print("compute 1 started")
        while (True):            
            
            i = Info.get_pendingC1(Info.MODE_REALTIME)
            
            if(i):
                try: 
                    compute1(i.id,i.rnumber)
                    i.c1status = Info.C1_STATUS_FINISHED
                except Exception as e:                    
                    i.c1status = Info.C1_STATUS_ERROR
                    traceback.print_exc()
                session.flush()
                print("computed")
            else:
                time.sleep(5)


class Compute2(threading.Thread):
    """SCHEDULE time computing"""
    def __init__(self):
        super(Compute2, self).__init__()
        
    def run(self):
        print("compute 2 started")
        while (True):
            i = Info.get_pendingC1(Info.MODE_SCHEDULE)
            if(i):                
                try:     
                    compute1(i.id,i.rnumber)
                    i.c1status = Info.C1_STATUS_FINISHED
                except Exception as e:                    
                    i.c1status = Info.C1_STATUS_ERROR
                    traceback.print_exc()
                session.flush()
            else:
                time.sleep(5)

#c1 = Compute1()
#c1.start()

c2 = Compute2()
c2.start()
