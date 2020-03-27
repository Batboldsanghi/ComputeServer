import requests, json, time
from pprint import pprint
from config import INFOSERVER_CONFIG

class InfoServer:
    STATUS_PENDING=0
    STATUS_PROCESSING=1
    STATUS_FINISHED=2
    STATUS_ERROR=10

    MODE_REALTIME = 0
    MODE_SCHEDULE = 1

    C1_STATUS_PENDING=0
    C1_STATUS_PROCESSING=1
    C1_STATUS_FINISHED=2
    C1_STATUS_ERROR=10

    C2_STATUS_PENDING=0
    C2_STATUS_PROCESSING=1
    C2_STATUS_FINISHED=2
    C2_STATUS_ERROR=10

    headers = {"Content-Type": "application/json"}

    def __init__(self):
        self.getToken()

    def getToken(self):        
        r = self.sendData(INFOSERVER_CONFIG['TOKEN_URL'],{"email":INFOSERVER_CONFIG['email'],"password":INFOSERVER_CONFIG['password']},headerType="token")        
        if(r.get('token')!=None):
                self.headers['Authorization'] ='Bearer '+r.get('token')
        

    def sendData(self,url, data=None, type="POST", headerType = "data"):
        try:
            if(headerType=="data"):
                headers = self.headers
            else:
                headers = {"Content-Type": "application/json"}
            
            if(type=="POST"):                
                response = requests.post(INFOSERVER_CONFIG['BASE_URL']+url, json=data, headers=headers)                
            else:
                response = requests.get(INFOSERVER_CONFIG['BASE_URL']+url, params=data, headers=headers)
            rjson = response.json()
            #print(rjson)
            if(rjson['result']=="error" and 'code' in rjson and rjson['code']==9002):                
                self.getToken()
                return self.sendData(url, data ,type=type, headerType = headerType)
            else:
                return rjson
        except Exception as e:            
            return {"result": "error", "msg": e.args}

    
    def sendFile(self, url, data, files):
        try:
            headers = {
                'Authorization': self.headers['Authorization']
            }
            
            response = requests.post(INFOSERVER_CONFIG['BASE_URL']+'/info/update', files=files,data=data,headers = headers)
            rjson = response.json()
            
            if(rjson['result']=="error" and 'code' in rjson and rjson['code']==9002):                
                self.getToken()
                return self.sendFile(url, data, files)
            else:
                return rjson
        except Exception as e:
            return {"result": "error", "msg": e.args}


    def getInfo(self,mode=None):
        data = {
            "withprocess":1,
            'mode':self.MODE_REALTIME if mode is None else mode
        }        
        return self.sendData('/info/get',data=data,type='POST')

    def setDetail(self,data):        
        return self.sendData('/info/setDetail',data=data,type='POST')

    def updateInfo(self,id,filename):
        data = {
            'id':id,
            'filename':filename
        }
        
        f = open('infofiles/'+filename,'rb')
        files = {
            'file1': (filename,f,'application/pdf')
        }

        return self.sendFile('/info/update',data,files)