import json, uuid, random, string, math
from models.base import Base, session
from sqlalchemy import Column, String, Integer, TIMESTAMP, Enum, JSON, DateTime, Float, TEXT
from sqlalchemy import func
from pprint import pprint

class User(Base):
    """User Системийн хэрэглэгч
    """
    __tablename__ = 'users'
    STATUS_PENDING=0
    STATUS_ACTIVE=1
        
    id = Column(Integer, primary_key=True)
    uuid = Column(String(36),unique=True)        
    email = Column(String(100),unique=True)
    password = Column(String(100))
    activation = Column(String(100))
    status = Column(Integer,default=0)
    role = Column(Integer,default=4)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

    def __repr__(self):
        return json.dumps({
            "user_uuid": self.uuid,
            "email":self.email,            
            "role":self.role
        })
    
    @classmethod
    def generateID(cls):
        generateID =uuid.uuid4()

        c = session.query(cls).filter_by(id=generateID).count()
        if(c>0):
            return cls.generateID()
        else:
            return generateID


class Info(Base):
    """Info Монгол Банкин дээрх иргэний  зээлийн мэдээлэл
    """
    __tablename__ = 'infos'
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

    id = Column(Integer, primary_key = True, autoincrement = True)
    user_uuid = Column(String(36))
    params = Column(TEXT,default=None)
    rnumber = Column(String(10),default=None)
    filename = Column(String(30),nullable=True)
    mode = Column(Integer,default=0)
    detail = Column(TEXT,default = None)
    detail2 = Column(TEXT,default = None)
    status = Column(Integer,default = 0)
    created_at = Column(TIMESTAMP)
    c1status = Column(Integer,default = 0)
    c1data = Column(TEXT,default = None)
    c2status = Column(Integer,default = 0)
    c2data = Column(TEXT,default = None)
    
    @classmethod
    def generateFilename(cls):
        randomName = ''.join(random.choices(string.ascii_letters + string.digits,k=15))
        filename = 'MB'+randomName+'.pdf'
        c = session.query(cls).filter_by(filename=filename).count()
        if(c>0):
            return cls.generateFilename()
        else:
            return filename
        
    def to_tuple(self):
        return (self.id,
                self.rnumber,
                #json.dumps(self.detail,ensure_ascii=False),
                self.detail,
                self.detail2,
                self.status,
                self.created_at)

    def to_dict(self):
        return {
            'id':self.id,
            'rnumber':self.rnumber,
            'filename': self.filename,
            'params': self.params,
            'detail':self.detail,
            'detail2':self.detail2,
            'status':self.status,
            'created_at':self.created_at.strftime("%Y-%m-%d, %H:%M:%S"),
            'c1status':self.c1status,
            'c1data':self.c1data,
            'c2status':self.c2status,
            'c2data':self.c2data
        }

    @classmethod
    def columns(cls):
        return [
            'id','rnumber','detail','detail2','status','created_at'
        ]

    @classmethod
    def get_pendingC1(cls,mode):
        row = session.query(cls)\
            .filter_by(mode = mode, c1status = cls.C1_STATUS_PENDING)\
            .filter(cls.detail.isnot(None))\
            .filter(cls.status==cls.STATUS_FINISHED)\
            .order_by(cls.created_at.desc())\
            .first()
        if(row!=None):
            row.c1status = cls.C1_STATUS_PROCESSING
            session.flush()
            return row
        else:
            return None


class Ddrows(Base):
    __tablename__ = 'ddrows'

    id = Column(Integer, primary_key = True, autoincrement = True)
    filename = Column(String(30))
    downloaded_date = Column(DateTime)
    dd = Column(Integer)
    created_at = Column(TIMESTAMP)
    registriindugaar = Column(String(12))
    zeeliinkhemzhee = Column(Float)
    zeelolgosonognoo = 	Column(DateTime)
    tologdokhognoo = Column(DateTime)
    valiutynner	= Column(String(3))
    oriinuldegdel = Column(Float,default=0)
    kheviin	= Column(Integer,default=0)
    khugatsaakhetersen=Column(Integer,default=0)
    kheviinbus=Column(Integer,default=0)
    ergelzeetei=Column(Integer,default=0)
    muu=Column(Integer,default=0)
    buleg = Column(String(255),default=None)
    dedbuleg = Column(String(255),default=None)
    ukhekhbg_yndugaar= Column(String(255),default=None)
    ulsynburtgeliindugaar= Column(String(255),default=None)
    kartyndugaar = Column(String(30),default=None)
    tailbar	= Column(String(255),default=None)
    pred_tag = Column(Integer,default=None)
    payment_pred = Column(Float, default=None)
    payment_pred_last= Column(Float, default=None)
    rate_mean= Column(Float, default=None)
    rate_pred= Column(Float, default=None)
    month0= Column(Integer, default=None)
    heviinbus_uldegdeltei= Column(Integer, default=None)
    heviinbus_uldegdelgui= Column(Integer, default=None)
    heviinbus_shugam= Column(Integer, default=None)
    created_at = Column(TIMESTAMP)

    loantypecode = Column(String(30))
    orgcode = Column(String(100))
    statuscode = Column(String(30))
    loanclasscode = Column(String(30))

    new_loan = Column(Integer,default=None)

    @classmethod
    def update1(cls, row):
        ddrow = session.query(cls)\
                .filter_by(id=row['id'])\
                .first()
        if(ddrow is None):
            print("-insert-")
        else:
            session.query(cls)\
                .filter_by(id=row['id'])\
                .update({
                    'kheviin':15,
                    'oriinuldegdel':11
                })

    @classmethod
    def insert(cls, row):                
        ddrow = session.query(cls)\
            .filter_by(dd=row['d/d'],\
                registriindugaar=row['registriindugaar'],\
                zeeliinkhemzhee = row['zeeliinkhemzhee'],\
                zeelolgosonognoo = row['zeelolgosonognoo']\
            ).first()
        
        if(ddrow is None):                     
            new_row = Ddrows(
                downloaded_date=row['downloaded_date'],
                filename = row['filename'],                
                dd = row['d/d'],                
                registriindugaar = row['registriindugaar'],
                zeeliinkhemzhee = row['zeeliinkhemzhee'],
                zeelolgosonognoo = 	row['zeelolgosonognoo'],
                tologdokhognoo=row['tologdokhognoo'],
                valiutynner=row['valiutynner'],
                oriinuldegdel=row['oriinuldegdel'],
                kheviin=row['kheviin'],
                khugatsaakhetersen=row['khugatsaakhetersen'],
                kheviinbus=row['kheviinbus'],
                ergelzeetei=row['ergelzeetei'],
                muu=row['muu'],
                buleg=row['buleg'],
                dedbuleg=row['dedbuleg'],
                ukhekhbg_yndugaar=row['ukhekhbg-yndugaar'],
                ulsynburtgeliindugaar=row['ulsynburtgeliindugaar'],
                kartyndugaar=row['kartyndugaar'],
                tailbar=row['tailbar'],
                pred_tag=row['pred_tag'],
                payment_pred=row['payment_pred'],
                payment_pred_last=row['payment_pred_last'],
                rate_mean=row['rate_mean'],
                rate_pred=row['rate_pred'],
                month0=row['month0'],
                heviinbus_uldegdeltei=row['heviinbus_uldegdeltei'],
                heviinbus_uldegdelgui=row['heviinbus_uldegdelgui'],
                heviinbus_shugam=row['heviinbus_shugam'],

                loantypecode = row['loantypecode'],
                orgcode = row['orgcode'],
                statuscode = row['statuscode'],
                loanclasscode = row['loanclasscode'],
                new_loan = row['new_loan']
            )
            
            session.add(new_row)
            session.flush()
        elif(ddrow.oriinuldegdel!=0 or ddrow.oriinuldegdel!=row['oriinuldegdel']):
            
            session.query(cls)\
                .filter_by(id=ddrow.id)\
                .update({
                    'downloaded_date':row['downloaded_date'],
                    'tologdokhognoo':row['tologdokhognoo'],
                    'valiutynner':row['valiutynner'],
                    'oriinuldegdel':row['oriinuldegdel'],
                    'kheviin':row['kheviin'],
                    'khugatsaakhetersen':row['khugatsaakhetersen'],
                    'kheviinbus':row['kheviinbus'],
                    'ergelzeetei':row['ergelzeetei'],
                    'muu':row['muu'],
                    'buleg':row['buleg'],
                    'dedbuleg':row['dedbuleg'],
                    'ukhekhbg_yndugaar':row['ukhekhbg-yndugaar'],
                    'ulsynburtgeliindugaar':row['ulsynburtgeliindugaar'],
                    'kartyndugaar':row['kartyndugaar'],
                    'tailbar':row['tailbar'],
                    'pred_tag':row['pred_tag'],
                    'payment_pred':row['payment_pred'],
                    'payment_pred_last':row['payment_pred_last'],
                    'rate_mean':row['rate_mean'],
                    'rate_pred':row['rate_pred'],
                    'month0':row['month0'],
                    'heviinbus_uldegdeltei':row['heviinbus_uldegdeltei'],
                    'heviinbus_uldegdelgui':row['heviinbus_uldegdelgui'],
                    'heviinbus_shugam':row['heviinbus_shugam'],

                    'loantypecode':row['loantypecode'],
                    'orgcode':row['orgcode'],
                    'statuscode':row['statuscode'],
                    'loanclasscode':row['loanclasscode'],
                    'new_loan':row['new_loan']
                })
            #session.flush()

    