#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import datetime
import tkinter
from tkinter import ttk



# In[2]:

#capture database log in credentials
def saveInput():
    global dbuser, dbpassword, InputDate1, InputDate2, dbname
    dbuser = DbUserEntry.get()
    dbpassword = UserpasswordEntry.get()
    InputDate1 = FromDateEntry.get()
    InputDate2 = ToDateEntry.get()
    dbname = BankNameCombo.get()

def closeApp():
    window.destroy()
    

    

#Main Window
window = tkinter.Tk()
window.geometry("700x350")
window.title('Account Reconciliation')
frame = tkinter.Frame(window)
frame.pack()

UserInfoFrame = tkinter.LabelFrame(frame,text="User Log In",bg='orange')
UserInfoFrame.grid(row=0,column=0,sticky="news")

DbUserlab = tkinter.Label(UserInfoFrame,text="Enter user name")
DbUserlab.grid(row=0,column=0)
DbUserEntry = tkinter.Entry(UserInfoFrame)
DbUserEntry.grid(row=1,column=0)

UserpasswordLabel = tkinter.Label(UserInfoFrame,text="Enter password")
UserpasswordLabel.grid(row=0,column=1)
UserpasswordEntry = tkinter.Entry(UserInfoFrame,show="*")
UserpasswordEntry.grid(row=1,column=1)

for widget in UserInfoFrame.winfo_children():
    widget.grid_configure(padx=10,pady=5)



PeriodFrame = tkinter.LabelFrame(frame,text="Reconciliation Details",bg='orange')
PeriodFrame.grid(row=1,column=0,sticky="news")

FromDateLabel = tkinter.Label(PeriodFrame,text="Reconciliation Start Date")
FromDateLabel.grid(row=0,column=0)
FromDateEntry = tkinter.Entry(PeriodFrame)
FromDateEntry.insert(0,"yyyy-mm-dd")
FromDateEntry.grid(row=1,column=0)

ToDateLabel = tkinter.Label(PeriodFrame,text="Reconciliation End Date")
ToDateLabel.grid(row=0,column=1)
ToDateEntry = tkinter.Entry(PeriodFrame)
ToDateEntry.insert(0,"yyyy-mm-dd")
ToDateEntry.grid(row=1,column=1)

BankNameLabel = tkinter.Label(PeriodFrame,text="Bank Name")
BankNameLabel.grid(row=2,column=0)
BankNameCombo = ttk.Combobox(PeriodFrame,values=['All','KYCliteBankAccs','FullKYCliteBankAccs'])
BankNameCombo.grid(row=3,column=0)

AccountNameLabel = tkinter.Label(PeriodFrame,text="Account Name")
AccountNameLabel.grid(row=2,column=1)
AccNameCombo = ttk.Combobox(PeriodFrame,values=['All'])
AccNameCombo.grid(row=3,column=1)


for widget in PeriodFrame.winfo_children():
    widget.grid_configure(padx=10,pady=5)

SaveFrame = tkinter.LabelFrame(frame,text="Verify and save",bg='orange')
SaveFrame.grid(row=2,column=0,sticky="news")
Savebutton = tkinter.Button(SaveFrame,text='Save',command=saveInput)
Savebutton.grid(row=0,column=0)

Closebutton = tkinter.Button(SaveFrame,text='Close',command=closeApp)
Closebutton.grid(row=0,column=1,pady=20,padx=30)



# In[3]:


# Get the current datetime
dt = datetime.datetime.now()



# In[4]:


#log in to the server khu:khumbu
dbuser = dbuser
dbpassword = dbpassword


# In[ ]:


engine = create_engine('mssql+pyodbc://'
                      +str(dbuser)+':'+str(dbpassword)+'SERVERNAME\SQLEXPRESS/KYCliteBankAccs?'
                      'driver=ODBC Driver 17 for SQL Server'
                      )


# In[ ]:


#Define server conn parameters
server = 'BARD-KHUMBULANI\SQLEXPRESS'
database = 'KYCliteBankAccs'
driver = 'SQL+Server+Native+Client+11.0'
username = str(dbuser)
password = str(dbpassword)


# In[ ]:


#2024-04-16,2024-04-16


InputDate1 = InputDate1
InputDate2 = InputDate2


# In[ ]:


#Define period to be covered
fromDate = str(InputDate1)
toDate =  str(InputDate2)


# In[ ]:


#
reconPeriod = "BETWEEN "+"'"+str(fromDate)+"'" +"AND "+"'"+str(toDate)+"'"



if dt.hour < 12:
    print('Good Morning '+dbuser+'. CURRENTLY PERFORMING RECONCILIATION FOR BankAcc WALLETS FOR PERIOD BETWEEN '+reconPeriod+'PLEASE WAIT ....')
elif dt.hour < 17 and dt.hour > 12:
    print('Good afternoon '+dbuser+'. CURRENTLY PERFORMING RECONCILIATION FOR BankAcc WALLETS FOR PERIOD BETWEEN '+reconPeriod+'PLEASE WAIT ....')
else: print('Good evening '+dbuser+'. CURRENTLY PERFORMING RECONCILIATION FOR BankAcc WALLETS FOR PERIOD BETWEEN '+reconPeriod+'PLEASE WAIT ....')


# In[ ]:


path_stmt = r'C:\Users\HP\Documents\BankAcc\StatementUpload\statement'
path_ledger = r'C:\Users\HP\Documents\BankAcc\StatementUpload\ledger'


# In[ ]:


#read and clean statement 
def stmtupload(accname):
    statement = pd.read_csv(str(path_stmt)+'\\'+str(accname),index_col=0)
    statement.rename(columns={"Auth No":"Auth_No"},inplace = True)
    statement.to_sql(con=engine,name='BankZee_stmt',if_exists='replace')
    


# In[ ]:


#upload ledger to sqlserver
def lgrupload(lgrname):
    ledger = pd.read_csv(str(path_ledger)+'\\'+str(lgrname))
    ledger.rename(columns={"Register ID":"Register_ID"},inplace = True)
    ledger.to_sql(con=engine,name='BankAcc_lgr',if_exists='replace')


# In[ ]:


#establish conn to the SQLEXP Server
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
                      SERVER='+server+';\
                      DATABASE='+database+';\
                      UID='+username+';\
                      PWD='+ password)

cursor = cnxn.cursor()


# In[ ]:





# In[ ]:


stmtupload('VictoriaBank1_stmt.csv')
lgrupload('VictoriaBank1_lgr.csv')


# In[ ]:


#VICTORIAL BANK 1
query1 =  '''

DELETE FROM Bank_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
Bank_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_VicBank1;
DROP TABLE IF EXISTS outstandingStmtDR_VicBank1;
DROP TABLE IF EXISTS outstandingStmtCR_VicBank1;
DROP TABLE IF EXISTS outstandingLgrCR_VicBank1;
DROP TABLE IF EXISTS outstandingStmtCR_VicBank1;
DROP TABLE IF EXISTS outstandingLgrDR_VicBank1;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_VicBank1 from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_VicBank1
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;




----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_VicBank1
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_VicBank1
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_VicBank1
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#VICTORIA FALLS 2
#upload stmt to sqlserver

stmtupload('VictoriaBank2_stmt.csv')
lgrupload('VictoriaBank2_lgr.csv')


# In[ ]:





# In[ ]:


#VICTORIA FALLS 2

query1 =  '''

DELETE FROM BankZee_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
BankZee_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_VicBank2;
DROP TABLE IF EXISTS outstandingStmtDR_VicBank2;
DROP TABLE IF EXISTS outstandingStmtCR_VicBank2;
DROP TABLE IF EXISTS outstandingLgrCR_VicBank2;
DROP TABLE IF EXISTS outstandingStmtCR_VicBank2;
DROP TABLE IF EXISTS outstandingLgrDR_VicBank2;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_VicBank2 from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_VicBank2
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_VicBank2
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_VicBank2
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_VicBank2
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#BY_Bank
#upload stmt to sqlserver

stmtupload('BY_Bank_stmt.csv')
lgrupload('BY_Bank_lgr.csv')


# In[ ]:


#BANK ZEE
query1 =  '''

DELETE FROM Bank_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
BankZee_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_BY_Bank;
DROP TABLE IF EXISTS outstandingStmtDR_BY_Bank;
DROP TABLE IF EXISTS outstandingStmtCR_BY_Bank;
DROP TABLE IF EXISTS outstandingLgrCR_BY_Bank;
DROP TABLE IF EXISTS outstandingStmtCR_BY_Bank;
DROP TABLE IF EXISTS outstandingLgrDR_BY_Bank;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_BY_Bank from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_BY_Bank
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_BY_Bank
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_BY_Bank
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_BY_Bank
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#WanBank

#upload stmt to sqlserver

stmtupload('WanBank_stmt.csv')
lgrupload('WanBank_lgr.csv')


# In[ ]:


#WanBank

#BY_Bank
query1 =  '''

DELETE FROM Bank_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
Bank_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_WanBank;
DROP TABLE IF EXISTS outstandingStmtDR_WanBank;
DROP TABLE IF EXISTS outstandingStmtCR_WanBank;
DROP TABLE IF EXISTS outstandingLgrCR_WanBank;
DROP TABLE IF EXISTS outstandingStmtCR_WanBank;
DROP TABLE IF EXISTS outstandingLgrDR_WanBank;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_WanBank from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_WanBank
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_WanBank
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_WanBank
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_WanBank
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#BankZee

#upload stmt to sqlserver

stmtupload('BankZee_stmt.csv')
lgrupload('BankZee_lgr.csv')


# In[ ]:


#BankZee

query1 =  '''

DELETE FROM BankZee_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
BankZee_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_BankZee;
DROP TABLE IF EXISTS outstandingStmtDR_BankZee;
DROP TABLE IF EXISTS outstandingStmtCR_BankZee;
DROP TABLE IF EXISTS outstandingLgrCR_BankZee;
DROP TABLE IF EXISTS outstandingStmtCR_BankZee;
DROP TABLE IF EXISTS outstandingLgrDR_BankZee;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_BankZee from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_BankZee
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_BankZee
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_BankZee
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_BankZee
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#PLUMBANK

#upload stmt to sqlserver

stmtupload('PlumBank_stmt.csv')
lgrupload('PlumBank_lgr.csv')


# In[ ]:


#PLUMBANK
query1 =  '''

DELETE FROM Bank_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
Bank_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_PlumBank;
DROP TABLE IF EXISTS outstandingStmtDR_PlumBank;
DROP TABLE IF EXISTS outstandingStmtCR_PlumBank;
DROP TABLE IF EXISTS outstandingLgrCR_PlumBank;
DROP TABLE IF EXISTS outstandingStmtCR_PlumBank;
DROP TABLE IF EXISTS outstandingLgrDR_PlumBank;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_PlumBank from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_PlumBank
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_PlumBank
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_PlumBank
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_PlumBank
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#FrazerBank

#upload stmt to sqlserver

stmtupload('FrazerBank_stmt.csv')
lgrupload('FrazerBank_lgr.csv')


# In[ ]:


#FrazerBank

query1 =  '''

DELETE FROM BankZee_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
BankZee_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_FrazerBank;
DROP TABLE IF EXISTS outstandingStmtDR_FrazerBank;
DROP TABLE IF EXISTS outstandingStmtCR_FrazerBank;
DROP TABLE IF EXISTS outstandingLgrCR_FrazerBank;
DROP TABLE IF EXISTS outstandingStmtCR_FrazerBank;
DROP TABLE IF EXISTS outstandingLgrDR_FrazerBank;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_FrazerBank from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_FrazerBank
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_FrazerBank
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_FrazerBank
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_FrazerBank
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


#BankChols

#upload stmt to sqlserver
stmtupload('BankChols_stmt.csv')
lgrupload('BankChols_lgr.csv')


# In[ ]:


#BankChols

query1 =  '''

DELETE FROM Bank_stmt WHERE Auth_No IS NULL;


---------------- CREATE TABLE FOR statement debits---------------------

DROP TABLE IF EXISTS stmtDr1; -- DELETE TABLE TO LOAD NEW DATA
DROP TABLE IF EXISTS stmtDr;  -- DELETE TABLE TO LOAD NEW DATA

DROP TABLE IF EXISTS clean_Statement1;

SELECT *
INTO clean_Statement1
FROM
Bank_stmt 
WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'

query2= '''EXEC CleanedStmt;'''

query3 =  '''DELETE FROM BankAcc_lgr WHERE Register_ID IS NULL; 
DROP TABLE IF EXISTS lgrDR; 
DROP TABLE IF EXISTS lgrDR1;
DROP TABLE IF EXISTS clnledger1;'''

query4 = '''SELECT * INTO clnledger1 FROM BankAcc_lgr WHERE CAST(Date AS date) '''+' '+str(reconPeriod)+' ;'



query5 = '''EXEC CleanedLedger;


---------------MATCH RECEIPTS------------------------------

----------MATCHED ITEMS----------------------------------------
DROP TABLE IF EXISTS Matcheditems_BankChols;
DROP TABLE IF EXISTS outstandingStmtDR_BankChols;
DROP TABLE IF EXISTS outstandingStmtCR_BankChols;
DROP TABLE IF EXISTS outstandingLgrCR_BankChols;
DROP TABLE IF EXISTS outstandingStmtCR_BankChols;
DROP TABLE IF EXISTS outstandingLgrDR_BankChols;

WITH Matched1 AS ((SELECT *

FROM lgrCr A INNER JOIN stmtDr B
ON A.createdref = B.ref_created)

UNION ALL 

(SELECT *

FROM lgrDR A INNER JOIN stmtCr B
ON A.createdref = B.ref_created))

SELECT * INTO Matcheditems_BankChols from Matched1;

----------OUTSTANDING STMTDR----------------------------------------
SELECT *
INTO outstandingStmtDR_BankChols
FROM  stmtDr A LEFT JOIN lgrCr B
ON A.ref_created = B.createdref
WHERE B.createdref IS NULL;



----------OUTSTANDING LEDGER CR----------------------------------------
SELECT *
INTO outstandingLgrCR_BankChols
FROM lgrCr A  LEFT JOIN stmtDr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

---------OUTSTANDING STMT CR----------------------------------
SELECT *
INTO outstandingStmtCR_BankChols
FROM  stmtCR A LEFT JOIN lgrDR B
ON A.ref_created = B.createdref
WHERE B.createdref IS  NULL;

----------OUTSTANDING LEDGER DR----------------------------------------
SELECT *
INTO outstandingLgrDR_BankChols
FROM lgrDR A  LEFT JOIN stmtCr B
ON A.createdref = B.ref_created
WHERE B.ref_created IS NULL;

'''

cursor.execute(query1)
cursor.execute(query2)
cursor.execute(query3)
cursor.execute(query4)
cursor.execute(query5)
cnxn.commit()


# In[ ]:


cursor.close()

cnxn.close()


# In[ ]:
window.mainloop()




# In[ ]:





# In[ ]:




