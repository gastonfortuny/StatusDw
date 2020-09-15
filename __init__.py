import logging
import requests
import json
import pyodbc
import os
import time
import azure.functions as func

#Seteo Credenciales
suscriptionId= os.environ["suscriptionId"]
resourceGroup= os.environ["resourceGroup"]
serverName= os.environ["serverName"]
databaseName= os.environ["databaseName"]
client_id= os.environ["client_id"]
client_secret= os.environ["client_secret"]
tenant_id= os.environ["tenant_id"]
host= os.environ["host"]
username= os.environ["DBuser"]
password= os.environ["password"]
driver= os.environ["driver"]



#Obtengo varialbes de la llamada
def main(req: func.HttpRequest) -> func.HttpResponse:
    action = req.params.get('action')
    if not action:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            action = req_body.get('action')
    
    #obtengo estado
    status=getStatus()

#Segun el valor que obtube (Status, Resume o pause)
    #si solicita status, le devuelvo el estado
    if action=='status':         
        return func.HttpResponse(f"El estado del servidor es: {status}. ")

    #Si solicita resume, verifico el estado y luego ejecuto el resume    
    elif action=='resume':
        if status=='Paused':           
            
            status=resume()
            return func.HttpResponse(f"El estado del servidor es: {status}. ")

        else:
            return  func.HttpResponse(f"No se pudo reanudar el servidor debido a que su estado actual es: {status}. ") 
    #Si solicta pause, verifico el estado, verifico si hay cursores activos y luego ejecuto el pause
    elif action=='pause':  

        if status=='Online':  
            #verifico cursores activos    
            conn=getConnection()
            cursor = conn.cursor()
            query="SELECT count(*) FROM sys.dm_pdw_exec_requests WHERE status not in ('Completed','Failed','Cancelled') AND session_id <> session_id()"
            cursor.execute(query)
            row = cursor.fetchone()                     
            while row[0]>0:
                time.sleep(5)
                cursor.execute(query)
                row = cursor.fetchone()  

            #Pauso el servidor    
            status=pause()
            return func.HttpResponse(f"El estado del servidor es: {status}. ")

        else:
            return  func.HttpResponse(f"No se pudo pausar el servidor debido a que su estado actual es: {status}. ")     

    #Si la varialbe obtenida no esta contemplada, devuelvo el mensaje         
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )

    
    return status

#llama a la api que ejecuta resume
def resume():
    endpoint='https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Sql/servers/{}/databases/{}/resume?api-version=2014-04-01-preview'.format(suscriptionId,resourceGroup,serverName,databaseName)
    token=getToken()
    token = 'Bearer ' + token 
    headers = {"Authorization": token}
    requests.post(endpoint, headers=headers).json()
    status=getStatus()
    return status

#llama a la api que ejecuta pause
def pause():
    endpoint='https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Sql/servers/{}/databases/{}/pause?api-version=2014-04-01-preview'.format(suscriptionId,resourceGroup,serverName,databaseName)
    token=getToken()
    token = 'Bearer ' + token 
    headers = {"Authorization": token}
    requests.post(endpoint, headers=headers).json()
    status=getStatus()
    return status

#llama a la api que ejecuta status
def getStatus():
    endpoint='https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Sql/servers/{}/databases/{}?api-version=2014-04-01'.format(suscriptionId,resourceGroup,serverName,databaseName)
    token=getToken()
    token = 'Bearer ' + token       
    headers = {"Authorization": token,'Accept': 'application/json'}
    response = requests.get(endpoint,headers=headers).json()       
    return response['properties']['status'] 

#obtiene el tocken necesario para llamar a las apis
def getToken():
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.azure.com/'
    }
    endpoint='https://login.microsoftonline.com/{}/oauth2/token'.format(tenant_id)
    response = requests.post(endpoint, data=payload).json() 
    return response['access_token']

#obtiene conexión
def getConnection():    
    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+host+';DATABASE='+databaseName+'; UID='+username+'; PWD='+password)
    return conn