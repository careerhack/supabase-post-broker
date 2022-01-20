import os
import re
from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# get config file
CONFIGFILE = '.secrets/config.json'

# get configuration file
import json
CONFIG = json.load(open(CONFIGFILE,'r'))
AUTHORIZATION_TOKEN = CONFIG['token']

########################################################################
#
# CLASS: BaseModel
# post data schema
#
########################################################################
from pydantic import BaseModel
class PostData(BaseModel):
    url: Optional[str] = None
    post_uid: Optional[str] = None
    post_ts: Optional[str] = None
    source_name: Optional[str] = None
    source_type: Optional[str] = None

class RowData(BaseModel):
    record: Optional[dict] = None


########################################################################
#
# FUNCTION: extractURLs(text: str)
# extract urls in a text body.
#
########################################################################
def extractURLs(text: str):
    expression = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(expression, text)
    return urls

########################################################################
#
# FUNCTION: sinkData(table_name, data)
# sends data to Supabase. Assumes you are inputting correct schema 
# as a dict object.
#
########################################################################
def sinkData(table_name: str, data: dict):
    try:
        data = supabase.table(table_name).insert(data).execute()
        return data
    except:
        return None

# init fastapi
app = FastAPI()

# create database connection
from supabase import create_client, Client
supabase_url: str = CONFIG['supabase_url']
supabase_token: str = CONFIG['supabase_service_token']
supabase: Client = create_client(supabase_url, supabase_token)


@app.get('/api')
def read_root():
    return {'status':200}

@app.post('/api/v1/fetch/')
async def gitUpdate(request: Request, body: RowData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            os.system('cd /root/supabase-webhook-broker; git pull')
            return JSONResponse({'status':200})

@app.post('/api/v1/function/extractAndInsertURL')
async def webhook_extractAndInsertURL(request: Request, body: RowData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            record_data = body.record
            source_name = record_data['source_name']
            source_type = record_data['source_type']
            post_uid = record_data['uid']
            post_ts = record_data['created_at']
            post = record_data['post']

            urls = extractURLs(post)

            responseData = []
            for url in urls:
                INSERTDATA = {
                    'source_name': source_name,
                    'source_type': source_type,
                    'post_uid': post_uid,
                    'post_ts': post_ts,
                    'url': url,
                }
                responseData.append(sinkData('urls',INSERTDATA))
            return JSONResponse(responseData)

@app.post('/api/v1/function/getURLPreview')
async def webhook_getURLPreview(request: Request, body: RowData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            record_data = body.record
            source_name = record_data['source_name']
            source_type = record_data['source_type']
            post_uid = record_data['uid']
            post_ts = record_data['created_at']
            post = record_data['post']

            urls = extractURLs(post)

            responseData = []
            for url in urls:
                INSERTDATA = {
                    'source_name': source_name,
                    'source_type': source_type,
                    'post_uid': post_uid,
                    'post_ts': post_ts,
                    'url': url,
                }
                responseData.append(sinkData('urls',INSERTDATA))
            return JSONResponse(responseData)