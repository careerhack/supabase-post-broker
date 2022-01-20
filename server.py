from typing import Optional
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# get command line args
import sys
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
        return 200
    except:
        return 400

# init fastapi
app = FastAPI()

# create database connection
from supabase import create_client, Client
supabase_url: str = CONFIG['supabase_url']
supabase_token: str = CONFIG['supabase_service_token']
supabase: Client = create_client(supabase_url, supabase_token)


@app.get("/")
def read_root():
    return {'status':200}


@app.post("api/v1/function/extractAndInsertURL")
def webhook_extractAndInsertURL(body: PostData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            return body