import os
import re
import requests
from bs4 import BeautifulSoup
from typing import Optional
from fastapi import FastAPI, Request, Response
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
# FUNCTION: customJSONResponse(data: dict)
# bypass use of JSONResponse for something better
#
########################################################################
def customJSONResponse(data: dict):
    jsonData = json.dumps(dict,indent=4)
    headers = {
        'content-type':'application/json'
    }
    return Response(jsonData,200)


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
# FUNCTION: getWebpreview(url: str)
# uses lxml parser to get info from URL page
#
########################################################################
def getWebpreview(url: str):
    try:
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        }
        response = requests.get(url,headers=headers, timeout=10)
        html = response.text

        soup = BeautifulSoup(html, 'html.parser')

        
        ogtitle = None
        ogdescription = None
        ogimage = None
        ogurl = None
        ogsite_name = None
        text = None
        lines = None
        chunks = None

        try:
            ogtitle = soup.find('meta', property='og:title')['content']
        except:
            pass
        try:
            ogdescription = soup.find('meta', property='og:description')['content']
        except:
            pass
        try:
            ogimage = soup.find('meta', property='og:image')['content']
        except:
            pass
        try:
            ogurl = soup.find('meta', property='og:url')['content']
        except:
            pass
        try:
            ogsite_name = soup.find('meta', property='og:site_name')['content']
        except:
            pass
        try:
            # get text
            text = soup.get_text()

            # break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # drop blank lines
            text = ' \n '.join(chunk for chunk in chunks if chunk)
        except:
            pass

        data = {
            'status':200,
            'ogtitle':ogtitle,
            'ogdescription':ogdescription,
            'ogimage':ogimage,
            'ogurl':ogurl,
            'ogsite_name':ogsite_name,
            'text':text
        }
        return data
    except Exception as e:
        return {'status':400}


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
    except Exception as e:
        print(e)
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
    return customJSONResponse({'status':200})

@app.get('/api/v1/jobs')
async def get_jobs(request: Request, auth: Optional[str] = None, days: Optional[int] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            if days == None:
                data = supabase.table('jobs').select("*").execute()
                return JSONResponse(data)
            else:
                interval = f'{days} days'
                data = supabase.table('jobs').select("*").gte('created_at', f'current_date - interval {interval}').execute()
                return customJSONResponse(data[0])

@app.get('/api/v1/jobs/{uid}')
async def get_job(request: Request, auth: Optional[str] = None, uid: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            if uid:
                data = supabase.table('jobs').select("*").eq('uid', f'{uid}').execute()[0]
                return customJSONResponse(data[0])

@app.post('/api/v1/fetch/')
async def gitUpdate(request: Request, body: RowData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            os.system('sudo gitpullbroker')
            return customJSONResponse({'status':200})

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
            return customJSONResponse(responseData)

@app.post('/api/v1/function/getDataFromURL')
async def webhook_getDataFromURL(request: Request, body: RowData, auth: Optional[str] = None):
    if auth:
        if auth == AUTHORIZATION_TOKEN:
            record_data = body.record
            source_name = record_data['source_name']
            source_type = record_data['source_type']
            url = record_data['url']
            url_uid = record_data['uid']
            post_uid = record_data['post_uid']

            urlPreview = getWebpreview(url)

            INSERTDATA = {
                'source_name': source_name,
                'source_type': source_type,
                'post_uid': post_uid,
                'url_uid': url_uid,
                'url': url
            }
            INSERTDATA.update(urlPreview)
            INSERTDATA.pop('status')
            print(INSERTDATA.keys())
            sinkData('jobs',INSERTDATA)
            

            return customJSONResponse({'status':200})