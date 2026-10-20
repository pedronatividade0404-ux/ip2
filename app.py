import os, time, json, asyncio, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Header, Query, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from supabase import create_client, Client
import httpx

load_dotenv()
TOKEN=os.getenv('JARVIS_CLOUD_TOKEN','').strip()
SUPABASE_URL=os.getenv('SUPABASE_URL','').strip(); SUPABASE_SERVICE_KEY=os.getenv('SUPABASE_SERVICE_KEY','').strip()
AI_BASE_URL=os.getenv('AI_BASE_URL','https://api.groq.com/openai/v1').rstrip('/')
AI_API_KEY=os.getenv('AI_API_KEY','').strip(); AI_MODEL=os.getenv('AI_MODEL','openai/gpt-oss-120b').strip()
if not TOKEN: raise RuntimeError('JARVIS_CLOUD_TOKEN nao configurado')
if not SUPABASE_URL or not SUPABASE_SERVICE_KEY: raise RuntimeError('Supabase nao configurado')
supabase:Client=create_client(SUPABASE_URL,SUPABASE_SERVICE_KEY)
app=FastAPI(title='JARVIS V22 Cloud',version='22.0')
app.add_middleware(CORSMiddleware,allow_origins=[x.strip() for x in os.getenv('JARVIS_CORS_ORIGINS','*').split(',')],allow_credentials=False,allow_methods=['*'],allow_headers=['*'])
STATIC=Path(__file__).parent/'static'; app.mount('/static',StaticFiles(directory=STATIC),name='static')
connections:dict[str,WebSocket]={}

def auth(v): return bool(v) and v==TOKEN
def denied(): return JSONResponse({'ok':False,'error':'unauthorized'},401)
def now(): return datetime.now(timezone.utc).isoformat()

def upsert_device(device_id,name,kind,metadata=None):
    row={'device_id':device_id,'device_name':name,'device_type':kind,'metadata':metadata or {},'last_seen':now()}
    supabase.table('jarvis_devices').upsert(row,on_conflict='device_id').execute()

@app.get('/')
def dashboard(): return FileResponse(STATIC/'index.html')
@app.get('/health')
def health(): return {'ok':True,'service':'JARVIS V22 Cloud','time':time.time(),'online_devices':list(connections)}

@app.websocket('/ws/{device_id}')
async def ws(websocket:WebSocket,device_id:str):
    token=websocket.query_params.get('token','')
    if not auth(token): await websocket.close(code=4401); return
    await websocket.accept(); connections[device_id]=websocket
    try:
        upsert_device(device_id,device_id,'agent',{'transport':'websocket'})
        await websocket.send_json({'type':'connected','device_id':device_id,'server_time':now()})
        while True:
            msg=await websocket.receive_json()
            if msg.get('type')=='telemetry':
                upsert_device(device_id,msg.get('name',device_id),msg.get('device_type','pc'),msg.get('data',{}))
            elif msg.get('type')=='command_result':
                supabase.table('jarvis_command_results').insert({'command_id':msg.get('command_id'),'device_id':device_id,'ok':bool(msg.get('ok')),'result':msg.get('result',{})}).execute()
    except WebSocketDisconnect: pass
    finally:
        if connections.get(device_id) is websocket: connections.pop(device_id,None)

@app.post('/v1/device/heartbeat')
async def heartbeat(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json(); did=str(b.get('device_id') or 'unknown')
    row={'device_id':did,'device_name':str(b.get('device_name') or did),'device_type':str(b.get('device_type') or 'client'),'battery':b.get('battery'),'charging':b.get('charging'),'metadata':b.get('metadata') or {},'last_seen':now()}
    supabase.table('jarvis_devices').upsert(row,on_conflict='device_id').execute(); return {'ok':True,'device':row}

@app.get('/v1/devices')
def devices(x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    rows=supabase.table('jarvis_devices').select('*').order('last_seen',desc=True).execute().data or []
    for r in rows:r['websocket_online']=r['device_id'] in connections
    return {'ok':True,'devices':rows}

@app.post('/v1/command/{device_id}')
async def command(device_id:str,req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json(); cid=str(uuid.uuid4())
    row={'id':cid,'device_id':device_id,'command':str(b.get('command') or ''),'args':b.get('args') or {},'status':'queued'}
    supabase.table('jarvis_commands').insert(row).execute()
    ws=connections.get(device_id)
    if ws:
        await ws.send_json({'type':'command','command_id':cid,'command':row['command'],'args':row['args']})
        supabase.table('jarvis_commands').update({'status':'sent','sent_at':now()}).eq('id',cid).execute()
        return {'ok':True,'command_id':cid,'delivered':True}
    return {'ok':True,'command_id':cid,'delivered':False,'message':'Dispositivo offline; comando ficou na fila.'}

@app.post('/v1/location')
async def location(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json(); lat=b.get('latitude',b.get('lat')); lon=b.get('longitude',b.get('lon'))
    if lat is None or lon is None:return JSONResponse({'ok':False,'error':'latitude_longitude_required'},400)
    row={'device_id':str(b.get('device_id') or 'j4plus'),'latitude':float(lat),'longitude':float(lon),'accuracy':b.get('accuracy'),'altitude':b.get('altitude'),'battery':b.get('battery')}
    supabase.table('jarvis_location').upsert(row,on_conflict='device_id').execute(); return {'ok':True,'location':row}

@app.post('/v1/sentinel/state')
async def sentinel_state(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    b=await req.json(); row={'device_id':str(b.get('device_id') or 'j4plus'),'enabled':bool(b.get('enabled')),'mode':str(b.get('mode') or 'motion_face')}
    supabase.table('jarvis_sentinel_state').upsert(row,on_conflict='device_id').execute(); return {'ok':True,'state':row}

@app.post('/v1/sentinel/image')
async def sentinel_image(image:UploadFile=File(...),device_id:str=Form('j4plus'),x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    data=await image.read(); ct=(image.content_type or 'image/jpeg').lower()
    if not data or len(data)>8*1024*1024:return JSONResponse({'ok':False,'error':'invalid_image'},400)
    ext={'image/png':'png','image/webp':'webp'}.get(ct,'jpg'); path=f"{device_id}/{datetime.now(timezone.utc):%Y/%m/%d}/{uuid.uuid4().hex}.{ext}"
    supabase.storage.from_('jarvis-sentinel').upload(path,data,{'content-type':ct,'upsert':'false'})
    return {'ok':True,'image_url':f'{SUPABASE_URL}/storage/v1/object/public/jarvis-sentinel/{path}'}

@app.post('/v1/sentinel/event')
async def sentinel_event(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    b=await req.json(); row={'device_id':str(b.get('device_id') or 'j4plus'),'event_type':str(b.get('event_type') or 'motion'),'person_name':b.get('person_name'),'confidence':b.get('confidence'),'image_url':b.get('image_url'),'metadata':b.get('metadata') or {}}
    r=supabase.table('jarvis_sentinel_events').insert(row).execute(); return {'ok':True,'event':(r.data or [row])[0]}

@app.get('/v1/sentinel/events')
def events(limit:int=Query(30,ge=1,le=200),x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    return {'ok':True,'events':supabase.table('jarvis_sentinel_events').select('*').order('created_at',desc=True).limit(limit).execute().data or []}

@app.post('/v1/memory')
async def memory(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    b=await req.json(); row={'category':str(b.get('category') or 'general'),'content':str(b.get('content') or ''),'importance':float(b.get('importance',0.5)),'metadata':b.get('metadata') or {}}
    r=supabase.table('jarvis_memories').insert(row).execute();return {'ok':True,'memory':(r.data or [row])[0]}

@app.post('/v1/routines')
async def routine(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    b=await req.json(); row={'name':str(b.get('name') or 'Rotina'),'enabled':bool(b.get('enabled',True)),'cron':str(b.get('cron') or ''),'timezone':str(b.get('timezone') or 'America/Sao_Paulo'),'actions':b.get('actions') or []}
    r=supabase.table('jarvis_routines').insert(row).execute();return {'ok':True,'routine':(r.data or [row])[0]}

@app.get('/v1/routines')
def routines(x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    return {'ok':True,'routines':supabase.table('jarvis_routines').select('*').order('created_at',desc=True).execute().data or []}

@app.post('/v1/chat')
async def chat(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token):return denied()
    if not AI_API_KEY:return JSONResponse({'ok':False,'error':'ai_not_configured'},503)
    b=await req.json(); did=str(b.get('device_id') or 'web'); text=str(b.get('text') or '').strip()
    if not text:return JSONResponse({'ok':False,'error':'text_required'},400)
    low=text.lower()
    if 'ativar' in low and 'sentinela' in low:
        supabase.table('jarvis_sentinel_state').upsert({'device_id':'j4plus','enabled':True,'mode':'motion_face'},on_conflict='device_id').execute(); return {'ok':True,'answer':'Modo Sentinela ativado.','action':'sentinel_activate'}
    if 'desativar' in low and 'sentinela' in low:
        supabase.table('jarvis_sentinel_state').upsert({'device_id':'j4plus','enabled':False,'mode':'motion_face'},on_conflict='device_id').execute(); return {'ok':True,'answer':'Modo Sentinela desativado.','action':'sentinel_deactivate'}
    hist=supabase.table('jarvis_cloud_messages').select('role,content').eq('device_id',did).order('created_at',desc=True).limit(12).execute().data or [];hist.reverse()
    mem=supabase.table('jarvis_memories').select('category,content,importance').order('importance',desc=True).limit(12).execute().data or []
    memtxt='\n'.join(f"- [{m['category']}] {m['content']}" for m in mem)
    system='Voce e JARVIS, assistente pessoal em portugues brasileiro. Seja rapido, natural e objetivo. Nunca diga que executou uma acao local sem confirmacao do dispositivo. Memorias relevantes:\n'+memtxt
    msgs=[{'role':'system','content':system}]+[{'role':x['role'],'content':x['content']} for x in hist]+[{'role':'user','content':text}]
    async with httpx.AsyncClient(timeout=45) as c:
        rr=await c.post(AI_BASE_URL+'/chat/completions',headers={'Authorization':f'Bearer {AI_API_KEY}'},json={'model':AI_MODEL,'messages':msgs,'temperature':0.45,'max_tokens':800})
    if rr.status_code>=400:return JSONResponse({'ok':False,'error':'ai_error','detail':rr.text[:700]},502)
    answer=rr.json()['choices'][0]['message']['content'].strip()
    supabase.table('jarvis_cloud_messages').insert([{'device_id':did,'role':'user','content':text},{'device_id':did,'role':'assistant','content':answer}]).execute()
    return {'ok':True,'answer':answer,'model':AI_MODEL}

# ===== V23: rotas, noticias, TTS, memoria pesquisavel e configuracao =====
from services.maps import route as build_route
from services.news import headlines
from services.tts import synthesize
from fastapi.responses import Response

@app.get('/v1/memories')
def memories(q:str='',limit:int=Query(30,ge=1,le=200),x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    query=supabase.table('jarvis_memories').select('*').order('importance',desc=True).limit(limit)
    if q: query=query.ilike('content',f'%{q}%')
    return {'ok':True,'memories':query.execute().data or []}

@app.delete('/v1/memory/{memory_id}')
def delete_memory(memory_id:int,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    supabase.table('jarvis_memories').delete().eq('id',memory_id).execute(); return {'ok':True}

@app.patch('/v1/routines/{routine_id}')
async def update_routine(routine_id:int,req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json(); allowed={k:b[k] for k in ('name','enabled','cron','timezone','actions') if k in b}
    r=supabase.table('jarvis_routines').update(allowed).eq('id',routine_id).execute(); return {'ok':True,'routine':(r.data or [{}])[0]}

@app.delete('/v1/routines/{routine_id}')
def delete_routine(routine_id:int,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    supabase.table('jarvis_routines').delete().eq('id',routine_id).execute(); return {'ok':True}

@app.post('/v1/maps/route')
async def maps_route(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json()
    try:return {'ok':True,'route':await build_route(str(b.get('destination') or ''),b.get('origin'))}
    except Exception as e:return JSONResponse({'ok':False,'error':str(e)},400)

@app.get('/v1/news')
async def news(limit:int=Query(5,ge=1,le=10),x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    try:return {'ok':True,'headlines':await headlines(limit)}
    except Exception as e:return JSONResponse({'ok':False,'error':str(e)},502)

@app.post('/v1/tts')
async def tts(req:Request,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    b=await req.json(); audio,err=await synthesize(str(b.get('text') or ''))
    if err:return JSONResponse({'ok':False,'error':err},503)
    return Response(audio,media_type='audio/mpeg')

@app.get('/v1/sentinel/state/{device_id}')
def get_sentinel_state(device_id:str,x_jarvis_token:str|None=Header(None)):
    if not auth(x_jarvis_token): return denied()
    rows=supabase.table('jarvis_sentinel_state').select('*').eq('device_id',device_id).limit(1).execute().data or []
    return {'ok':True,'state':rows[0] if rows else {'device_id':device_id,'enabled':False,'mode':'motion_face'}}
