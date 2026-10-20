import os, httpx

async def synthesize(text:str):
    key=os.getenv('ELEVENLABS_API_KEY','').strip(); voice=os.getenv('ELEVENLABS_VOICE_ID','').strip()
    if not key or not voice: return None, 'tts_not_configured'
    model=os.getenv('ELEVENLABS_MODEL','eleven_flash_v2_5')
    url=f'https://api.elevenlabs.io/v1/text-to-speech/{voice}'
    async with httpx.AsyncClient(timeout=45) as c:
        r=await c.post(url,headers={'xi-api-key':key,'accept':'audio/mpeg','content-type':'application/json'},json={'text':text,'model_id':model,'voice_settings':{'stability':0.45,'similarity_boost':0.75}})
    if r.status_code>=400: return None, f'elevenlabs_{r.status_code}'
    return r.content, None
