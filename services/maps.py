import os, httpx
async def route(destination:str, origin:str|None=None):
    origin=origin or os.getenv('DEFAULT_ORIGIN','Morada de Laranjeiras, Serra, ES')
    headers={'User-Agent':'JARVIS-Personal-Assistant/23'}
    async with httpx.AsyncClient(timeout=20,headers=headers) as c:
        async def geo(q):
            r=await c.get('https://nominatim.openstreetmap.org/search',params={'q':q,'format':'jsonv2','limit':1}); r.raise_for_status(); a=r.json()
            if not a: raise ValueError(f'local_nao_encontrado:{q}')
            return float(a[0]['lat']),float(a[0]['lon']),a[0].get('display_name',q)
        olat,olon,oname=await geo(origin); dlat,dlon,dname=await geo(destination)
        r=await c.get(f'https://router.project-osrm.org/route/v1/driving/{olon},{olat};{dlon},{dlat}',params={'overview':'false','steps':'false'}); r.raise_for_status(); x=r.json()['routes'][0]
    return {'origin':oname,'destination':dname,'distance_km':round(x['distance']/1000,1),'duration_min':round(x['duration']/60),'map_url':f'https://www.openstreetmap.org/directions?engine=fossgis_osrm_car&route={olat}%2C{olon}%3B{dlat}%2C{dlon}'}
