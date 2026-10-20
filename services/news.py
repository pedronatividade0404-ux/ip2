import httpx
async def headlines(limit=5):
    async with httpx.AsyncClient(timeout=15,headers={'User-Agent':'JARVIS/23'}) as c:
        r=await c.get('https://g1.globo.com/rss/g1/'); r.raise_for_status(); txt=r.text
    import re, html
    items=re.findall(r'<item>(.*?)</item>',txt,re.S)[:limit]; out=[]
    for item in items:
        m=re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>',item,re.S)
        if m: out.append(html.unescape(re.sub('<.*?>','',m.group(1))).strip())
    return out
