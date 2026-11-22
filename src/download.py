import hashlib,json,datetime,urllib.request
from pathlib import Path
url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'
p=Path('data/source.parquet');p.parent.mkdir(exist_ok=True)
if not p.exists():urllib.request.urlretrieve(url,p)
r=dict(url=url,file=p.name,download_date=datetime.datetime.fromtimestamp(p.stat().st_mtime,datetime.timezone.utc).isoformat(),sha256=hashlib.sha256(p.read_bytes()).hexdigest())
Path('data/download.json').write_text(json.dumps(r,indent=2))
print(json.dumps(r))
