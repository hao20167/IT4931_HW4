import json,hashlib,datetime,math
from decimal import Decimal,ROUND_HALF_UP
from pathlib import Path
import pyarrow.parquet as pq
p=Path('data/source.parquet')
f=pq.ParquetFile(p)
valid=[]; rejected=0; scanned=0; seen=set()
for batch in f.iter_batches(batch_size=65536):
    for r in batch.to_pylist():
        scanned+=1
        t=r.get('tpep_pickup_datetime'); a=r.get('total_amount'); cat=r.get('PULocationID')
        if t is None or t.year!=2025 or t.month!=1 or a is None or not math.isfinite(a) or a<0 or a>10000 or cat is None or not 1<=cat<=265:
            rejected+=1; continue
        eid=hashlib.sha256(json.dumps(r,sort_keys=True,default=str).encode()).hexdigest()
        if eid in seen:
            rejected+=1; continue
        seen.add(eid)
        valid.append(dict(event_id=eid,seq=len(valid)+1,event_time=t.isoformat(),category=str(cat),amount_cents=int((Decimal(str(a))*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))))
    if len(valid)>=20000:
        break
valid=valid[:20000]
for name,rows in [('events.jsonl',valid),('sample.jsonl',valid[:1000])]:
    Path('data/'+name).write_text(''.join(json.dumps(x)+'\n' for x in rows))
manifest=dict(url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet',download_date=datetime.datetime.fromtimestamp(p.stat().st_mtime,datetime.timezone.utc).isoformat(),file='source.parquet',sha256=hashlib.sha256(p.read_bytes()).hexdigest(),input_rows=f.metadata.num_rows,scanned_rows=scanned,valid_scanned=scanned-rejected,rejected_scanned=rejected,selected=len(valid),seed=None,selection='20.000 bản ghi hợp lệ đầu tiên theo thứ tự tệp; đọc hết nhóm quét cuối',rules='Tháng 1/2025; vùng đón 1–265; tổng tiền hữu hạn từ 0 đến 10.000 USD; loại bản ghi trùng theo SHA-256 toàn bộ trường gốc; tiền đổi sang xu, làm tròn nửa lên.')
Path('data/manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
print(manifest)
