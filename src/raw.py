import json,time,os
from pathlib import Path
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq
c=KafkaConsumer('events',bootstrap_servers='kafka:9092',auto_offset_reset='earliest',enable_auto_commit=False,group_id='raw')
Path('data/raw').mkdir(parents=True,exist_ok=True)
rows=[]; last=time.monotonic(); h=0
while True:
    for records in c.poll(timeout_ms=200,max_records=1000).values():
        for m in records:
            r=json.loads(m.value); assert r['seq']==h+len(rows)+1
            rows.append(r)
    if rows and time.monotonic()-last>=1:
        end=rows[-1]['seq']; path=f'data/raw/{end:09}.parquet'
        pq.write_table(pa.Table.from_pylist(rows),path+'.tmp'); os.replace(path+'.tmp',path)
        h=end; rows=[]; last=time.monotonic()
        Path('data/raw_h.tmp').write_text(str(h)); os.replace('data/raw_h.tmp','data/raw_h')
        c.commit()
