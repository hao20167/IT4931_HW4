import json
from pathlib import Path
from common import db,insert_speed
results=list(Path('metrics').glob('*/result.json'))
assert len(results)==16,len(results)
for p in results:
 r=json.loads(p.read_text());assert r['correct'] and not r['errors'] and r['samples']>=50
with db() as c:
 with c.cursor() as q:
  q.execute('SELECT * FROM q2');before=q.fetchone()
  q.execute('SELECT count(*) FROM speed_events');stored_before=q.fetchone()
  q.execute('SELECT * FROM speed_events LIMIT 100');rows=q.fetchall()
insert_speed(rows);insert_speed(rows)
with db() as c:
 with c.cursor() as q:
  q.execute('SELECT * FROM q2');after=q.fetchone()
  q.execute('SELECT count(*) FROM speed_events');stored_after=q.fetchone()
assert before==after and stored_before==stored_after
print('16 lượt đo: số lượng và tổng tiền đúng; ghi lại sự kiện không làm thay đổi kết quả.')
