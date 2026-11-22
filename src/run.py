import csv,json,time,sys,subprocess,shutil,os,signal,statistics
from pathlib import Path
from collections import defaultdict
from kafka import KafkaProducer,KafkaAdminClient
from kafka.admin import NewTopic
from common import db
mode=sys.argv[1]; exp=sys.argv[2]; repeat=sys.argv[3]; cores=int(sys.argv[4]); improved=int(sys.argv[5]); rate=int(sys.argv[6]) if len(sys.argv)>6 else (20 if exp=='E1' else (150 if exp=='E5' else 100))
name=f'{exp}_{mode}_C{cores}_{improved}_{repeat}'
out=Path('metrics')/name; out.mkdir(parents=True,exist_ok=True)
for p in ['data/raw','data/checkpoint']:
    shutil.rmtree(p,ignore_errors=True)
for p in Path('data').glob('*ready'): p.unlink()
for p in ['data/raw_h','data/batch_cost.csv']:
    Path(p).unlink(missing_ok=True)
with db() as c:
    with c.cursor() as q:q.execute(Path('sql/schema.sql').read_text())
admin=KafkaAdminClient(bootstrap_servers='kafka:9092')
try:admin.delete_topics(['events'])
except Exception:pass
time.sleep(3)
for attempt in range(30):
    try:admin.create_topics([NewTopic('events',1,1)]); break
    except Exception:time.sleep(1)
else:raise RuntimeError('Không tạo được chủ đề Kafka')
admin.close()
processes=[]; handles=[]
def launch(kind):
    h=open(out/(kind+'.log'),'a'); handles.append(h)
    if kind=='raw':cmd=['python3','src/raw.py']
    else:cmd=['/opt/spark/bin/spark-submit','--driver-memory','512m' if cores==2 else '1g','--conf','spark.jars.ivySettings=/work/ivysettings.xml','--packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3','src/engine.py',kind,str(cores//2),'512m' if cores==2 else '1g',str(improved)]
    p=subprocess.Popen(cmd,stdout=h,stderr=h,start_new_session=True); processes.append(p); return p
raw=launch('raw'); batch=launch('batch'); speed=launch('speed') if mode=='B' else None
try:
    deadline=time.time()+240
    while not Path('data/batch_ready').exists() or (mode=='B' and not Path('data/speed_ready').exists()):
        if any(p.poll() is not None for p in processes):raise RuntimeError('Thành phần dừng khi khởi động')
        if time.time()>deadline:raise TimeoutError('Khởi động quá hạn')
        time.sleep(1)
    import threading
    events=[json.loads(x) for x in Path('data/events.jsonl').read_text().splitlines()]
    sent=[]; errors=[]; stop=threading.Event(); start=time.time()
    def produce():
        producer=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',retries=5,max_in_flight_requests_per_connection=1)
        for i,e in enumerate(events[:rate*120]):
            target=start+i/rate; time.sleep(max(0,target-time.time()))
            e=dict(e,seq=i+1,produced_at=time.time())
            try:producer.send('events',json.dumps(e).encode()).get(timeout=30); sent.append(e)
            except Exception as err:errors.append(str(err)); break
        producer.flush(); producer.close(); stop.set()
    thread=threading.Thread(target=produce); thread.start()
    samples={}; timeline=[]; sqlcost=[]; restart_at=None; interrupted=False
    monitor=db(); monitor.autocommit=True
    while True:
        now=time.time(); elapsed=now-start
        if exp=='E3' and elapsed>=40 and not interrupted:
            print(json.dumps({'action':'dung_xu_ly_luong','seconds':elapsed}),flush=True)
            os.killpg(speed.pid,signal.SIGTERM); speed.wait(timeout=20); restart_at=time.time()+30; interrupted=True
        if restart_at and time.time()>=restart_at:
            print(json.dumps({'action':'khoi_dong_lai','seconds':time.time()-start}),flush=True)
            speed=launch('speed'); restart_at=None
        with monitor.cursor() as q:
            q.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
            q.execute('SELECT event_id FROM visible'); visible={r[0] for r in q.fetchall()}
            for e in list(sent)[::max(1,rate//2)]:
                if e['event_id'] in visible and e['event_id'] not in samples:samples[e['event_id']]=(e['produced_at']-start,time.time()-e['produced_at'])
            t=time.perf_counter();q.execute('SELECT * FROM q1');q.fetchall(); q1=time.perf_counter()-t
            t=time.perf_counter();q.execute('SELECT * FROM q2');count,total=q.fetchone();q2=time.perf_counter()-t
            q.execute('COMMIT')
        assert int(count)==len(visible), 'Kết quả công bố không nhất quán'
        sqlcost.append([elapsed,q1,q2]); timeline.append([elapsed,len(sent),len(visible),len(sent)-len(visible),count,total])
        if stop.is_set() and len(visible)==len(sent):break
        if elapsed>300:raise TimeoutError('Chưa xử lý hết sau 300 giây')
        if batch.poll() is not None or raw.poll() is not None:raise RuntimeError('Thành phần dừng')
        time.sleep(.5)
    thread.join(); monitor.close()
    truth=defaultdict(lambda:[0,0])
    for e in {e['event_id']:e for e in sent}.values():truth[e['category']][0]+=1;truth[e['category']][1]+=e['amount_cents']
    with db() as c:
        with c.cursor() as q:q.execute('SELECT * FROM q1');actual={r[0]:[r[1],r[2]] for r in q.fetchall()}
    assert actual==dict(truth),(actual,truth)
    assert not errors,errors
    def write(file,head,rows):
        with (out/file).open('w') as f:w=csv.writer(f);w.writerow(head);w.writerows(rows)
    write('timeline.csv',['seconds','sent','visible','backlog','count','sum'],timeline)
    write('freshness.csv',['event_id','produced_seconds','latency_seconds'],[(k,*v) for k,v in samples.items()])
    write('sql.csv',['seconds','q1_seconds','q2_seconds'],sqlcost)
    (out/'sent.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in sent))
    if Path('data/batch_cost.csv').exists():shutil.copy('data/batch_cost.csv',out/'batch.csv')
    (out/'result.json').write_text(json.dumps(dict(name=name,mode=mode,experiment=exp,cores=cores,improved=improved,rate=rate,sent=len(sent),count=int(count),sum=int(total),correct=actual==dict(truth),errors=errors,poll_seconds=.5,warmup_seconds=20,start_epoch=start,end_epoch=time.time(),duration=time.time()-start,samples=len(samples))))
    print(name,'ĐÚNG',len(sent),flush=True)
finally:
    for p in processes:
        if p.poll() is None:
            try:os.killpg(p.pid,signal.SIGTERM)
            except ProcessLookupError:pass
    for p in processes:
        try:p.wait(timeout=20)
        except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL)
    for h in handles:h.close()
