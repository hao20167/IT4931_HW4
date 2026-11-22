import subprocess,time,json,threading,csv
from pathlib import Path
runs=[]
for exp in ['E1','E2']:
 for mode in ['A','B']:
  for repeat in [1,2]:runs.append((mode,exp,repeat,2,0))
for repeat in [1,2]:runs.append(('B','E3',repeat,2,0))
for repeat in [1,2]:runs.append(('B','E4',repeat,2,1))
for cores in [2,4]:
 for repeat in [1,2]:runs.append(('B','E5',repeat,cores,0))
for mode,exp,repeat,cores,improved in runs:
 name=f'{exp}_{mode}_C{cores}_{improved}_{repeat}'; out=Path('metrics')/name
 if (out/'result.json').exists():continue
 subprocess.run(['docker','update','--cpus',str(cores),'--memory',f'{cores}g','--memory-swap',f'{cores}g',subprocess.check_output(['docker','compose','ps','-q','app'],text=True).strip()],check=True)
 out.mkdir(parents=True,exist_ok=True);done=threading.Event()
 def stats():
  with (out/'resources.csv').open('w') as f:
   w=csv.writer(f);w.writerow(['epoch','service','cpu_percent','memory_bytes'])
   while not done.is_set():
    ids=subprocess.check_output(['docker','compose','ps','-q'],text=True).split()
    s=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}',*ids],capture_output=True,text=True)
    stamp=time.time()
    for line in s.stdout.splitlines():
     r=json.loads(line); v=r['MemUsage'].split('/')[0].strip(); units={'KiB':1024,'MiB':1024**2,'GiB':1024**3,'B':1}
     for u in ['GiB','MiB','KiB','B']:
      if v.endswith(u):mem=float(v[:-len(u)])*units[u];break
     w.writerow([stamp,r['Name'],float(r['CPUPerc'].strip('%')),mem]);f.flush()
    done.wait(1)
 t=threading.Thread(target=stats);t.start()
 try:
  with (out/'runner.log').open('w') as log:
   p=subprocess.run(['docker','compose','exec','-T','app','python3','src/run.py',mode,exp,str(repeat),str(cores),str(improved),str(150 if exp=='E5' else (20 if exp=='E1' else 100))],stdout=log,stderr=subprocess.STDOUT)
  if p.returncode:raise RuntimeError(name+' thất bại')
 finally:done.set();t.join()
 print(name+' hoàn tất',flush=True)
