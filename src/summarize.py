import csv,json,statistics
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

if Path('assets/Times.ttf').exists():
 font_manager.fontManager.addfont('assets/Times.ttf');family='Times New Roman'
else:
 family='DejaVu Serif'
plt.rcParams.update({'font.family':family,'font.size':10,'figure.figsize':(7,2.5)})
comma=FuncFormatter(lambda v,_:f'{v:g}'.replace('.',','))
def vn(v,d=2):return f'{v:.{d}f}'.replace('.',',')

def read(p):
 with p.open() as f:return list(csv.DictReader(f))
def p95(x):return float(np.percentile(x,95)) if x else 0

WARMUP,END=20,120
sample=Path('metrics/E2_B_C2_0_1/sent.jsonl')
Path('data/sample.jsonl').write_text('\n'.join(sample.read_text().splitlines()[:1000])+'\n')
summary=[]
for p in sorted(Path('metrics').glob('*/result.json')):
 r=json.loads(p.read_text()); d=p.parent
 lat=[float(x['latency_seconds']) for x in read(d/'freshness.csv') if WARMUP<=float(x['produced_seconds'])<END]
 resources=[x for x in read(d/'resources.csv') if r['start_epoch']+WARMUP<=float(x['epoch'])<=r['start_epoch']+END]
 total={};app=[]
 for x in resources:
  b=total.setdefault(float(x['epoch']),[0,0])
  b[0]+=float(x['cpu_percent']);b[1]+=float(x['memory_bytes'])/1024**2
  if '-app-' in x['service']:app.append(float(x['cpu_percent']))
 vals=list(total.values())
 t=read(d/'timeline.csv');after=[x for x in t if WARMUP<=float(x['seconds'])<=END]
 throughput=(int(after[-1]['visible'])-int(after[0]['visible']))/(float(after[-1]['seconds'])-float(after[0]['seconds']))
 costs=read(d/'sql.csv')
 r.update(p50=float(np.median(lat)),p95=p95(lat),samples_measured=len(lat),missing_samples=len(range(0,r['sent'],max(1,r['rate']//2)))-r['samples'],
  cpu_mean=statistics.mean(x[0] for x in vals),cpu_max=max(x[0] for x in vals),cpu_app_mean=statistics.mean(app),cpu_app_max=max(app),
  ram_mean=statistics.mean(x[1] for x in vals),ram_max=max(x[1] for x in vals),throughput=throughput,backlog_max=max(int(x['backlog']) for x in t),
  q1_p95=p95([float(x['q1_seconds'])*1000 for x in costs]),q2_p95=p95([float(x['q2_seconds'])*1000 for x in costs]))
 if (d/'batch.csv').exists():r['batch_mean']=statistics.mean(float(x.split(',')[2]) for x in (d/'batch.csv').read_text().splitlines())
 if r['experiment']=='E3':
  tl=[(float(x['seconds']),int(x['backlog'])) for x in t]
  peak=max(tl,key=lambda x:x[1])[0]
  r['recovered_at']=next(s for s,b in tl if s>peak and b<=250)
 summary.append(r)

with open('metrics/correctness.csv','w') as f:
 w=csv.writer(f);w.writerow(['run','expected_count','actual_count','count_error','expected_sum','actual_sum','sum_error'])
 for r in summary:
  events={e['event_id']:e for e in [json.loads(x) for x in (Path('metrics')/r['name']/'sent.jsonl').read_text().splitlines()]}
  expected_sum=sum(e['amount_cents'] for e in events.values())
  w.writerow([r['name'],len(events),r['count'],r['count']-len(events),expected_sum,r['sum'],r['sum']-expected_sum])
Path('metrics/summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
keys=['name','rate','p50','p95','samples_measured','missing_samples','throughput','backlog_max','cpu_mean','cpu_max','cpu_app_mean','cpu_app_max','ram_mean','ram_max','q1_p95','q2_p95','batch_mean','correct']
with open('metrics/summary.csv','w') as f:
 w=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore');w.writeheader();w.writerows(summary)

def group(exp,mode='B',cores=2):return [r for r in summary if r['experiment']==exp and r['mode']==mode and r['cores']==cores]
def avg(rows,key):return statistics.mean(r[key] for r in rows)
def save(name):plt.tight_layout();plt.savefig('metrics/'+name+'.png',dpi=180);plt.close()
LABEL={'A':'A (batch-only)','B':'B (Lambda)'}
STYLE={'A':'-','B':'--'}

def bars(names,groups,panels,file):
 fig,axes=plt.subplots(1,len(panels),figsize=(7.5,2.5))
 for ax,(key,label) in zip(axes,panels):
  values=[avg(g,key) for g in groups]
  b=ax.bar(names,values,color='white',edgecolor='black',width=.6);b[1].set_hatch('///')
  for i,v in enumerate(values):ax.text(i,v,vn(v,0 if v>=1000 else 2),ha='center',va='bottom',fontsize=9)
  ax.set_ylim(0,max(values)*1.25);ax.set_ylabel(label);ax.yaxis.set_major_formatter(comma)
 save(file)

# Hình 1: p95 freshness theo mức tải
fig,ax=plt.subplots(figsize=(7.5,2.1))
for mode,marker in [('A','o-'),('B','s--')]:
 y=[avg(group(e,mode),'p95') for e in ['E1','E2']]
 ax.plot([20,100],y,marker,color='black',label=LABEL[mode])
 for x,v in zip([20,100],y):ax.annotate(vn(v)+' s',(x,v),xytext=(0,6),textcoords='offset points',ha='center')
ax.set(xlabel='Tốc độ phát (sự kiện/giây)',ylabel='p95 freshness (giây)',xlim=(10,110),ylim=(0,38),xticks=[20,100])
ax.yaxis.set_major_formatter(comma);ax.legend(loc='center right');save('01_do_tre')

# Hình 2: throughput, backlog theo thời gian (E2) và backlog E3
fig,axes=plt.subplots(1,3,figsize=(7.5,2.9))
for mode in ['A','B']:
 t=read(Path('metrics')/group('E2',mode)[0]['name']/'timeline.csv');last={}
 for x in t:last[int(float(x['seconds'])//10)]=x
 xx=[];yy=[];pn=0;pt=0
 for _,x in sorted(last.items()):
  now=float(x['seconds']);n=int(x['visible']);xx.append(now);yy.append((n-pn)/(now-pt));pn=n;pt=now
 axes[0].plot(xx,yy,STYLE[mode],color='black',label=LABEL[mode])
 axes[1].plot([float(x['seconds']) for x in t],[int(x['backlog']) for x in t],STYLE[mode],color='black',label=LABEL[mode])
axes[0].axhline(100,color='grey',lw=.8,ls=':',label='Input rate')
t=read(Path('metrics')/group('E3')[0]['name']/'timeline.csv')
axes[2].plot([float(x['seconds']) for x in t],[int(x['backlog']) for x in t],'--',color='black',label='B, E3')
for s in [40,70]:axes[2].axvline(s,color='grey',lw=.8,ls=':')
axes[2].text(55,2200,'speed dừng',ha='center',fontsize=8)
axes[0].set(title='(a) E2: throughput',xlabel='Thời gian (giây)',ylabel='Sự kiện/giây')
axes[1].set(title='(b) E2: backlog',xlabel='Thời gian (giây)',ylabel='Sự kiện')
axes[2].set(title='(c) E3: backlog',xlabel='Thời gian (giây)',ylabel='Sự kiện',ylim=(0,2400))
for ax in axes:ax.title.set_fontsize(10)
h,l=axes[0].get_legend_handles_labels();fig.legend(h,l,loc='lower center',ncol=3,fontsize=9,frameon=False)
plt.tight_layout(rect=(0,.1,1,1));plt.savefig('metrics/02_toc_do_ton_dong.png',dpi=180);plt.close()

# Hình 3: trước/sau cải tiến
bars(['E2','E4'],[group('E2'),group('E4')],[('p95','p95 freshness (giây)'),('throughput','Throughput (sự kiện/giây)'),('cpu_mean','CPU trung bình (%)'),('ram_mean','RAM trung bình (MiB)')],'03_cai_tien')
# Hình 4: E5, C1 và C2
bars(['C1','C2'],[group('E5',cores=2),group('E5',cores=4)],[('throughput','Throughput (sự kiện/giây)'),('p95','p95 freshness (giây)'),('cpu_app_mean','CPU app trung bình (%)'),('ram_mean','RAM trung bình (MiB)')],'04_tang_tai_nguyen')
print(json.dumps(summary,ensure_ascii=False,indent=2))
