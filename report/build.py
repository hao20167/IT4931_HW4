import json,statistics,math
from pathlib import Path
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,PageBreak,Image,KeepTogether,Preformatted
from reportlab.lib.enums import TA_JUSTIFY,TA_CENTER
from reportlab.graphics.shapes import Drawing,Rect,String,Line,Polygon

HO_TEN='Bùi Công Hào'
MSSV='202400042'
MA_LOP='175681'
REPO='https://github.com/hao20167/IT4931_HW4'

if Path('assets/Times.ttf').exists():
 regular,bold='assets/Times.ttf','assets/TimesBold.ttf'
else:
 import matplotlib
 base=Path(matplotlib.get_data_path())/'fonts/ttf'
 regular,bold=str(base/'DejaVuSerif.ttf'),str(base/'DejaVuSerif-Bold.ttf')
pdfmetrics.registerFont(TTFont('F',regular));pdfmetrics.registerFont(TTFont('FB',bold))
pdfmetrics.registerFontFamily('F',normal='F',bold='FB',italic='F',boldItalic='FB')
styles={'body':ParagraphStyle('body',fontName='F',fontSize=10.5,leading=13,spaceAfter=4,alignment=TA_JUSTIFY),
 'h':ParagraphStyle('h',fontName='FB',fontSize=12.5,leading=16,spaceBefore=4,spaceAfter=5),
 'title':ParagraphStyle('title',fontName='FB',fontSize=15,leading=19,spaceAfter=4,alignment=TA_CENTER),
 'info':ParagraphStyle('info',fontName='F',fontSize=11,leading=14,spaceAfter=10,alignment=TA_CENTER),
 'cap':ParagraphStyle('cap',fontName='F',fontSize=9.5,leading=11.5,spaceAfter=6,alignment=TA_JUSTIFY),
 'cell':ParagraphStyle('cell',fontName='F',fontSize=9,leading=10.5),
 'code':ParagraphStyle('code',fontName='Courier',fontSize=8,leading=9.5,spaceAfter=6)}
story=[]
def p(t,style='body'):story.append(Paragraph(t,styles[style]))
def h(t):p(t,'h')
def vn(v,d=2):return f'{v:,.{d}f}'.replace(',','_').replace('.',',').replace('_','.')
def table(rows,widths,size=None):
 st=styles['cell'] if size is None else ParagraphStyle('c',parent=styles['cell'],fontSize=size,leading=size+2)
 t=Table([[Paragraph(str(v),st) for v in row] for row in rows],colWidths=widths,hAlign='CENTER',repeatRows=1)
 t.setStyle(TableStyle([('GRID',(0,0),(-1,-1),.4,colors.black),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#eeeeee')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),1.5),('BOTTOMPADDING',(0,0),(-1,-1),2.5)]))
 story.append(t);story.append(Spacer(1,5))
def figure(name,caption,height=None):
 from reportlab.lib.utils import ImageReader
 iw,ih=ImageReader('metrics/'+name+'.png').getSize()
 story.append(KeepTogether([Image('metrics/'+name+'.png',width=485,height=485*ih/iw),Spacer(1,3),Paragraph(caption,styles['cap'])]))

rows=json.loads(Path('metrics/summary.json').read_text());m=json.loads(Path('data/manifest.json').read_text())
def g(e,mode='B',c=2):return [r for r in rows if r['experiment']==e and r['mode']==mode and r['cores']==c]
def a(rs,k):return statistics.mean(r[k] for r in rs)
def speedlog(rs):
 out=[]
 for r in rs:
  for line in (Path('metrics')/r['name']/'speed.log').read_text().splitlines():
   if line.startswith('{'):
    z=json.loads(line)
    if z['rows']>0:out.append(z)
 return out
E1A,E1B,E2A,E2B,E3,E4,C1,C2=g('E1','A'),g('E1'),g('E2','A'),g('E2'),g('E3'),g('E4'),g('E5',c=2),g('E5',c=4)
s2,s4=speedlog(E2B),speedlog(E4)
collect,write=statistics.mean(x['collect_seconds'] for x in s2),statistics.mean(x['write_seconds'] for x in s2)
nb2,nb4=len(s2)/len(E2B),len(s4)/len(E4)
change=(a(E2B,'p95')-a(E4,'p95'))/a(E2B,'p95')*100
missing=sum(r['missing_samples'] for r in rows);errors=sum(len(r['errors']) for r in rows)

p('THIẾT KẾ VÀ ĐÁNH GIÁ DATA PIPELINE<br/>THEO KIẾN TRÚC LAMBDA','title')
p(f'Họ và tên: {HO_TEN} – MSSV: {MSSV} – Lớp: IT4931 ({MA_LOP})<br/>Mã nguồn, số liệu thô và hướng dẫn chạy lại: {REPO}','info')

h('1. Bài toán và dữ liệu')
p('Hệ thống thống kê số chuyến taxi (COUNT) và tổng tiền (SUM) theo vùng đón khách. Hai phương án chạy trên cùng dữ liệu: <b>A – batch-only</b>, Spark batch chạy định kỳ 30 giây trên dữ liệu raw rồi cập nhật PostgreSQL; <b>B – Lambda</b>, giữ nguyên nhánh batch và thêm Spark Structured Streaming để đưa sự kiện mới vào serving trong lúc chờ lần batch tiếp theo.')
table([['Nguồn','NYC TLC Trip Record Data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)'],['URL tải',m['url']],['File, ngày tải','yellow_tripdata_2025-01.parquet (lưu thành data/source.parquet), tải ngày 21/11/2026'],['SHA-256',m['sha256']],['Số dòng',f'{vn(m["input_rows"],0)} dòng trong file; quét 65.536; hợp lệ 63.616; bị loại 1.920; chọn 20.000']],[80,415])
p(f'Script làm sạch đọc khối 65.536 dòng đầu tiên, giữ 63.616 dòng hợp lệ và loại 1.920 dòng: 1.899 dòng có total_amount âm (giao dịch hoàn tiền/điều chỉnh) và 21 dòng có thời điểm đón ngoài tháng 1/2025; không có dòng trùng hay mã vùng ngoài 1–265. Lấy 20.000 dòng hợp lệ đầu tiên theo thứ tự file nên không có bước lấy mẫu ngẫu nhiên và không cần seed. E1 phát 2.400 sự kiện đầu, E2–E4 phát 12.000, E5 phát 18.000 sự kiện đầu của tập này.')
p('Quy tắc hợp lệ: thời điểm đón thuộc tháng 1/2025, PULocationID trong khoảng 1–265, total_amount không rỗng, hữu hạn và nằm trong [0; 10.000] USD. Bản ghi trùng toàn bộ các trường gốc được loại theo SHA-256. Tiền được đổi sang cent (làm tròn half-up) để SUM là phép cộng số nguyên.')
table([['Trường','Cách tạo','Trường','Cách tạo'],['event_id','SHA-256 của toàn bộ trường gốc','produced_at','thời điểm producer gửi (UTC, epoch)'],['seq','1, 2, 3, … do producer gán khi phát','category','PULocationID (vùng đón khách)'],['event_time','tpep_pickup_datetime gốc','amount_cents','total_amount × 100, làm tròn half-up']],[70,170,70,185])

h('2. Môi trường và cấu hình tài nguyên')
p('Máy thử: MacBook Pro Apple M1 Pro 10 lõi, RAM 32 GB; Docker Desktop được cấp 10 CPU và 7,65 GiB RAM. Phần mềm: Spark 3.5.3 (local mode), Python 3.8.10, Java 11.0.24, Kafka 3.9.0 (KRaft, 1 topic, 1 partition), PostgreSQL 16.6. Hệ thống gồm ba container: <i>kafka</i> giới hạn 1 CPU/1 GiB, <i>db</i> giới hạn 1 CPU/512 MiB, và <i>app</i> chứa producer, raw consumer, hai job Spark và script đo.')
p('Hai cấu hình tài nguyên chỉ khác ở container app. <b>C1</b>: app 2 CPU/2 GiB, mỗi job Spark chạy local[1] với driver memory 512 MB. <b>C2</b>: app 4 CPU/4 GiB, mỗi job Spark local[2], driver memory 1 GB. Giới hạn được đặt bằng <i>docker update</i> trước mỗi lượt; Kafka, PostgreSQL, dữ liệu và mã xử lý giữ nguyên. E1–E4 chạy với C1, E5 chạy cả C1 và C2. Thư mục metrics/ đặt tên theo tổng số lõi của app, nên <i>…_C2_…</i> là cấu hình C1 và <i>…_C4_…</i> là cấu hình C2.')

h('3. Kiến trúc và cơ chế hợp nhất')
d=Drawing(485,175)
def box(x,y,w,hh,lines,dash=False):
 d.add(Rect(x,y,w,hh,fillColor=colors.white,strokeColor=colors.black,strokeDashArray=[3,2] if dash else None))
 for i,t in enumerate(lines):d.add(String(x+w/2,y+hh/2+4-11*i+5.5*(len(lines)-1)-4,t,fontName='F',fontSize=8.5,textAnchor='middle'))
def arrow(x1,y1,x2,y2):
 d.add(Line(x1,y1,x2,y2,strokeColor=colors.black));an=math.atan2(y2-y1,x2-x1);ux,uy=math.cos(an),math.sin(an)
 d.add(Polygon([x2,y2,x2-6*ux-3*uy,y2-6*uy+3*ux,x2-6*ux+3*uy,y2-6*uy-3*ux],fillColor=colors.black,strokeColor=colors.black))
W=85;G=15
xs=[i*(W+G) for i in range(5)]
for x,lines in zip(xs,[['Producer (Python)','replay theo rate','gán seq, produced_at'],['Kafka','topic events','1 partition'],['Raw consumer','kiểm tra seq liên tục','ghi .tmp rồi rename'],['Parquet raw','file bất biến','mốc H = seq cuối'],['Spark batch','30 giây/lần','đọc snapshot seq ≤ H']]):box(x,125,W,45,lines)
for i in range(4):arrow(xs[i]+W,147,xs[i+1],147)
box(100,62,185,40,['Spark Structured Streaming (chỉ ở B)','trigger 2 s, checkpoint, ghi idempotent'],dash=True)
box(300,52,185,60,['PostgreSQL','batch_agg, batch_events, state(H)','speed_events (khóa event_id, seq)','view q1, q2, visible'])
box(300,0,185,32,['Client Q1/Q2 và đo freshness','poll 0,5 s, snapshot REPEATABLE READ'])
arrow(xs[1]+W/2,125,xs[1]+W/2,102);arrow(285,82,300,82);arrow(xs[4]+W/2,125,xs[4]+W/2,112);arrow(392,32,392,52)
diagram=KeepTogether([d,Spacer(1,4),Paragraph('Sơ đồ kiến trúc. Khung nét đứt chỉ có ở phương án B; A dùng toàn bộ các thành phần còn lại, kể cả Kafka và raw consumer.',styles['cap'])])
p('<b>Raw và mốc H.</b> Raw consumer đọc Kafka theo thứ tự và kiểm tra seq tăng liên tiếp (không có lỗ hổng). Khoảng mỗi giây nó ghi các sự kiện đã nhận ra file Parquet tạm, đổi tên nguyên tử thành <i>&lt;seq cuối&gt;.parquet</i>, sau đó mới tăng mốc raw lên seq cuối và commit offset. Vì vậy mọi seq ≤ H đều đã nằm trong các file hoàn chỉnh. Batch chỉ đọc dữ liệu đã đi qua Kafka và không bao giờ đọc file đang ghi.')
p('<b>Batch view.</b> Mỗi 30 giây, Spark batch đọc lại toàn bộ file có seq ≤ H, bỏ trùng theo event_id, tính COUNT/SUM theo category. Trong một transaction có khóa bảng state, job thay toàn bộ batch_agg, batch_events (danh sách event_id đã tính) và cập nhật state.h = H. Truy vấn chỉ thấy trạng thái trước hoặc sau lần công bố, không thấy trạng thái dở dang.')
story.append(diagram)
p('<b>Speed view (B).</b> Structured Streaming đọc Kafka với checkpoint, mỗi micro-batch ghi các sự kiện vào speed_events bằng INSERT … ON CONFLICT DO NOTHING. event_id là khóa chính và seq có ràng buộc UNIQUE, nên khi job khởi động lại và xử lý lại một micro-batch, các dòng đã có bị bỏ qua. Script kiểm tra (src/check.py) ghi lại 100 sự kiện đã có hai lần và xác nhận Q2 cùng số dòng speed_events không đổi.')
p('<b>Hợp nhất.</b> Kết quả phục vụ = BatchAgg(seq ≤ H) + SpeedEvents(seq &gt; H). View q1 cộng batch_agg với phần speed_events có seq lớn hơn state.h; q2 cộng q1 cho toàn hệ thống; view visible trả đúng tập event_id đang đóng góp vào q1/q2. Vì H và batch view đổi trong cùng một transaction, khi H tăng thì các sự kiện vừa được batch bao phủ đồng thời rời khỏi phần speed, nên không có lúc thiếu hoặc cộng trùng. Ở A bảng speed_events luôn rỗng, kết quả chỉ gồm batch view. SQL của Q1/Q2 và schema nằm ở Phụ lục A.')

h('4. Phương pháp thử tải và đo')
table([['Thí nghiệm','Workload','Phương án','Lượt','Tài nguyên'],['E1 – tải thấp','20 sự kiện/s × 120 s','A, B','2 + 2','C1'],['E2 – tải cao','100 sự kiện/s × 120 s','A, B','2 + 2','C1'],['E3 – lỗi','như E2; dừng speed job ở giây 40 trong 30 s, chạy lại từ checkpoint','B','2','C1'],['E4 – sau cải tiến','như E2; trigger speed 1 s thay vì 2 s','B','2','C1'],['E5 – scale-up','150 sự kiện/s × 120 s','B','2 + 2','C1 và C2']],[80,190,60,45,110])
p('Các lượt chạy tuần tự, không bao giờ chạy hai hệ thống cùng lúc. Trước mỗi lượt script xóa và tạo lại topic Kafka, tạo lại schema PostgreSQL, xóa raw Parquet và checkpoint. Producer gửi đồng bộ (acks=all) theo lịch cố định i/rate giây, ghi lại các sự kiện gửi thành công; cả 16 lượt đều có 0 lỗi gửi.')
p(f'<b>Freshness.</b> Script đo chọn cứ rate/2 sự kiện lấy một mẫu (2 mẫu/giây, 240 mẫu/lượt). Cứ 0,5 giây, trong một transaction REPEATABLE READ READ ONLY, nó đọc view visible, chạy Q1 và Q2, đồng thời kiểm tra COUNT của Q2 bằng số event_id trong visible. Freshness của một mẫu = thời điểm lần poll đầu tiên thấy event_id trong visible − produced_at. Do chu kỳ poll, số đo có thể lớn hơn thực tế tối đa khoảng 0,5 s cộng thời gian truy vấn. Mẫu chưa xuất hiện được tiếp tục theo dõi tới khi hệ thống xử lý hết; trong cả 16 lượt có {missing} mẫu không xuất hiện. Bỏ 20 giây đầu làm warm-up, p50/p95 tính trên các sự kiện phát trong giây 20–120 (200 mẫu/lượt). Quy ước này áp dụng cho mọi thí nghiệm.')
p('<b>Throughput</b> = số event_id visible tăng thêm từ giây 20 đến giây 120, chia cho 100 s; Hình 2 vẽ thêm theo từng khoảng 10 s. <b>Backlog</b> = số sự kiện đã phát − số sự kiện visible, ghi ở mỗi lần poll. <b>CPU/RAM</b> lấy từ docker stats khoảng 1 lần/giây, cộng ba container; CPU tính theo % một lõi (200% = 2 lõi). Vì producer và raw consumer luôn chạy, chi phí thu nhận và lưu raw được tính ở cả A và B. Script đo cũng nằm trong container app nên phần chi phí này giống nhau giữa các phương án. <b>Correctness</b>: sau khi hệ thống bắt kịp, Q1 được so với COUNT/SUM tính trực tiếp từ các event_id duy nhất đã phát. Số liệu trong bảng là trung bình hai lượt.')

h('5. Kết quả E1–E3: batch-only so với Lambda')
table([['Thí nghiệm','p50 (s)','p95 (s)','Throughput (sk/s)','Backlog max','CPU tb / max (%)','RAM tb / max (MiB)']]+[[f'{e} – {mo}',vn(a(rs,'p50')),vn(a(rs,'p95')),vn(a(rs,'throughput')),vn(a(rs,'backlog_max'),0),vn(a(rs,'cpu_mean'),1)+' / '+vn(a(rs,'cpu_max'),1),vn(a(rs,'ram_mean'),0)+' / '+vn(a(rs,'ram_max'),0)] for e,mo,rs in [('E1','A',E1A),('E1','B',E1B),('E2','A',E2A),('E2','B',E2B),('E3','B',E3)]],[68,50,50,75,60,90,92])
figure('01_do_tre','Hình 1. p95 freshness theo mức tải (trung bình hai lượt mỗi điểm).',height=112)
p(f'Ở A, freshness gần như chỉ phụ thuộc chu kỳ batch: p50 khoảng 16,5 s (nửa chu kỳ 30 s) và p95 khoảng 31,5 s (khoảng 95% chu kỳ cộng {vn(a(E2A,"batch_mean"))} s chạy một lần batch). Ở B, p95 khoảng 2,2 s, tức xấp xỉ trigger 2 s cộng thời gian xử lý và poll. Khi tải tăng 5 lần từ E1 lên E2, p95 của cả hai phương án gần như không đổi, nghĩa là ở các mức tải này cả hai đều chưa bị giới hạn bởi tài nguyên.')
p(f'B có throughput gần bằng input rate ({vn(a(E2B,"throughput"))}/100 sự kiện/s) và backlog không vượt quá {vn(max(r["backlog_max"] for r in E2B),0)} sự kiện. A phục vụ theo từng đợt, backlog dạng răng cưa lên tới khoảng 3.360 sự kiện (khoảng 33 s dữ liệu); throughput của A trong cửa sổ 20–120 s thấp hơn input vì lần công bố cuối rơi sau giây 120, còn tính trên toàn lượt thì A cũng phục vụ đủ 12.000 sự kiện. Cái giá của B ở E2 là CPU trung bình tăng từ {vn(a(E2A,"cpu_mean"),1)}% lên {vn(a(E2B,"cpu_mean"),1)}% (khoảng +25%) và RAM tăng khoảng {vn(a(E2B,"ram_mean")-a(E2A,"ram_mean"),0)} MiB (+47%), chủ yếu do thêm một JVM Spark chạy liên tục. CPU max của cả hai đều chạm khoảng 200%, tức container app dùng hết 2 lõi trong lúc job batch chạy mỗi 30 s.')
figure('02_toc_do_ton_dong','Hình 2. (a) Throughput theo khoảng 10 s và (b) backlog theo thời gian của lượt E2 đầu tiên; (c) backlog của lượt E3 đầu tiên, hai đường chấm đánh dấu lúc dừng (40 s) và lúc chạy lại speed job (70 s).',height=165)
p(f'<b>E3.</b> Khi speed job bị dừng ở giây 40, backlog tăng tuyến tính tới khoảng 2.000 sự kiện. Ở khoảng giây 60, lần công bố batch kế tiếp nâng H và kéo backlog xuống; trong lúc speed job ngừng, hệ thống quay về hành vi giống A thay vì ngừng phục vụ. Speed job được chạy lại ở giây 70, mất vài giây khởi động JVM, rồi micro-batch đầu tiên xử lý toàn bộ phần tồn đọng; backlog về dưới 250 ở giây {vn(a(E3,"recovered_at"),1)}, tức khoảng {vn(a(E3,"recovered_at")-70,1)} s sau lệnh chạy lại. p95 của E3 là {vn(a(E3,"p95"))} s do các sự kiện phát trong thời gian dừng. Kết quả cuối của cả hai lượt khớp ground truth, không có sự kiện bị tính hai lần dù speed job đọc lại từ checkpoint.')

h('6. Nút thắt và cải tiến (E4)')
p(f'<b>(i) Bằng chứng.</b> speed.log của E2 ghi thời gian từng micro-batch: trung bình {vn(collect,3)} s để lấy dữ liệu từ Spark và {vn(write,3)} s để ghi PostgreSQL, tổng khoảng {vn(collect+write,2)} s cho khoảng 195 sự kiện, rất nhỏ so với trigger 2 s. CPU trung bình của container app chỉ khoảng {vn(a(E2B,"cpu_app_mean"),0)}% trên giới hạn 200%. Trong khi đó p95 ở E1 và E2 đều khoảng 2,2 s dù tải chênh 5 lần, và E3 cho thấy speed job xử lý 2.000 sự kiện tồn đọng chỉ trong một micro-batch. Như vậy freshness của B không bị giới hạn bởi khả năng xử lý mà bởi thời gian sự kiện nằm chờ tới lần trigger tiếp theo.')
p('<b>(ii) Giả thuyết.</b> Giảm trigger từ 2 s xuống 1 s sẽ giảm p95 khoảng 1 s, throughput không đổi, CPU tăng nhẹ do số micro-batch tăng gấp đôi. <b>(iii) Thay đổi.</b> Chỉ đổi processingTime của speed job từ 2 s sang 1 s (src/engine.py, cờ improved). Chu kỳ batch 30 s, cách ghi PostgreSQL, khóa duy nhất, dữ liệu, workload E2 và tài nguyên C1 giữ nguyên.')
table([['Chỉ số (trung bình 2 lượt)','E2 – trigger 2 s','E4 – trigger 1 s','Thay đổi']]+[[lab,vn(a(E2B,k),dd),vn(a(E4,k),dd),('+' if a(E4,k)>=a(E2B,k) else '')+vn((a(E4,k)-a(E2B,k))/a(E2B,k)*100,1)+'%'] for k,lab,dd in [('p50','p50 freshness (s)',2),('p95','p95 freshness (s)',2),('throughput','Throughput (sự kiện/s)',2),('backlog_max','Backlog max (sự kiện)',1),('cpu_mean','CPU trung bình (%)',1),('cpu_max','CPU max (%)',1),('ram_mean','RAM trung bình (MiB)',0)]]+[['Số micro-batch có dữ liệu / lượt',vn(nb2,0),vn(nb4,0),'+'+vn((nb4-nb2)/nb2*100,0)+'%']],[170,95,95,80])
figure('03_cai_tien','Hình 3. Trước (E2) và sau (E4) khi giảm trigger của speed job; hai chỉ số hiệu năng và hai chỉ số tài nguyên.',height=128)
p(f'<b>(iv) Kết quả.</b> p95 giảm {vn(change,1)}% (từ {vn(a(E2B,"p95"))} s xuống {vn(a(E4,"p95"))} s), đúng với giả thuyết. Mức giảm lớn hơn nhiều so với chênh lệch giữa hai lượt của cùng cấu hình (p95 E2: 2,39 và 2,09 s; E4: 1,15 và 1,23 s). Throughput giữ ở mức input, backlog max giảm, kết quả COUNT/SUM vẫn đúng. <b>(v) Đánh đổi và khuyến nghị.</b> Số micro-batch và số transaction ghi PostgreSQL tăng gấp đôi; CPU trung bình tăng {vn(a(E4,"cpu_mean")-a(E2B,"cpu_mean"),1)} điểm phần trăm, RAM tăng khoảng {vn(a(E4,"ram_mean")-a(E2B,"ram_mean"),0)} MiB. Mỗi micro-batch có chi phí cố định khoảng 0,05–0,07 s, nên khi trigger tiến gần thời gian xử lý một micro-batch (hoặc khi tải tăng làm micro-batch chậm hơn), giảm thêm trigger sẽ không còn lợi. Ở tải đã thử, nên áp dụng trigger 1 s.')

h('7. Khả năng mở rộng (E5)')
table([['Chỉ số (trung bình 2 lượt)','C1: 2 CPU / 2 GiB','C2: 4 CPU / 4 GiB']]+[[lab,vn(a(C1,k),dd),vn(a(C2,k),dd)] for k,lab,dd in [('throughput','Throughput (sự kiện/s), input 150',2),('p50','p50 freshness (s)',2),('p95','p95 freshness (s)',2),('backlog_max','Backlog max (sự kiện)',0),('cpu_app_mean','CPU container app, trung bình (%)',1),('cpu_app_max','CPU container app, max (%)',1),('ram_mean','RAM ba container, trung bình (MiB)',0)]],[210,130,130])
figure('04_tang_tai_nguyen','Hình 4. E5: so sánh C1 và C2 ở cùng tải 150 sự kiện/s về throughput, p95 freshness, CPU của container app và RAM.',height=125)
p(f'E5 dùng 150 sự kiện/s, cao hơn E2 vì 100 sự kiện/s chưa gây tồn đọng kéo dài; mức này cũng gần hết tập 20.000 sự kiện đã chuẩn bị (150 × 120 = 18.000). Ở 150 sự kiện/s cả hai cấu hình đều theo kịp tải: throughput bằng input, backlog max khoảng 330 sự kiện và không tăng dần theo thời gian, COUNT/SUM đúng. CPU trung bình của app chỉ khoảng {vn(a(C1,"cpu_app_mean"),0)}% trên giới hạn 200% ở C1 và {vn(a(C2,"cpu_app_mean"),0)}% trên giới hạn 400% ở C2, nghĩa là tốc độ phát chứ không phải tài nguyên là giới hạn của phép đo. Tỉ số throughput quan sát được C2/C1 ≈ 1,00 vì vậy không phản ánh năng lực tối đa, nên theo yêu cầu đề bài, Speedup và Scaling Efficiency không được tính. RAM ở C2 cao hơn khoảng {vn(a(C2,"ram_mean")-a(C1,"ram_mean"),0)} MiB chủ yếu do driver memory của Spark được đặt 1 GB thay vì 512 MB; vì C2 tăng cả CPU lẫn RAM nên cũng không thể quy chênh lệch nào chỉ cho CPU. Để đo được S và E cần tăng tải tới khi C1 không theo kịp, việc này đòi hỏi tập dữ liệu lớn hơn 20.000 sự kiện và producer gửi bất đồng bộ, đặt ngoài container app.')

h('8. Kết luận')
p(f'<b>Gần thời gian thực.</b> Với tải 20–150 sự kiện/s, Lambda cho p95 freshness khoảng 2,2 s (1,2 s sau cải tiến E4) và kết quả đúng sau khi bắt kịp ở cả 16 lượt; batch-only cho p95 khoảng 31 s. Nếu yêu cầu là cập nhật trong vài giây thì chỉ B đáp ứng. Q1/Q2 phản hồi nhanh (ở E2 – B, p95 khoảng {vn(a(E2B,"q1_p95"))} ms và {vn(a(E2B,"q2_p95"))} ms, đo trong lúc dữ liệu đang cập nhật), batch mất trung bình {vn(a(E2B,"batch_mean"))} s mỗi lần.')
p('<b>Khi nào Lambda đáng dùng.</b> Lambda hữu ích khi người dùng cần thấy sự kiện mới giữa hai lần batch nhưng vẫn muốn giữ khả năng tính lại toàn bộ từ raw; E3 cho thấy khi speed job lỗi, hệ thống vẫn phục vụ được với độ trễ của batch và tự hồi phục từ checkpoint mà không đếm trùng. Đổi lại B tốn thêm khoảng 25% CPU và 570 MiB RAM ở E2, thêm một job chạy liên tục, checkpoint, bảng speed_events và logic hợp nhất theo H cần được viết cẩn thận. Nếu độ trễ khoảng 30 s chấp nhận được, A đơn giản và rẻ hơn.')
p('<b>Scale-up.</b> Ở tải đã thử, tăng từ C1 lên C2 không cải thiện throughput hay freshness vì C1 chưa bão hòa; kết quả này không đủ để kết luận về khả năng mở rộng của hệ thống, chỉ cho thấy C1 là đủ cho 150 sự kiện/s. <b>Cải tiến E4</b> nên được áp dụng: p95 giảm gần một nửa với chi phí CPU tăng nhỏ. Giới hạn của các kết luận: một máy, hai lượt mỗi cấu hình, freshness bị làm tròn theo chu kỳ poll 0,5 s.')

story.append(PageBreak())
h('Phụ lục A. Schema PostgreSQL và truy vấn Q1/Q2')
story.append(Preformatted(Path('sql/schema.sql').read_text().strip(),styles['code'],maxLineLength=110,newLineChars='  '))
p('Truy vấn dùng để kiểm thử (sql/queries.sql):')
story.append(Preformatted(Path('sql/queries.sql').read_text().strip(),styles['code']))
h('Phụ lục B. Số liệu từng lượt')
table([['Lượt','Rate','p50 (s)','p95 (s)','Thr. (sk/s)','Backlog','CPU tb (%)','RAM tb (MiB)','Kết quả']]+[[r['name'],r['rate'],vn(r['p50']),vn(r['p95']),vn(r['throughput']),r['backlog_max'],vn(r['cpu_mean'],1),vn(r['ram_mean'],0),'đúng' if r['correct'] else 'sai'] for r in rows],[85,32,45,45,55,48,55,60,55],size=8.5)
p('Tên lượt có dạng &lt;thí nghiệm&gt;_&lt;phương án&gt;_C&lt;số lõi của app&gt;_&lt;trigger 1 s: 1, trigger 2 s: 0&gt;_&lt;lượt&gt;. Số liệu thô của từng lượt (timeline, freshness, tài nguyên, thời gian truy vấn, log Spark) nằm trong metrics/&lt;tên lượt&gt;/ của repository.','cap')
SimpleDocTemplate('report/Bao_cao.pdf',pagesize=(595.28,841.89),rightMargin=50,leftMargin=50,topMargin=36,bottomMargin=36,title='Bao cao HW4 - Lambda Architecture',author=HO_TEN).build(story)
