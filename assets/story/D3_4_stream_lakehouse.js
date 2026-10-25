/* D3_4 Streams into the lakehouse, told on one evolving board (engine: story.js), one idea per board.
   The running example is Khalid's order 5043883 (00_intro.js, D3_3): his four courier events arrive in one 30-minute batch,
   batch 177 of Lab 3.4 (offsets 13488–13499, 01:30–02:00 Riyadh on 28 September), which becomes snapshot 177 of the table.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every event, offset, count and size is real: data/raw/events/delivery_events.jsonl replayed exactly as Lab 3.4's solution
   replays it (4 days from 24 Sep 00:00 UTC, 30-minute micro-batches), and the numbers Lab 3.4 and Demo 3.4 print. Times are Riyadh time. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,tool,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={iceberg:'https://iceberg.apache.org/docs/latest/maintenance/',connect:'https://iceberg.apache.org/docs/latest/kafka-connect/',spark:'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',kappa:'https://www.oreilly.com/radar/questioning-the-lambda-architecture/',intro:'00_intro.html',d33:'D3_3_streaming.html',demo:colab('demos/D3_4_stream_lakehouse_demo.ipynb'),lab:colab('D3_4_stream_to_lakehouse.ipynb')};
const H=(...c)=>({c,cls:'h'}),SANS=v=>`<span style="font-family:var(--sans)">${v}</span>`;

// One card per event, as in D3_3. A card's id is its offset in the log, so an event glides from the log into the table.
const TYPES={assigned:'assigned',picked:'picked up',way:'on the way',delivered:'delivered',failed:'failed'};
const evt=(o,t,x,y,id,cls='',w=100)=>label(`<b>…${o}</b><span>${TYPES[t]}</span>`,x,y,`evt ${t} ${cls}`,w,id);
// Batch 177: the events that arrived between 01:30 and 02:00 on 28 September, at offsets 13488–13499: [order, type, arrived].
const B177=[[774,'assigned','01:32'],[883,'assigned','01:36'],[774,'picked','01:36'],[939,'delivered','01:37'],[774,'way','01:37'],[590,'delivered','01:38'],
 [883,'picked','01:46'],[883,'way','01:48'],[774,'failed','01:49'],[775,'assigned','01:50'],[883,'delivered','01:54'],[775,'picked','01:59']];
const bx=i=>56+112*i;
const batch=(times=true)=>svg(outline(40,14,1364,170,'frame','batch'))+B177.map(([o,t,a],i)=>label(String(13488+i),bx(i),34,'off',100,`off${13488+i}`)+evt(o,t,bx(i),58,`ev${13488+i}`)+(times?label(a,bx(i),134,'tick',100,`at${i}`):'')).join('');
const KHALID=[[1,'assigned'],[6,'picked'],[7,'way'],[10,'delivered']];
const snap=(n,x,y,w,body,amount='',cls='copy')=>rec(`s${n}`,x,y,w,`snapshot ${n}`,body,amount,cls);
const tblName=(x,y,w=420,id='tbl-name')=>label('bronze.delivery_events',x,y,'hand teal',w,id);

// The late-data board: deliveries that happened 21:00–22:00 on 25 September, [happened, arrived]. x = 140 + 10 px per minute after 21:00.
const HOUR=[['21:00:09','21:00:10'],['21:00:29','21:00:29'],['21:00:47','21:00:53'],['21:01:53','21:01:59'],['21:02:21','21:02:27'],['21:02:28','21:02:30'],['21:03:49','21:03:51'],['21:03:51','21:03:52'],['21:04:08','21:04:09'],['21:06:18','21:06:19'],['21:06:30','21:06:35'],['21:07:04','21:07:10'],['21:09:56','21:09:59'],['21:10:12','21:10:18'],['21:10:59','21:11:06'],['21:11:46','21:11:51'],['21:12:31','21:12:36'],['21:13:23','21:13:28'],['21:13:46','21:13:52'],['21:14:35','21:14:38'],['21:14:47','21:14:51'],['21:15:09','21:15:14'],['21:16:06','21:16:13'],['21:16:09','21:16:12'],['21:18:01','21:18:01'],['21:19:02','21:19:06'],['21:19:48','21:19:54'],['21:20:59','21:21:02'],['21:22:15','21:22:20'],['21:22:46','21:22:50'],['21:23:28','21:23:29'],['21:23:45','21:23:47'],['21:23:50','21:23:54'],['21:25:12','21:25:18'],['21:25:53','21:26:00'],['21:26:34','21:26:37'],['21:26:54','21:27:01'],['21:32:08','21:32:16'],['21:32:30','21:32:36'],['21:33:10','21:33:14'],['21:35:03','21:35:09'],['21:36:46','21:36:54'],['21:37:51','21:37:58'],['21:38:02','21:38:07'],['21:38:35','21:38:35'],['21:39:47','21:39:51'],['21:39:56','21:40:04'],['21:41:11','21:41:18'],['21:41:34','21:41:38'],['21:41:38','21:41:44'],['21:41:40','21:41:41'],['21:42:52','21:42:56'],['21:44:54','21:45:00'],['21:45:52','21:45:54'],['21:46:07','21:46:09'],['21:48:28','21:48:29'],['21:48:50','21:48:50'],['21:49:11','21:49:14'],['21:52:20','21:52:23'],['21:52:27','21:52:30'],['21:53:31','21:53:35'],['21:54:03','21:54:11'],['21:54:11','21:54:15'],['21:54:19','21:54:25'],['21:55:11','21:55:14'],['21:55:13','21:55:18'],['21:55:13','21:55:20'],['21:56:06','21:56:11'],['21:56:35','22:39:58'],['21:58:26','22:43:11'],['21:59:52','21:59:56']];
const X=s=>{const [h,m,sec]=s.split(':').map(Number);return +(140+10*(h*60+m+sec/60-1260)).toFixed(1);};
const YE=70,YA=290;

// Delivered within 30 minutes, 24–27 September, per store (Lab 3.4 task 4, all of each store's orders), best first.
const SLA=[['DMM-01',78.1],['RUH-02',73.9],['JED-04',73.2],['JED-02',72.9],['RUH-05',72.8],['JED-03',72.8],['RUH-04',72.3],['JED-01',71.2],['RUH-03',69.4],['DMM-02',68.4],['RUH-06',67.6],['RUH-01',37.2]];
const bar=([s,v],y)=>label(`<b>${s}</b><span class="track"><span style="width:${v}%"></span></span><i>${Math.round(v)}%</i>`,0,y,`bar${s==='RUH-01'?' bad':s==='RUH-05'?' mine':''}`,820,`bar-${s}`);

add('Day 3 · Session 4','Streams into the lakehouse','',
svg(path('M172 136 L192 136','em')+path('M420 176 C440 215 470 240 500 262','em'))+art('courier',20,60,150,'Courier','','phone')+KHALID.map(([i,t],k)=>evt(883,t,200+114*k,100,`ev${13488+i}`,'',104)).join('')+art('house',500,215,210,'A table','','f-house'),
'Land the stream in a table you can query, exactly once, and keep it healthy.',
'About 25 minutes for the board, then Demo 3.4 sections 2 and 6 live if there is time, then Lab 3.4 (25 minutes). This session joins the last two: the courier events from D3_3 and the Iceberg tables from D3_1. Khalid’s order 5043883 runs through the board again: his four courier events arrive in one 30-minute batch and become one commit of the table. The hard part is the seam between the stream and the table: saving the data and the stream position together, and the small files a stream leaves behind. Many boards are builds: the heading stays and one piece arrives per click. Every number comes from the course data and Lab 3.4. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['d33','intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 3</p><h1>Streams into<br><span class="teal">the lakehouse</span></h1><p class="lead">Khalid’s four events,<br>one commit, a table you can query.</p>'});

add('00 / You are here','A stream that lands in a table','Two stages you know, joined: the events from D3_3 land in the Iceberg tables from D3_1.',
platform(n=>n==='Streaming'||n==='Iceberg'?'fresh':'met',{Streaming:'Courier events, as they arrive',Iceberg:'The table they land in'}),
'<strong>The seam is the hard part:</strong> the stream never stops, and the table must stay correct and fast.',
'The course map with two pieces lit. D3_3 read the events from a log; D3_1 built Iceberg tables with snapshots, partitions and time travel. Today the stream writes into such a table. Everything downstream (dbt, the semantic layer, dashboards) then reads one table instead of a log.');

const QT='Operations asks: which store keeps missing 30 minutes?',QS='Not one order on the road, as in D3_3: every order of the week, per store.';
const question=()=>art('operations',0,20,200,'Operations','“Which store keeps missing the promise?”','f-ops')+
 rec('need-ev',300,10,560,SANS('when each order was delivered'),'the delivered events · from the stream','','chg')+rec('need-ord',300,100,560,SANS('when each order was placed'),'created_at · app.orders','','src')+rec('need-sql',300,190,560,SANS('every store, four days'),'a join and a GROUP BY','','')+
 note('Khalid’s order: placed 01:35:37, delivered 01:54:55. Now do that for 3,157 orders.',920,40,'teal',520);
add('01 / The question',QT,QS,question(),
'The answer joins <strong>two sources</strong> over <strong>many days</strong>: a query, not a reader of a log.',
'A build: two boards. The promise from the introduction is 30 minutes from order to door. To find the store that misses it, operations needs each order’s delivered event (from the courier stream) and its order time (from the app database), for every order of the week. Khalid’s order is one row of that answer: placed at 01:35:37, delivered at 01:54:55. 3,157 orders were delivered between 24 and 27 September (UTC days).',['intro']);

add('01 / The question',QT,QS,question()+
 label('<b style="color:#b96355">The log can’t answer it</b><br>read one event at a time, in arrival order · kept for days · no joins',0,360,'reason',680,'log-no')+label('<b>A table can</b><br>SQL and joins from any engine · history kept as long as you like',760,360,'reason',680,'tbl-yes'),
'<strong>Land the stream in a table:</strong> the stream keeps it fresh, the table makes it queryable.',
'Kafka is built to hand events to readers in order, not to answer questions: no joins with the app’s orders, no GROUP BY, and it keeps events for days. An Iceberg table is what D3_1 built: SQL from DuckDB, Spark or Polars, snapshots and time travel. So the stream job’s output is a table: bronze.delivery_events, one row per event.',['d33']);

add('02 / Micro-batches','Every 30 minutes, append what arrived','The stream job takes everything that arrived in the last 30 minutes and adds it to the table.',
batch()+svg(path('M720 186 L720 278','em','w-down'))+label('batch 177 · arrived 01:30–02:00',740,196,'hand teal',600,'batch-name')+
tool('Stream job','every 30 minutes: take what arrived, append it',0,300,440,'t-job')+box('bronze.delivery_events','<p>an Iceberg table,<br>partitioned by day</p>',500,290,440,'silver','tbl-events')+note('4 days of the stream: 179 batches, 1 to 208 events each',1000,300,'',440),
'<strong>Micro-batch:</strong> a small batch of whatever arrived in one interval. The stream becomes a series of small appends.',
'The real batch 177 of Lab 3.4: the 12 events that arrived between 01:30 and 02:00 on 28 September, at log offsets 13488 to 13499 (D3_3 showed most of them). Khalid’s four events (bright) are all in it, with eight events of four other orders. Spark Structured Streaming works exactly like this (a trigger every 30 minutes); Flink commits on its checkpoints, typically every minute or so. A shorter interval means fresher data: keep that thought for the small-files boards.',['spark','lab']);

add('02 / One commit','Each batch is one commit','Batch 177 becomes snapshot 177 of the table: the same idea as every write in D3_1.',
batch(false)+svg(path('M720 186 L720 262','em','w-down')+outline(300,250,840,180,'teal','tbl-encl'))+label('batch 177',740,196,'hand teal',300,'batch-name')+tblName(330,262)+
snap(176,330,330,250,'01:00–01:30 · 13 rows')+snap(177,600,330,250,'01:30–02:00 · 12 rows')+snap(178,870,330,250,'02:00–02:30 · 3 rows')+
note('One batch = one commit = one snapshot',0,300,'teal',280)+note('Readers see all 12 rows of 177, or none of them',1170,300,'',270),
'<strong>Every micro-batch is an atomic commit:</strong> a new snapshot that holds the whole batch.',
'Recall D3_1: every write to an Iceberg table adds files and atomically switches the table to a new snapshot. A reader that runs during the commit sees snapshot 176 or snapshot 177, never half of batch 177. Over the 4 days the job makes 179 commits: 179 snapshots. The snapshot numbers on the board are positions in the table’s history; Iceberg’s own snapshot ids are long random numbers.',['lab']);

const CT='The job must remember where it stopped',CS='The offset from D3_3: the next position in the log to read. A first design saves it in a file after each commit.';
const ckpt=v=>rec('ckpt',800,330,640,'checkpoint.json','a separate file: the next offset to read',v,'chg');
const twoSteps=()=>batch(false)+svg(path('M520 186 C520 220 470 235 450 248','em','w-1')+path('M1200 186 C1200 240 1180 290 1170 322','em','w-2')+outline(0,250,660,180,'teal','tbl-encl'))+tblName(30,262)+snap(176,30,330,290,'01:00–01:30 · 13 rows')+snap(177,340,330,290,'01:30–02:00 · 12 rows')+
 label('<b>1 · commit</b> batch 177 to the table',0,196,'reason',440,'step1')+label('<b>2 · save</b> the next offset: 13500',740,196,'reason',420,'step2');
add('03 / Where did it stop?',CT,CS,twoSteps()+ckpt('13500'),
'<strong>Offset:</strong> the job’s place in the log. After batch 177 (offsets 13488–13499), the next offset is 13500.',
'This is the naive sink of Demo 3.4 section 2. After each batch the job does two writes: first the data (a commit to Iceberg), then the progress (offset 13500 into checkpoint.json). On restart it reads the file and continues from there. Before batch 177 the file said 13488. Each write is correct on its own. Ask the room what could go wrong.',['demo']);

add('03 / Where did it stop?','It crashes between step 1 and step 2. Where does it restart?','Batch 177 is in the table. checkpoint.json still says 13488.',
art('engineer',1120,30,250,'The stream job','“Where was I?”','f-job')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>At 13500: the table already has batch 177</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>At 13488: batch 177 is appended a second time</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Iceberg rolls the commit back, so nothing repeats</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: which number does the restart read?</p>',
'A restart trusts <strong>the saved offset</strong>, whatever the table already holds.',
'Allow 45 seconds and let learners commit. B is right: the job reads 13488 from the file and processes batch 177 again; the table has no idea it is the same batch, so it appends it as a new snapshot. C is the common misconception: the commit had finished, and Iceberg never undoes a finished commit. A would need the job to look at the table, which this design doesn’t do. Demo 3.4 section 2 runs this crash live (at batch 30 over two days).',['demo'],
{quiz:{answer:'B',right:'Yes. The file still says 13488, so batch 177 is read and appended again.',wrong:'Look at what checkpoint.json says after the crash. Try again.'}});

add('03 / Where did it stop?','Khalid’s order is now in the table twice','The restart read 13488 and appended batch 177 again, as a new snapshot.',
svg(outline(0,0,640,330,'teal','tbl-encl'))+tblName(30,12)+snap(176,30,70,580,'01:00–01:30 · 13 rows')+snap(177,30,155,580,'01:30–02:00 · 12 rows')+snap(178,30,240,580,'batch 177 again · 12 rows','','dup')+
 label('snapshot 177',720,40,'hand muted',300,'k-177')+KHALID.map(([i,t],k)=>evt(883,t,720+120*k,80,`ev${13488+i}`,'',104)).join('')+
 label('snapshot 178',720,190,'hand red',300,'k-178')+KHALID.map(([i,t],k)=>evt(883,t,720+120*k,230,`dup${i}`,'dup',104)).join('')+
 label('<b>13,518</b>rows in the table',0,380,'answer',300,'n-rows')+label('<b>13,506</b>events in the stream',340,380,'answer',300,'n-events')+label('<b>12 extra</b>batch 177, twice: Khalid delivered twice',680,380,'answer',560,'n-extra'),
'<strong>Two separate writes leave a gap.</strong> A crash in the gap replays the batch: duplicates.',
'Real numbers from replaying the 4 days with this naive job and a crash right after batch 177’s commit: 13,518 rows for 13,506 events, exactly 12 too many, and order 5043883 has 8 rows for its 4 events. The SLA query would now count Khalid’s delivery twice. Making the file write faster doesn’t help: the gap is still there. Saving the offset first doesn’t help either: then a crash loses the batch (at-most-once, D3_3).',['demo','d33']);

add('04 / The fix','Put the offset inside the commit','An Iceberg snapshot carries a summary of free-form properties. One commit can hold the data and the progress.',
label('<b style="color:#b96355">Two writes</b> · a gap between them',0,0,'reason',640,'wr-h')+rec('wr-data',0,60,640,'1 · commit to the table','snapshot 177 · 12 rows','','copy')+label('💥 a crash here',200,150,'ptr red',240,'wr-crash')+rec('wr-file',0,200,640,'2 · write checkpoint.json','offset 13500','','chg')+
 note('The data is saved, the offset isn’t: the batch is replayed.',0,300,'red',640)+
 label('<b>One commit</b> · no gap',760,0,'reason',680,'ok-h')+tbl('snap177',760,60,680,'snapshot 177 · one commit','200px 1fr',[H('in the commit','value'),['12 data rows',SANS('Khalid’s 4 events + 8 others')],['qafila.offset','13500'],['qafila.batch_end','2026-09-27T23:00:00+00:00']],'mono')+
 note('Both are saved, or neither is.',760,250,'teal',680)+label('tbl.append(rows, snapshot_properties={"qafila.offset": "13500"})',0,410,'code',1440,'code-append'),
'<strong>Exactly once into a table:</strong> commit the data and the offset together, in one atomic commit.',
'Wrong and right on one board. The left is the naive job. The right is what Lab 3.4 task 1 builds: append the batch with snapshot_properties, so the offset (and the batch’s end time, 23:00 UTC = 02:00 Riyadh) are part of snapshot 177’s summary. A commit is atomic, so there is no moment where the data exists without the offset. Spark does the same with its checkpoint folder tied to the Iceberg or Delta commit id; Flink uses a two-phase commit. Different code, same idea.',['lab','spark']);

add('04 / The fix','On restart, ask the table','Lab 3.4 kills the job after 40 batches. The restart reads the offset from the newest snapshot that has one.',
snap(40,0,40,400,'batch ending 01:00 · 25 Sep','3693')+label('💥 killed',430,58,'ptr red',120,'kill')+svg(path('M560 75 L660 75','em','w-restart'))+
 rec('restart',670,40,360,'restart','newest snapshot with an offset','3693','chg')+svg(path('M1040 75 L1080 75','em','w-on'))+rec('s-rest',1090,40,350,'snapshots 41 … 179','the other 139 batches','','copy')+
 label('def committed_offset(tbl):<br>&nbsp;&nbsp;&nbsp;&nbsp;for snap in reversed(tbl.snapshots()):<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;off = snap.summary.additional_properties.get("qafila.offset")<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if off is not None: return int(off)<br>&nbsp;&nbsp;&nbsp;&nbsp;return 0',0,190,'code',900,'code-off')+
 note('Compaction commits (later today) carry no offset: skip them.',960,200,'',480)+label('<b>0</b>batches written twice',0,400,'answer',300,'n-twice')+label('<b>0</b>batches skipped',340,400,'answer',300,'n-skip'),
'<strong>The table is the checkpoint.</strong> Whatever the table holds, it also knows how far it got.',
'Lab 3.4 task 2: the job is killed after its 40th batch, the one that ended at 01:00 on 25 September; snapshot 40 says offset 3693. The restart walks the snapshots backwards, finds 3693 and continues with batch 41: nothing written twice, nothing skipped. The loop skips snapshots without an offset because maintenance commits, like the compaction later in this session, don’t carry one.',['lab']);

add('04 / Repeats','The phones still send repeats','D3_3’s retries are in the log too. Keep the event_ids of the last two batches, and skip any id seen again.',
rec('rep1',0,10,660,'ev-0013926 · arrived 01:07:34','order 5043638 · picked up 01:07:31 · new: write it','+1','copy')+rec('rep2',0,100,660,'ev-0013926 · arrived 01:08:34','the same event, 60 s later · seen: skip it','+0','dup')+
 box('Seen ids','<p>the event_ids of the last<br>two batches: one hour</p>',760,0,680,'silver','seen','repeats arrive within 90 s')+
 label('<b>13,506</b>events arrived',0,300,'answer',300,'n-in')+label('<b>190</b>repeats skipped',340,300,'answer',300,'n-rep')+label('<b>13,316</b>rows, all distinct ✓',680,300,'answer',360,'n-out')+
 note('A crash, a restart and the phones’ repeats: every event exactly once.',0,420,'teal',1440),
'<strong>Offset in the commit + skip repeated ids</strong> = every event in the table exactly once.',
'A real repeat from batch 176: courier CR-DAM-015’s phone sent picked_up for order 5043638 (Dammam) at 01:07:34 and again at 01:08:34 with the same event_id. The offset in the commit stops the job’s own replays; the phones’ repeats need a key, as in D3_3. The memory is bounded: over these 4 days every repeat arrived within 90 seconds of the first copy, so the ids of the last two 30-minute batches are enough. Spark calls this dropDuplicatesWithinWatermark. Lab 3.4 proves the result: 13,316 rows, 13,316 distinct event_ids, the same as the distinct ids in the stream.',['lab','spark']);

// The table's day folders (UTC days), one small square per data file. Compaction draws each day as one file of the same total length.
// [day, files before, average kB before, kB after]: Lab 3.4 task 3. Batch 177's file is the 43rd of the 45 on 27 September.
const DAYS=[['2026-09-24',42,5.1,77],['2026-09-25',46,5.0,82],['2026-09-26',46,4.7,72],['2026-09-27',45,4.7,73]];
const folders=(compacted=false)=>label('bronze/delivery_events/data/',0,0,'code',700,'data-dir')+DAYS.map(([d,n,kb,after],i)=>label(`event_day=${d}/`,0,58+72*i,'code',320,`day${i}`)+
 label(compacted?`<span style="width:${14*n-2}px"></span>`:[...Array(n).keys()].map(k=>`<span${i===3&&k===42?' class="k"':''}></span>`).join(''),330,50+72*i,`files${compacted?' one':''}`,660,`files${i}`)+
 label(compacted?`1 file · ${after} kB`:`${n} files · ${kb.toFixed(1)} kB each`,1010,58+72*i,'caption',300,`cnt${i}`)).join('');
const ST='Every commit adds a file',SS='Each batch writes its rows as a new Parquet file. Nothing merges them.';
add('05 / Small files',ST,SS,
folders()+label('▲ batch 177’s file: Khalid’s batch, 12 rows, 3.6 kB',650,320,'ptr sla',520,'k-file')+
 label('<b>179</b>data files in 4 days',0,380,'answer',300,'n-files')+label('<b>4.9 kB</b>per file, on average',340,380,'answer',300,'n-size')+
 note('Engines like files of 100 MB or more. Every file costs an open, a footer read and a storage request.',700,370,'',740),
'<strong>The small-files problem:</strong> thousands of tiny files make every read slow. A stream creates it by design.',
'Lab 3.4 task 3, before compaction: 179 data files, one per batch, 4.9 kB on average, 873 kB in all. Each square is one real file; the teal one is snapshot 177’s file, Khalid’s 12-row batch in 3.6 kB. The day folders are UTC days, so Khalid’s events (01:36 Riyadh on 28 September) sit in event_day=2026-09-27. Each file means another object to open and another footer to read (Parquet, D1_2), and on object storage every request is billed. Shorter batches make it worse: fresher data, more files. At Qafila’s real scale, a 1-minute trigger would write 1,440 files a day per partition touched.',['lab']);

add('05 / Compaction','Compaction: rewrite each day as one file','One more commit: read the day’s rows, write them back sorted by time, in one file. Same rows.',
folders(true)+
 label('<b>179 → 4</b>data files',0,380,'answer',300,'n-files')+label('<b>4.9 → 76 kB</b>average file',340,380,'answer',300,'n-size')+label('<b>0.5 → 0.02 s</b>full table scan',700,380,'answer',340,'n-scan')+label('<b>13,316 = 13,316</b>rows: nothing changed',1080,380,'answer',360,'n-same'),
'<strong>Compaction:</strong> rewrite many small files into a few large ones. The data doesn’t change; reads get much faster.',
'Lab 3.4 task 3: for each day, scan its rows, sort by event time and overwrite the day with one file. Each day’s row of squares becomes one long file holding the same bytes. 179 files become 4, the average file grows from 4.9 kB to 76 kB, and a full scan on a laptop goes from about 0.5 s to 0.02 s (your timings will differ; the ratio is what matters). The row count is checked to be identical. Compaction is a commit too: a new snapshot with fewer, bigger files. In production nobody hand-rolls this: Spark’s rewrite_data_files procedure, or a managed service, does it on a schedule.',['lab','iceberg']);

add('05 / Housekeeping','Three chores keep a streaming table healthy','Run them on cold partitions (yesterday’s), so they don’t race the stream job.',
box('Compact','<p>rewrite small files<br>into large ones</p>',0,0,440,'silver','ch-compact','rewrite_data_files')+box('Expire snapshots','<p>drop old snapshots:<br>48 new ones a day</p>',500,0,440,'silver','ch-expire','expire_snapshots')+box('Remove orphan files','<p>delete files that no<br>snapshot points to</p>',1000,0,440,'silver','ch-orphans','remove_orphan_files')+
 note('Compaction and the stream job both commit. If they touch the same day at the same moment, one of them retries.',0,280,'',1440)+note('S3 Tables, Databricks and Snowflake run these for you.',0,400,'teal',1440),
'<strong>Maintenance:</strong> compact, expire snapshots, remove orphan files. Otherwise reads slow down and storage only grows.',
'One 30-minute stream makes 48 snapshots a day; after a month, time travel reaches back through 1,440 of them and every old file is still on disk. Expiring snapshots (D3_1’s stretch task) drops old versions; removing orphan files deletes data files that no snapshot references any more, such as leftovers of failed writes and expired snapshots. Each chore is itself a commit, so it can conflict with the stream job: Iceberg’s optimistic concurrency makes the loser retry, and scheduling the chores on yesterday’s partitions avoids most conflicts.',['iceberg']);

add('06 / The answer','The question, straight off the table','DuckDB reads the Iceberg table and joins the app’s orders. Khalid’s order first.',
tbl('q-ev',0,0,620,'bronze.delivery_events<span class="src">Iceberg</span>','130px 140px 1fr',[H('order_id','event','event_ts'),['5043883','delivered','28 Sep 01:54:55']],'mono')+tbl('q-ord',0,150,620,'app.orders<span class="src">app database</span>','130px 140px 1fr',[H('order_id','store','created_at'),['5043883','RUH-05','28 Sep 01:35:37']],'mono')+
 svg(path('M630 60 C680 60 690 120 730 120','em','w-q1')+path('M630 210 C680 210 690 140 730 140','em','w-q2'))+rec('q-khalid',740,90,700,'order 5043883 · RUH-05','01:35:37 → 01:54:55','19.3 min ✓','copy')+
 label('SELECT o.store_id,<br>&nbsp;&nbsp;avg((d.delivered_ts - o.created_at <= INTERVAL 30 MINUTE)::INT) AS sla_rate<br>FROM delivered d&nbsp;&nbsp;&nbsp;&nbsp;-- the delivered events, from iceberg_scan(…)<br>JOIN app.orders o USING (order_id)<br>GROUP BY o.store_id',0,320,'code',1440,'code-sla'),
'<strong>The stream became a table:</strong> one SQL query joins it with the app’s orders.',
'Lab 3.4 task 4, simplified: DuckDB’s iceberg_scan reads the table’s current snapshot, the delivered events are joined with app.orders on order_id, and each order’s minutes from created_at to the delivered event are compared with 30. Khalid’s order took 19.3 minutes: inside the promise (the same 19.3 minutes as his delivery row in D2_2). The app stores created_at in UTC without a zone, so the lab converts it before subtracting.',['lab']);

add('06 / The answer','Olaya (RUH-01) keeps only 37% of its promises','Share of each store’s orders delivered within 30 minutes, 24–27 September.',
SLA.map((s,i)=>bar(s,i*40)).join('')+art('operations',1000,20,170,'Operations','','f-ops')+note('RUH-01 had 662 orders, more than twice any other store. Its pickers can’t keep up.',920,250,'red',520)+note('Khalid’s store, RUH-05: 73%',920,400,'teal',520),
'<strong>Near real time:</strong> operations can see this during the day, not in tomorrow’s report.',
'Lab 3.4 task 4’s check asks which store is struggling: RUH-01 (Olaya) delivers only 37% of its orders within 30 minutes, against 68–78% everywhere else. It is also by far the busiest store (662 delivered orders; the next is DMM-01 with 306): the data generator made it about 1.6 times busier, and its median delivery takes 32 minutes. Khalid’s store, RUH-05, is in the pack at 73%. Because the table is at most 30 minutes behind, the operations lead sees this at 10:00 and can send extra pickers today.',['lab']);

const lateLines=()=>HOUR.map(([t,a],i)=>path(`M${X(t)} ${YE+6} L${X(a)} ${YA-6}`,a>'22'?'danger':'thin',`ln${i}`)).join('');
add('07 / Late data','The 22:00 dashboard said 69. The table now says 71.','Deliveries in the hour 21:00–22:00 on 25 September. Two arrived after the dashboard read the table.',
svg(outline(140,YE-56,600,44,'silver','hour')+path(`M140 ${YE} L1340 ${YE}`,'axis','ax-e')+path(`M140 ${YA} L1340 ${YA}`,'axis','ax-a')+lateLines()+path(`M740 ${YA-30} L740 ${YA+46}`,'danger dashed','dash'))+
 label('the hour 21:00–22:00',150,YE-50,'tick',580,'hour-l')+label('Happened',0,YE-20,'hand teal',130,'ax-e-t')+label('Arrived',0,YA-20,'hand muted',130,'ax-a-t')+
 ['21:00','21:30','22:00','22:30','23:00'].map((t,i)=>label(t,X(t+':00')-40,YA+10,'tick',80,`ta${i}`)).join('')+label('dashboard reads · 22:00',750,YA+40,'ptr red',260,'dash-l')+
 note('5041914: 21:56 → 22:39<br>5041278: 21:58 → 22:43',1030,322,'red',410)+
 rec('as-then',0,420,680,'scan(snapshot_id = the one current at 22:00)','what the dashboard saw','69','wrong')+rec('as-now',760,420,680,'scan() · the current snapshot','what the table says now','71','copy'),
'<strong>The newest hour is always a little short.</strong> Time travel shows what each reader saw; say which version each consumer gets.',
'Lab 3.4’s stretch task. Each line joins when a delivery happened (top) to when it arrived (bottom): almost all are vertical. Two are late: order 5041914 (JED-01) delivered at 21:56, arrived at 22:39; order 5041278 (RUH-01) delivered at 21:58, arrived at 22:43, phones that were offline for 43 and 45 minutes. The dashboard read the table at 22:00 and counted 69; both late events landed in the 22:30–23:00 batch, so the table now says 71. Neither number is wrong: scanning the snapshot that was current at 22:00 reproduces 69 exactly. Operations is fine with the live number; finance should read yesterday’s hours the next morning. Write that down in the data contract (Day 4).',['lab','d33']);

add('08 / Downstream','The table is a stream too','The silver job remembers the last snapshot it read, like a consumer offset, and asks only for what was added since.',
snap(179,0,20,330,'the last batch of the 4 days','')+rec('s-compact',360,20,330,'snapshots 180–187','compaction: rows moved, none added','','stale')+label('▲ silver read up to 187',360,105,'ptr saved',330,'ptr-silver')+rec('s-new',720,20,400,'snapshots 188–230','one more day: 43 batches','+2,962','chg')+
 svg(path('M920 110 C920 170 920 190 920 220','em','w-inc'))+box('Silver job','<p>reads only the rows added after 187</p>',720,230,400,'silver','silver-job','2,962 rows written · 2,962 read')+
 label('tbl.incremental_append_scan(<br>&nbsp;&nbsp;from_snapshot_id_exclusive=last_seen)',0,250,'code',660,'code-inc'),
'<strong>Incremental read:</strong> the rows appended since a given snapshot. Jobs chain through tables instead of rescanning them.',
'Lab 3.4 task 5. Iceberg records what each snapshot appended, so a downstream job keeps its own “offset”: the last snapshot it processed. After the compaction the table is at snapshot 187; the stream runs one more day (43 batches, snapshots 188 to 230, 2,962 new rows), and the incremental read returns exactly those 2,962 rows. Compaction snapshots add no new rows, so incremental appends skip them. This is how Flink and Spark stream from Iceberg tables, and how dbt incremental models avoid full rebuilds.',['lab']);

const LT='Lambda: two paths for one number',LS='A fast path for now, a nightly batch for the truth. Queries merge the two.';
add('09 / Architectures',LT,LS,
svg(path('M265 240 C310 240 320 100 375 100','em','w-l1')+path('M265 300 C310 300 320 400 375 400','em','w-l2')+path('M705 100 C750 100 770 220 815 230','em','w-l3')+path('M705 400 C750 400 770 290 815 280','em','w-l4')+path('M1125 255 L1215 255','em','w-l5'))+
 box('Log','<p>Kafka<br>kept for days</p>',0,170,260,'','lam-log')+box('Speed job','<p>stream: fast,<br>approximate</p>',380,0,320,'silver','lam-speed')+box('Batch job','<p>nightly: slow,<br>exact</p>',380,300,320,'gold','lam-batch')+box('Merged view','<p>today from speed,<br>older days from batch</p>',820,155,300,'','lam-view')+art('operations',1230,150,160,'Operations','','f-ops')+
 note('Every metric is written twice. Fix a bug in one path, and the other still has it.',820,400,'red',620),
'<strong>Lambda:</strong> a speed layer and a batch layer, merged at query time. Two code paths that drift apart.',
'The classic answer from the 2010s, when streams were thought to be approximate. The batch layer recomputes the truth every night from the stored events; the speed layer covers the hours since; queries merge the two. It works, but every metric exists twice, often in two languages (a Flink job and a SQL model), and the two drift: the live delivery count and the nightly count disagree, and nobody knows which to trust.',['kappa']);

add('09 / Architectures','Kappa: one path. To fix it, replay the log.','The naive table had Khalid twice. Fix the job, replay the log from offset 0 into a new table.',
svg(path('M265 250 L405 250','em','w-k1')+path('M715 230 C750 230 760 175 785 175','em','w-k2'))+box('Log','<p>13,506 events<br>kept for days</p>',0,150,260,'','lam-log')+box('Fixed job','<p>offset in the commit<br>skip repeats</p>',410,150,300,'silver','kap-job')+label('▶ from offset 0',268,212,'ptr saved',140,'kap-replay')+
 rec('kap-new',790,140,440,'delivery_events_v2','replayed with the fixed job','13,316','copy')+rec('kap-old',790,280,440,'delivery_events','the naive table: Khalid twice','13,518','wrong')+art('operations',1275,130,150,'Readers','switch to v2','f-ops')+
 note('Needs a log kept long enough to replay, and a table you can rebuild and swap.',0,420,'teal',1440),
'<strong>Kappa:</strong> one streaming path. A bug fix or a new metric means replaying the retained log through the new job.',
'Demo 3.4 section 6 does exactly this: the naive table (with a replayed batch and the phones’ repeats) is not patched; the fixed logic replays the retained log from offset 0 into a new table, and readers switch to it. On these 4 days that is 13,316 rows instead of 13,518. One code path, and the log is the source of truth. The catch: the log must be kept long enough (Kafka retention or tiered storage) and the output must be replaceable.',['demo','kappa']);

add('09 / Architectures','What most teams run in 2026','Stream into bronze. Run the same dbt models on it every 5 to 15 minutes.',
svg(path('M115 170 L140 170','em','w-m0')+path('M345 170 L385 170','em','w-m1')+path('M635 170 L675 170','em','w-m2')+path('M915 170 L955 170','em','w-m3')+path('M1195 170 L1235 170','em','w-m4'))+
 art('courier',0,100,110,'','','phone')+box('Kafka','<p>the log</p>',145,70,200,'','mg-kafka')+box('Stream job','<p>offset in the commit</p>',390,70,245,'silver','mg-job')+box('bronze','<p>Iceberg<br>every 30 min</p>',680,70,235,'bronze','mg-bronze')+box('dbt','<p>the same models<br>every 15 min</p>',960,70,235,'silver','mg-dbt')+box('silver · gold','<p>dashboards<br>and SQL</p>',1240,70,200,'gold','mg-gold')+
 label('<b>One code path</b><br>the dbt models from D2_3',0,330,'reason',440,'mg-one')+label('<b>Minutes, not seconds</b><br>enough for operations',500,330,'reason',440,'mg-fresh')+label('<b>Late data</b><br>lands in the next run',1000,330,'reason',440,'mg-late')+
 note('Seconds matter (a fraud check)? Keep that one metric in the stream job.',0,440,'',1440),
'<strong>The middle ground:</strong> stream into a bronze table, then run batch models on it often. One path, near real time.',
'Most of Qafila’s questions need minutes, not seconds: this design gives that with the tools learners already have. The stream job only lands events in bronze (exactly once, offset in the commit); dbt models (D2_3) run every 5–15 minutes with incremental reads of the new snapshots and build silver and gold. Late events simply land in bronze and are picked up by the next run. Only the few metrics that need seconds, like the fraud check from D3_3, stay in a stream job.',['lab']);

add('10 / The tools','The same pattern, ready-made','Everything on today’s board exists as a product or a procedure.',
'<table class="mapping compact"><thead><tr><th>On the board</th><th>In production</th></tr></thead><tbody><tr><td>micro-batches with the offset in the commit</td><td>Spark Structured Streaming (<code>checkpointLocation</code>) · Flink (two-phase commit) · Kafka Connect Iceberg sink</td></tr><tr><td>a topic that is also a table</td><td>Confluent Tableflow · Redpanda Iceberg topics</td></tr><tr><td>compaction, expiry, orphan files</td><td><code>rewrite_data_files</code> · <code>expire_snapshots</code> · managed: S3 Tables, Databricks, Snowflake</td></tr><tr><td>incremental reads</td><td>Flink and Spark streaming reads · dbt incremental models</td></tr></tbody></table>',
'<strong>You built each piece by hand in the lab;</strong> in production you configure them.',
'Map each row to its board. The Kafka Connect Iceberg sink lands topics as tables with no custom code and commits offsets with the data. Confluent Tableflow and Redpanda go further: the topic itself can be read as an Iceberg table. Spark’s writeStream with format("iceberg"), a 30-minute trigger and a checkpointLocation is Lab 3.4’s job in five lines (Demo 3.4 section 7 shows it).',['connect','spark','iceberg','demo']);

const WORDS=[['Micro-batch','what arrived in one interval'],['Commit','one atomic change: a snapshot'],['Offset in the commit','data and progress, saved together'],['Exactly once','no repeats, no gaps in the table'],['Seen ids','skip repeats from the phones'],['Small files','many tiny files: slow reads'],['Compaction','rewrite small files into large ones'],['Snapshot expiry','drop old versions of the table'],['Orphan files','files no snapshot points to'],['Incremental read','rows added since a snapshot'],['Lambda','a speed path and a batch path'],['Kappa','one path; fix by replaying the log']];
add('11 / The words','Twelve words for streams into tables','Every one appeared on a board today. Lab 3.4 uses them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>One commit for data and progress, a few big files, and a table others can read as a stream:</strong> the rest follows.',
'A reference board: point at each word and ask the room which board it appeared on, and what happened to Khalid’s batch there. Learners can come back to it during the lab.');

const RECALL=[
 ['The job commits batch 177, crashes before it writes 13500 to checkpoint.json, and restarts. What is in the table?','Batch 177 twice: the file still says 13488. Khalid’s four events have 8 rows; 12 rows too many.'],
 ['How does the fixed job know where to restart?','It reads qafila.offset from the newest snapshot that has one: the offset is part of the same commit as the data.'],
 ['179 batches left 179 files of 4.9 kB. What fixes it, and does it change the data?','Compaction: rewrite each day as one file (4 files, 76 kB). Same 13,316 rows, in a new snapshot.'],
 ['The 22:00 dashboard showed 69 deliveries for 21:00–22:00; the table now says 71. Which is wrong?','Neither: two deliveries arrived at 22:39 and 22:43. Time travel reproduces the 69. Say which version each team gets.'],
 ['The stream job had a bug. Patch the table with a batch job, or…?','Kappa: fix the job and replay the retained log from offset 0 into a new table. One code path.']];
add('12 / Recall','Your turn: answer before you look','Five questions on Khalid’s batch and the table it landed in. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer. The next slide fills in all five answers in place.');

add('12 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>Offset in the commit, compaction, time travel and replay:</strong> the whole session in one line.',
'Go back to the board for any answer the room missed: the crash and the duplicate batch (7–9), the offset in the commit (10–11), small files and compaction (13–14), late data (18), Kappa (21).');

add('Lab 3.4 · 25 min','Streams into the lakehouse','A crash-proof stream job into Iceberg, compaction, and the SLA query straight off the table.',
`<div class="references"><div><h3>Lab 3.4 · 25 minutes</h3><p>1 · A bronze table; the offset in the commit · 6′</p><p>2 · Crash, restart, no duplicates · 5′</p><p>3 · Small files and compaction · 5′</p><p>4 · The SLA query, straight off the table · 5′</p><p>5 · Incremental reads for the next job · 4′</p><p class="note">✱ Time travel the dashboard: late data rewrites the last hour (69 → 71). Find Khalid’s batch in your table: the snapshot whose qafila.offset is 13500.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D3_4_stream_to_lakehouse.ipynb</a><a href="${sources.demo}" target="_blank" rel="noopener">Demo 3.4 · the board, run on the data</a><a href="${sources.kappa}" target="_blank" rel="noopener">Further reading · Questioning the Lambda architecture</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next: Day 4 · orchestration. The stream job and the chores run on a schedule.',
'Hand over to the lab. A quick first check after task 2: the snapshot whose qafila.offset is 13500 holds 12 rows, Khalid’s batch. If you skipped Demo 3.4 sections 2 and 6 live, point learners to them: they run the naive crash and the Kappa replay on the real data.',['lab','demo']);

Story.start({brand:'QAFILA / STREAM TO LAKEHOUSE',sources});
})();
