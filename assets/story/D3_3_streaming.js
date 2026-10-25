/* D3_3 Streaming fundamentals, told on one evolving board (engine: story.js), one idea per board.
   The running example is Khalid's order 5043883 from the course introduction (00_intro.js): four courier events on 28 September.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every event, time and count is real: data/raw/events/delivery_events.jsonl in arrival order, keyed by order_id into
   Lab 3.3's 4-partition log (crc32 % 4), and the numbers Lab 3.3's solution prints. Times are Riyadh time. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,tool,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={streaming101:'https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/',kafka:'https://kafka.apache.org/documentation/#design',flink:'https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/',spark:'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',intro:'00_intro.html',demo:colab('demos/D3_3_streaming_demo.ipynb'),lab:colab('D3_3_streaming_fundamentals.ipynb')};
const K=v=>`<em class="k">${v}</em>`,BAD=v=>`<em class="k bad">${v}</em>`;
const H=(...c)=>({c,cls:'h'}),SANS=v=>`<span style="font-family:var(--sans)">${v}</span>`;

// One card per event. Cards keep their id, so an event glides from the phone into the log, into its partition and into a crash.
const TYPES={assigned:'assigned',picked:'picked up',way:'on the way',delivered:'delivered',failed:'failed'};
const eid=(o,t)=>`e${o}${t}`;
const evt=(o,t,x,y,cls='',id=eid(o,t))=>label(`<b>…${o}</b><span>${TYPES[t]}</span>`,x,y,`evt ${t} ${cls}`,104,id);
const ghost=(x,y,text='next',id='ghost')=>label(`<b>&nbsp;</b><span>${text}</span>`,x,y,'evt ghost',104,id);
const KHALID=[['assigned','01:36:34'],['picked','01:46:49'],['way','01:48:20'],['delivered','01:54:55']];
const phone=(y=170,w=110)=>art('courier',0,y,w,'Couriers','','phone');

// The ten events the platform received between 01:36 and 01:55 on 28 September, in arrival order:
// [order (last 3 digits), type, event time, partition, offset in that partition]. The arrival index in the whole log is 13489 + i.
const STREAM=[[883,'assigned','01:36:34',2,3336],[774,'picked','01:36:47',3,3397],[939,'delivered','01:37:35',0,3383],[774,'way','01:37:41',3,3398],[590,'delivered','01:38:36',2,3337],
 [883,'picked','01:46:49',2,3338],[883,'way','01:48:20',2,3339],[774,'failed','01:49:42',3,3399],[775,'assigned','01:50:55',1,3373],[883,'delivered','01:54:55',2,3340]];
const sx=i=>120+120*i,SY=110;
const stream=(cls=o=>o===883?'':'dim')=>STREAM.map(([o,t],i)=>evt(o,t,sx(i),SY,cls(o))).join('');

// The four partitions (slide 10): each event moves to its partition's lane, in arrival order within the lane.
const LY=[30,150,270,390],lx=k=>230+124*k;
const lanes=()=>[0,1,2,3].map(p=>STREAM.filter(s=>s[3]===p).map(([o,t,,,off],k)=>evt(o,t,lx(k),LY[p])+label(String(off),lx(k),LY[p]-24,'off',104,`off${off}`)).join('')+label(`P${p}`,162,LY[p]+18,'hand muted',60,`p${p}`)).join('');
const laneWires=()=>LY.map((y,p)=>path(`M115 245 C130 245 130 ${y+35} 145 ${y+35}`,'em',`w-p${p}`)).join('');

// Partition P2 up close (slides 12–15): offsets 3333–3340, order 5043590 and Khalid's order interleaved.
const P2=[[590,'assigned'],[590,'picked'],[590,'way'],[883,'assigned'],[590,'delivered'],[883,'picked'],[883,'way'],[883,'delivered']];
const px=i=>150+130*i;
const p2lane=(cls=()=>'')=>label('PARTITION P2 · key = order_id',150,60,'hand muted',700,'p2-title')+P2.map(([o,t],i)=>evt(o,t,px(i),150,cls(i))+label(String(3333+i),px(i),124,'off',104,`off${3333+i}`)).join('')+ghost(px(8),150);

// The crash (slides 16–17): offsets 3336–3340 in two lanes. saved = the lane index the restart begins at (5 = 3341).
const BATCH=P2.slice(3);
const lane=(top,y,cls,saved)=>BATCH.map(([o,t],i)=>evt(o,t,300+130*i,y,cls(i),top?eid(o,t):`b${i}`)+label(String(3336+i),300+130*i,y-24,'off',104,top?`off${3336+i}`:`boff${i}`)).join('')+label(`▲ restart ${3336+saved}`,300+130*saved-20,y+78,'ptr saved',150,`${top?'a':'b'}-saved`)+label('💥 crash',300+130*2+60,y-50,'ptr red',120,`${top?'a':'b'}-crash`);

// The timeline: Riyadh deliveries on 28 September, event time on top, arrival below. x = 140 + 20 px per minute after 13:00.
// Order 5043895's delivery arrived twice (the phone's retry), so it has two lines.
const RIYADH=[[5043785,'13:09:50','13:09:58'],[5043815,'13:11:52','13:57:54'],[5043911,'13:12:28','13:12:34'],[5043653,'13:17:03','13:17:04'],[5043820,'13:17:18','13:17:20'],[5043839,'13:17:42','13:17:47'],[5043983,'13:18:44','13:18:45'],[5043888,'13:19:25','13:19:29'],[5043933,'13:25:19','13:25:27'],[5044145,'13:27:26','13:27:28'],[5044232,'13:27:45','13:27:48'],[5043553,'13:28:38','13:28:45'],[5043661,'13:33:36','13:33:42'],[5043982,'13:39:51','13:39:53'],[5043895,'13:43:18','13:43:19'],[5043895,'13:43:18','13:44:36'],[5044028,'13:43:33','13:43:33'],[5043923,'13:49:29','13:49:36'],[5043793,'13:49:44','13:49:45'],[5043926,'13:50:30','13:50:35'],[5043952,'13:58:43','13:58:46'],[5043740,'13:58:58','13:59:00']];
const LATE=5043815,YE=120,YA=330;
const X=s=>{const [h,m,sec]=s.split(':').map(Number);return +(140+20*(h*60+m+sec/60-780)).toFixed(1);};
const TICKS=['13:00','13:15','13:30','13:45','14:00'];
const axes=()=>path(`M140 ${YE} L1370 ${YE}`,'axis','ax-e')+path(`M140 ${YA} L1370 ${YA}`,'axis','ax-a');
const ticks=()=>TICKS.map((t,i)=>label(t,X(t+':00')-40,YE-36,'tick',80,`te${i}`)+label(t,X(t+':00')-40,YA+10,'tick',80,`ta${i}`)).join('')+label('Happened',0,YE-20,'hand teal',130,'ax-e-t')+label('Arrived',0,YA-20,'hand muted',130,'ax-a-t');
// arrivedBy: draw only the events that have arrived by that clock time.
const lines=(arrivedBy='99')=>RIYADH.map(([o,t,a],i)=>a<=arrivedBy?path(`M${X(t)} ${YE+6} L${X(a)} ${YA-6}`,o===LATE?'danger':'thin',`ln-${i}`):'').join('');
// Tumbling 15-minute windows over event time; counts are labels inside each window.
const windows=(closed=[])=>[0,1,2,3].map(i=>outline(144+300*i,16,292,64,closed.includes(i)?'closed':'silver',`win${i}`)).join('');
const counts=c=>c.map((t,i)=>label(t,160+300*i,24,`count ${t.includes('published')?'closed':''}`,260,`cnt${i}`)).join('');

// Khalid's order at 01:50 (slides 3–4): 36 px per minute from the order at 01:35:37.
const T=m=>300+36*m;
const soFar=()=>label('KHALID’S ORDER 5043883 · RUH-05',300,0,'hand muted',600,'k-title')+KHALID.slice(0,3).map(([t,time],i)=>evt(883,t,300+124*i,50)+label(time.slice(0,5),300+124*i,128,'tick',104,`t883${t}`)).join('')+ghost(672,50,'delivered?','k-next')+
 label('15 min so far',T(0),240,'tl-bar',T(15)-T(0),'tl-bar')+label('',T(0),318,'tl-axis',T(30)-T(0),'tl-axis')+[[0,'01:35','ordered'],[15,'01:50','now'],[30,'02:05','the promise']].map(([m,t,e],i)=>label(`${t}<span>${e}</span>`,T(m)-70,326,'tl-tick',140,`tl-tick-${i}`)).join('')+label('',T(15)-2,196,'tl-now',4,'tl-now');

add('Day 3 · Session 3','Streaming fundamentals','',
svg(path('M172 136 L192 136','em')+path('M594 176 C600 215 605 235 605 255','em'))+art('courier',20,60,150,'Courier','','phone')+KHALID.map(([t],i)=>evt(883,t,200+114*i,100)).join('')+art('operations',520,265,170,'Operations','','f-ops'),
'Events, offsets and two clocks: what changes when data arrives one event at a time.',
'About 25 minutes for the board, then Demo 3.3 sections 3 and 6 live if there is time, then Lab 3.3 (25 minutes). Until now every source arrived in batches. Today Qafila’s courier events arrive one at a time. Khalid’s order from the introduction, 5043883 (28 September, Riyadh), runs through the board: its four courier events. The board builds a tiny Kafka (a log, partitions, offsets), breaks it with a crash, and then adds time: event time, windows and watermarks. Many boards are builds: the heading stays and one piece arrives per click. Every event, time and count comes from the course data and Lab 3.3. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['intro','streaming101'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 3</p><h1>Streaming<br><span class="teal">fundamentals</span></h1><p class="lead">Khalid’s delivery,<br>four events, two clocks.</p>'});

add('00 / You are here','Ingest, in real time','The same ingest stage with a different promise: seconds, not hours.',
platform(n=>n==='Streaming'?'fresh':'met',{Streaming:'Process events as they arrive'}),
'<strong>Same stage, new promise:</strong> the events land seconds after they happen, not the next morning.',
'The course map from the story, with today’s stage lit. Everything so far (dlt, CDC + MERGE) read sources in batches. Streaming is still ingest; what changes is the freshness promise. The next session (D3_4) lands this stream in the Iceberg tables from this morning.');

const QT='Operations asks: is Khalid’s order on time?',QS='01:50 on 28 September. Khalid ordered 15 minutes ago, and the promise is 30.';
add('01 / The question',QT,QS,
art('operations',0,30,200,'Operations','“Which orders will miss 30 minutes?”','f-ops')+soFar()+note('15 minutes left: still time to act',880,60,'teal',540),
'Operations must see each order’s latest step <strong>while it is still on the road</strong>, within a minute.',
'A build: two boards. Khalid’s order so far: assigned 01:36, picked up 01:46, on the way since 01:48. The promise from the introduction is 30 minutes from order to door, so at 01:50 there are 15 minutes left. If an order were at 28 minutes with no pick-up, operations could still call the store or send another courier. Ask the room: when does operations need to know?');

add('01 / The question',QT,QS,
art('operations',0,30,200,'Operations','“Which orders will miss 30 minutes?”','f-ops')+soFar()+note('15 minutes left: still time to act',880,60,'teal',540)+
note('<b>Nightly batch:</b> the answer comes tomorrow morning. Khalid had his coffee hours ago.',300,385,'red',540)+note('<b>Stream:</b> each event is processed seconds after it arrives.',900,385,'teal',540),
'<strong>Batch:</strong> collect records, process them on a schedule. <strong>Stream:</strong> process each event as it arrives.',
'Every pipeline so far was a batch: dlt and CDC copied what had changed since the last run. A nightly batch would tell operations about Khalid’s order the next morning, when it no longer matters. A stream processes each courier event seconds after the phone sends it. The introduction’s freshness board said the same thing: operations needs minutes.',['intro']);

add('01 / Who needs it','Stream only where someone acts within minutes','The freshness axis from the introduction. Most teams are fine with batch.',
svg(outline(-10,-14,560,400,'teal','zone-stream')+outline(580,-14,870,400,'silver','zone-batch')+path('M0 330 L1425 330','axis','axis')+[110,400,980,1270].map(x=>path(`M${x} 255 L${x} 320`,'thin')).join(''))+
art('payment',45,40,130,'Fraud check','','f-risk')+art('operations',335,40,130,'Operations','','f-ops')+art('analyst',915,40,130,'Finance','','f-fin')+art('stakeholders',1205,40,130,'Purchasing','','f-pur')+
['seconds','minutes','hours','next morning','next week'].map((t,i)=>label(t,10+i*290,345,'tick',200,`tk${i}`)).join('')+label('Stream',20,0,'hand teal',200,'z-s')+label('Batch',1250,0,'hand muted',180,'z-b')+
note('A stream costs more to build and to run. Data science needs months of history: batch.',0,420,'',1440),
'<strong>Freshness is a requirement:</strong> stream only when an answer minutes old would be too late.',
'The same axis the introduction drew, now split in two. A fraud check must answer before the payment is captured (seconds); operations acts on late orders (minutes). Finance closes yesterday’s books the next morning, purchasing reacts to weekly price lists, data science trains on months of history: batch is cheaper and simpler for all three. Ask the room: what decision changes if this number is 5 minutes old instead of 5 hours?',['intro']);

add('02 / The events','Every step sends an event','Khalid’s courier app reports each step the moment it happens.',
svg(path('M205 185 L290 185','em','w-produce'))+art('courier',0,90,200,'Khalid’s courier','CR-RIY-076','phone')+KHALID.map(([t,time],i)=>evt(883,t,300+150*i,150)+label(time,300+150*i,228,'tick',104,`t883${t}`)).join('')+
note('Each step is a new <b>event</b>: something that already happened.',920,140,'teal',520)+note('The app database keeps only “delivered”. The events keep every step.',300,330,'',1100),
'<strong>Event:</strong> a record of one thing that happened, sent as it happens. Events are added, never changed.',
'Real events from data/raw/events/delivery_events.jsonl: assigned 01:36:34, picked_up 01:46:49, out_for_delivery 01:48:20 (shown as “on the way”), delivered 01:54:55. The introduction showed both kinds of record: the app database’s order row was overwritten at each step and now says only delivered (state); the courier app keeps every step (events). Today we process those events as they arrive.',['intro']);

add('02 / One event','What one event holds','The fourth event, as the courier app sends it: one line of JSON.',
evt(883,'delivered',0,40)+tbl('ev-json',160,0,1280,'ev-0014868<span class="src">delivery_events.jsonl</span>','160px 380px 1fr',[H('field','value','what it says'),['event_id','ev-0014868',SANS('names this one event')],['order_id',K('5043883'),SANS('the order it belongs to: the key')],['event_type','delivered',SANS('which step')],['event_time','2026-09-28T01:54:55.084+03:00',SANS('when it happened, on the phone')],['ingest_time','2026-09-27T22:54:59.758Z',SANS('when the platform received it')]],'mono')+
note('Two ids and two clocks. Each matters later today.',160,290,'teal',1280),
'<strong>Every event carries</strong> its own id, its order’s id and two times. The board uses all four.',
'The raw record, with the courier, store and position fields left out. event_id names this event: we use it to spot duplicates. order_id ties it to Khalid’s order: we use it to keep his events together. event_time and ingest_time are two different clocks, in two different formats: the second half of the session is about them.');

add('03 / The stream','Khalid is not the only order on the road','01:36 to 01:55: ten events from five orders reach the platform, mixed together.',
phone(80)+stream()+STREAM.map(([,,time],i)=>label(time,sx(i),SY+78,'tick',104,`tt${i}`)).join('')+
note('Riyadh and Jeddah, in the order they arrive',120,250,'',1200),
'<strong>A stream</strong> never ends: events from every order and courier arrive one after another.',
'The real arrival order, from the 13,490th event of the week onwards. Khalid’s four events (bright) arrive between events of four other orders: two in Riyadh, three in Jeddah, one of which failed at 01:49. Qafila receives about 3,200 courier events a day. Every reader of this stream has to cope with orders interleaved like this.');

add('03 / The log','Write them down in arrival order: a log','Each event goes on the end. Its position is its offset.',
phone(80)+stream(()=>'')+STREAM.map((s,i)=>label(String(13489+i),sx(i),SY-26,'off',104,`o${i}`)).join('')+ghost(1320,SY)+
tool('Kafka','keeps this log: a topic',120,250,500,'t-kafka')+note('Only added to · never edited · kept for days',700,265,'teal',700),
'<strong>Log:</strong> an ordered list you only add to. <strong>Offset:</strong> a position in it. Kafka calls the log a topic.',
'This is all a Kafka topic is underneath, minus replication and disks. The producer (the phone, through Qafila’s backend) appends at the end; Khalid’s first event is at offset 13,489 because 13,489 events of the week arrived before it. Offsets are positions, not timestamps. The log is kept for days, or forever with tiered storage, so anything in it can be read again. Kafka 4 runs on KRaft only (no ZooKeeper); Redpanda, WarpStream and the cloud services speak the same protocol.',['kafka']);

add('03 / Partitions','Split the log in four: partitions','So several readers can share the work. The key, order_id, picks each event’s partition.',
svg(laneWires())+phone()+lanes()+note('partition = hash(order_id) mod 4',900,40,'',540)+note('All of Khalid’s events land in P2, in order',900,280,'teal',540),
'<strong>Partition:</strong> one of the logs a topic is split into. <strong>Key:</strong> the field that picks it. Same key, same partition.',
'Exactly Lab 3.3 task 1: every event keyed by order_id into 4 partitions (crc32 of the key, modulo 4). The offsets are the real ones in the lab’s log. Khalid’s four events are at P2 offsets 3336, 3338, 3339 and 3340; order 5043590’s delivery landed between them at 3337 because it hashes to P2 too. Each partition numbers its own offsets, so P2’s 3336 and P3’s 3397 have nothing to do with each other. Partitions are the unit of parallelism: 4 partitions, at most 4 readers in one group.',['kafka','lab']);

add('03 / Partitions','Why key by order?','Four readers work in parallel, one per partition. Where Khalid’s events sit decides the order they are read.',
label('<b>No key: spread evenly</b> · illustrative',0,0,'reason',680,'k-none')+[0,1,2,3].map(p=>label(`P${p}`,0,68+90*p,'hand muted',50,`q${p}`)+[...Array(3-p).keys()].map(j=>ghost(60+114*j,50+90*p,'…',`g${p}${j}`)).join('')+evt(883,KHALID[p][0],60+114*(3-p),50+90*p,'',`x883${p}`)+label(['read 4th','read 3rd','read 2nd','read 1st'][p],530,72+90*p,'ptr red',130,`r${p}`)).join('')+
note('“Delivered” is read first, “assigned” last: the board runs backwards.',0,420,'red',680)+
label('<b>Key = order_id</b>',760,0,'reason',680,'k-order')+BATCH.map(([o,t],i)=>evt(o,t,760+124*i,140)+label(String(3336+i),760+124*i,116,'off',104,`off${3336+i}`)).join('')+
note('One reader reads P2 front to back: assigned → picked up → on the way → delivered.',760,250,'teal',680),
'<strong>Order holds inside a partition, never across partitions.</strong> Key by what must stay in order.',
'Wrong and right on one board. The left half is illustrative: if events were spread evenly with no key, Khalid’s four events could land in four partitions with different backlogs, and four readers working in parallel would reach “delivered” before “assigned”. The live board would show him on the way after he had his coffee. Keyed by order_id, all four sit in one partition and one reader sees them in order. Choose the key for the ordering you need: by order here; by courier if you need a courier’s route in order. A hot key (one huge store) makes one partition hot.',['kafka']);

add('04 / Readers','Each reader keeps its own place','Operations and data science read the same partition. Each saves the next offset it will read.',
p2lane()+label('▲ next 3341',px(8)-10,228,'ptr sla',130,'ptr-ops')+label('▲ next 3336',px(3)-10,228,'ptr fraud',130,'ptr-ds')+
art('operations',px(8)-23,280,150,'Operations','live board','f-ops')+art('scientist',px(3)-23,280,150,'Data science','loads history','f-ds')+
note('Reading deletes nothing. A reader’s place is just a saved number.',0,320,'',480),
'<strong>Consumer group:</strong> readers with their own saved offsets. Groups never slow each other down.',
'P2 up close, as it stands at 01:55: offsets 3333 to 3340, order 5043590 and Khalid’s order interleaved, each in its own order. The operations board has read everything (its next offset is 3341, where the next event will go). Data science loads history in batches and is behind, at 3336. Neither removes anything. A consumer saves (commits) its offset as it goes; Kafka stores it per group. The next question is when it saves.',['kafka','demo']);

add('04 / Readers','A new reader can start at offset 0','The log still holds the whole week. A new group reads it again from the start.',
p2lane()+label('▲ next 3341',px(8)-10,228,'ptr sla',130,'ptr-ops')+label('▲ next 3336',px(3)-10,228,'ptr fraud',130,'ptr-ds')+
art('operations',px(8)-23,280,150,'Operations','live board','f-ops')+art('scientist',px(3)-23,280,150,'Data science','loads history','f-ds')+
ghost(0,150,'24 Sep','start')+label('0',0,124,'off',104,'off0')+label('▲ next 0',0,228,'ptr saved',110,'ptr-new')+art('engineer',0,280,140,'A new job','fixes a bug','f-new')+
note('Offset 0 is order 5040442, 24 Sep 05:14',150,320,'teal',360),
'<strong>Replay:</strong> a new group reads the log again from offset 0, to fix a bug or rebuild history.',
'P2’s offset 0 is the first event of the week in that partition: order 5040442, assigned at 05:14 on 24 September in Jeddah. A new consumer group with no saved offset starts there and reads all 5,603 of P2’s events again; the other groups don’t notice. Replay is how you fix a bug in a stream job, backfill a new metric or bootstrap a new system: Lab 3.3 task 5 does it and gets the same counts.',['kafka','lab']);

const crashBatch=()=>svg(outline(px(3)-12,112,px(7)+116-px(3)+12,114,'frame','batch'))+p2lane()+label('▲ saved 3341',px(8)-10,258,'ptr saved',130,'a-saved')+label('💥 crash',px(5)+52,72,'ptr red',120,'a-crash')+[3,4,5].map(i=>label('✓',px(i),232,'ptr sla',104,`ok${i}`)).join('')+art('operations',0,280,150,'Operations','live board','f-ops');
add('05 / A crash','The live board reads in batches','It reads 3336–3340, saves its place first (3341), then works through the batch. After 3338 it crashes.',
crashBatch()+label('<b>1 · read</b> 3336–3340',300,330,'reason',260,'s1')+label('<b>2 · save</b> next = 3341',600,330,'reason',260,'s2')+label('<b>3 · work</b> ✓ ✓ ✓ 💥',900,330,'reason',260,'s3'),
'The crash falls <strong>between saving the offset and finishing the work</strong>.',
'Set up the quiz: the operations consumer reads a batch from P2 (offsets 3336 to 3340), saves its next offset straight away (3341), then updates the live board one event at a time. It has handled 3336, 3337 and 3338 when the machine crashes. A restart always resumes from the last saved offset. Don’t say what happens yet.');

add('05 / A crash','Where does it restart?','The offset was saved before the work. What happens to the rest of the batch?',
art('operations',1100,40,270,'Operations','“Where is Khalid’s order?”','f-ops')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>It reads 3336–3340 again: all five twice</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>It starts at 3341: 3339 and 3340 are never read</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Kafka re-sends what wasn’t processed</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: which number did the reader save?</p>',
'A restart resumes from <strong>the last saved offset</strong>, wherever the work had got to.',
'Allow 45 seconds and let learners commit. B is right: the saved offset already points past the batch, so the restart skips 3339 and 3340, Khalid’s “on the way” and “delivered”. C is the common misconception: Kafka does not track what you processed, only the offset you saved. Then ask: what if it saved after the work instead?',[],
{quiz:{answer:'B',right:'Yes. The saved offset points past the batch, so Khalid’s last two events are never read.',wrong:'Look at the number the reader saved before the crash. Try again.'}});

const LT='Lost, or counted twice',LS='The same crash, with the place saved before or after the work.';
const lostLane=()=>lane(true,40,i=>i>2?'lost':'',5)+label('<b>Save → work</b><br>at-most-once',0,45,'reason',270,'lane-a')+note('Restart at 3341: Khalid’s last two events are <b>never read</b>. The board still says “picked up”.',1100,20,'red',340);
add('05 / A crash',LT,LS,lostLane(),
'<strong>At-most-once:</strong> save first, and a crash <strong>loses</strong> events.',
'A build: the top lane is the quiz’s answer. The live board missed Khalid’s delivery: until someone notices, operations sees an order stuck at picked up and may call a courier who has already delivered.');

add('05 / A crash',LT,LS,lostLane()+lane(false,270,i=>i<3?'twice':'',0)+label('<b>Work → save</b><br>at-least-once',0,275,'reason',270,'lane-b')+note('Restart at 3336: three events read <b>twice</b>. Order 5043590 counts as two deliveries.',1100,250,'red',340)+
note('Neither is exactly once. Choose at-least-once, then make repeats harmless.',0,440,'teal',1440),
'<strong>At-least-once:</strong> save after the work, and a crash <strong>repeats</strong> events. Nothing is lost.',
'Bottom lane: saved after the work, so the restart re-reads the batch from 3336 and handles 3336 to 3338 again. Order 5043590’s delivery is counted twice. Over the whole week Demo 3.3 section 3 shows the same thing with one crash (batches of 100, crash halfway through batch 5): 5,327 delivered events become 5,315 or 5,340. Run that cell live if there is time. Losing is usually worse than repeating, so choose at-least-once and fix the repeats in the output.',['demo']);

add('06 / Duplicates','The phones send repeats too','At 13:43 a courier’s phone sent the same delivered event twice, 77 seconds apart.',
art('courier',0,20,170,'A courier’s phone','CR-RIY-070','f-retry')+tbl('dups',230,20,1210,'order 5043895 · what arrived','150px 120px 130px 140px 1fr',[H('event_id','order_id','event_type','happened','arrived'),[BAD('ev-0014914'),'5043895','delivered','13:43:18','13:43:19'],{c:[BAD('ev-0014914'),'5043895','delivered','13:43:18','13:44:36 · 77 s later'],cls:'unk'}],'mono')+
note('The phone didn’t hear back, so it sent the event again. Same event_id.',230,200,'red',1210)+
label('<b>312</b>repeats in the week',230,300,'answer',360,'n-rep')+label('<b>70</b>of them deliveries',640,300,'answer',360,'n-del')+label('<b>2 copies</b>both in the log: no reader setting stops this',1050,300,'answer',390,'n-zero'),
'Repeats come from crashes <strong>and</strong> from the phones. Both are already in the log.',
'A real retry from the same day as Khalid’s order: ev-0014914, order 5043895 delivered at 13:43:18, arrived at 13:43:19 and again at 13:44:36. Phones retry when the network drops before they hear “received”, so the platform gets two copies of one event. 312 events of the week are such repeats; 70 of them are deliveries. No consumer setting prevents this: the repeat is a real record in the log. So the fix cannot be in the reader. It has to be in what the reader writes.',['demo']);

add('06 / Duplicates','Count each event_id once','The output remembers which ids it has counted. A repeat changes nothing.',
box('Idempotent output','<p>Keep the event_ids already counted.<br>Seen before? Skip it.</p>',0,0,640,'silver','sink','MERGE on a key · upsert · a seen-set')+
rec('seen1',0,260,640,'ev-0014914 · first copy','not seen before: count it','+1','copy')+rec('seen2',0,345,640,'ev-0014914 · second copy','already counted: skip','+0','dup')+
box('Lab 3.3 · the week','<div class="data-row"><span>right answer</span><span>5,257</span></div><div class="data-row"><span>crash + repeats</span><span class="red">5,374</span></div><div class="data-row"><span>each id once</span><span class="teal">5,257 ✓</span></div>',760,0,680,'gold','week','delivered events, 24–30 Sep')+
note('One fix for both: the crash’s re-read and the phone’s repeat.',760,330,'teal',680),
'<strong>Idempotent:</strong> doing it twice has the same effect as once. At-least-once + an idempotent output = effectively once.',
'Lab 3.3 task 2: a naive consumer that crashes once between processing and committing, on top of the phones’ repeats, counts 5,374 deliveries instead of 5,257: 117 too many. Counting each event_id once gives exactly 5,257 despite both. In production the seen-set lives in the output: a MERGE keyed by event_id (Lab 1.4), an upsert into Iceberg (D3_4), or keyed state with a time limit in Flink. “Exactly-once” in Kafka and Flink means effectively-once: at-least-once delivery plus idempotent or transactional writes.',['lab']);

add('07 / Two clocks','Every event has two clocks','Khalid’s delivered event: when it happened on the phone, and when the platform received it.',
tbl('clocks',0,0,940,'ev-0014868 · delivered','160px 1fr 170px',[H('field','as written','Riyadh time'),['event_time','2026-09-28T01:54:55.084+03:00',K('01:54:55')],['ingest_time','2026-09-27T22:54:59.758Z',K('01:54:59')]],'mono')+
label('<b>4.7 s</b>from the phone to the platform',0,200,'answer',440,'gap')+note('Different dates, different zones: the same moment, give or take seconds.',480,215,'teal',460)+
box('Three formats, one stream','<div class="data-row"><span>+03:00 offset</span><span>18,031</span></div><div class="data-row"><span>Z (UTC)</span><span>3,372</span></div><div class="data-row"><span>epoch millis</span><span>1,115</span></div>',1000,0,440,'bronze','formats','parse into one type first'),
'<strong>Event time:</strong> when it happened. <strong>Processing time:</strong> when we received or read it.',
'event_time is the phone’s clock, written with a +03:00 offset; ingest_time is the platform’s, in UTC with a Z, which is why it shows 27 September. Converted to Riyadh time the delivery happened at 01:54:55.1 and arrived at 01:54:59.8, 4.7 seconds later. Two app versions in the field write event_time in three formats, counted on the right over the week’s 22,518 events; Lab 3.3 task 3 parses all three into one timestamp type.',['lab','streaming101']);

add('07 / Two clocks','Which day was Khalid delivered?','Count by the clock that says when we read it, and a replay changes the answer.',
label('<b>Processing time</b><br>when our job read it',0,25,'reason',330,'r-proc')+rec('pt-live',380,20,500,'live run','read at 01:54:59 on 28 Sep','28 Sep','copy')+rec('pt-replay',940,20,500,'replay after a bug fix','read on 29 Sep at 10:00','29 Sep','wrong')+
label('<b>Event time</b><br>when it happened',0,205,'reason',330,'r-event')+rec('et-live',380,200,500,'live run','happened 01:54:55 on 28 Sep','28 Sep','copy')+rec('et-replay',940,200,500,'replay after a bug fix','happened 01:54:55 on 28 Sep','28 Sep','copy')+
note('The business asks when things happened. Processing time only says when we heard.',0,380,'teal',1440),
'<strong>Group by event time:</strong> the same answer live, after a hiccup, and after every replay.',
'The replay time is illustrative: say the job had a bug and we replay the topic the next morning at 10:00. By processing time, Khalid’s delivery would move to 29 September and the 28th would lose a delivery. By event time it stays on 28 September, every time. Processing time is easy (it is just “now”), but it shifts with every network hiccup, backlog and replay. “Deliveries between 13:00 and 13:15” means event time.',['streaming101','flink']);

add('08 / Late events','Most events arrive in seconds. One took 46 minutes.','Riyadh deliveries on 28 September at lunchtime. Each line joins when a delivery happened to when it arrived.',
svg(axes()+lines())+ticks()+note('Over the week: half arrive within 4.2 s, the slowest after ≈ 50 min',0,410,'',840)+note('Order 5043815: delivered 13:11,<br>arrived 13:57',880,400,'red',560),
'<strong>A long tail:</strong> most events arrive in seconds, a few much later. Every count per time slice must decide how long to wait.',
'Real Riyadh deliveries whose event time falls between 13:00 and 14:00 on 28 September, the day of Khalid’s order. Almost every line is vertical: the event arrived seconds after it happened (Khalid’s took 4.7 s). One line leans far right: order 5043815 from RUH-02 was delivered at 13:11:52 and reached the platform at 13:57:54, 46 minutes late, from a phone offline in a basement or a car park. Order 5043895’s repeat from the duplicates board is in here too: two lines from 13:43. The percentiles are for all 22,518 events of the week (Lab 3.3 task 3). Ask: “deliveries between 13:00 and 13:15”: when can we publish that number?',['lab']);

add('08 / Windows','Group the endless stream into windows','Tumbling 15-minute windows by event time: deliveries in each window.',
svg(windows()+axes()+lines())+ticks()+counts(['<b>3</b> deliveries','<b>9</b> deliveries','<b>4</b> deliveries','<b>5</b> deliveries'])+label('<b>Tumbling</b><br>fixed, no overlap: this board',0,400,'reason',440,'w-tumble')+label('<b>Hopping</b><br>15 min every 5: each event in 3 windows',480,400,'reason',440,'w-hop')+label('<b>Session</b><br>ends after 30 min of silence: a shift',960,400,'reason',440,'w-session'),
'<strong>Window:</strong> a slice of event time that a count is grouped by. The hard question is when a slice is finished.',
'The counts are complete, counted once the whole hour has arrived: the late delivery is one of the 3 in 13:00–13:15, and order 5043895 counts once in 13:30–13:45 despite its repeat (Lab 3.3 counts distinct orders). A tumbling window puts each event in exactly one bucket. A hopping window of 15 minutes every 5 puts each event in three overlapping buckets (“deliveries in the last 15 minutes, updated every 5”). A session window has no fixed size: a courier’s shift, split wherever there was no event for 30 minutes. But a live board must publish 13:00–13:15 at some point, and the late delivery only arrives at 13:57.',['demo']);

const WT='When is a window finished?',WS='The engine guesses with a watermark: the newest event time seen, minus 5 minutes of allowed lateness.';
add('09 / Watermarks',WT,WS,
svg(windows([0])+axes()+lines('13:25:28')+path(`M${X('13:20:19')} 104 L${X('13:20:19')} ${YE+34}`,'danger dashed','wm')+path(`M${X('13:25:27')} ${YA+60} L${X('13:25:27')} ${YA+8}`,'em','now'))+ticks()+counts(['<b>2</b> published','<b>6</b> so far','<b>0</b> so far','<b>0</b> so far'])+label('W = 13:20:19',X('13:20:19')+8,YE+14,'ptr red chip',160,'wm-l')+label('now 13:25',X('13:25:27')+8,YA+44,'ptr teal',110,'now-l')+
note('A delivery from 13:25:19 arrives: W passes 13:15',0,410,'',700)+note('13:00–13:15 is published with 2. The third is still on a phone.',720,410,'red',720),
'<strong>Watermark:</strong> “I have probably seen everything before W.” When W passes a window’s end, the window is published.',
'A build: two moments. Only the deliveries that have arrived by 13:25:27 are drawn. A delivery that happened at 13:25:19 arrives, so W = 13:25:19 − 5 minutes = 13:20:19. W has passed 13:15, the end of the first window, so the engine publishes 13:00–13:15 with 2 deliveries and forgets the window. Order 5043815’s delivery, which happened at 13:11, has not arrived yet.',['demo','flink']);

add('09 / Watermarks',WT,WS,
svg(windows([0,1,2])+axes()+lines('13:57:54')+path(`M${X('13:45:30')} 104 L${X('13:45:30')} ${YE+34}`,'danger dashed','wm')+path(`M${X('13:57:54')} ${YA+60} L${X('13:57:54')} ${YA+8}`,'em','now'))+ticks()+counts(['<b>2</b> published','<b>9</b> published','<b>4</b> published','<b>3</b> so far'])+label('W = 13:45:30',X('13:45:30')-168,YE+14,'ptr red chip',160,'wm-l')+label('now 13:57',X('13:57:54')-118,YA+44,'ptr teal',110,'now-l')+label('✕',X('13:57:54')-28,YA-34,'ptr red',30,'drop')+
note('13:57:54: order 5043815’s delivery finally arrives',0,410,'',700)+note('Its window closed half an hour ago: the event is dropped.',720,410,'red',720),
'<strong>Late event:</strong> one that arrives after the watermark passed its window. It is dropped, or handled separately.',
'Now 13:57:54: the newest delivery seen happened at 13:50:30, so W = 13:45:30, and three windows have been published (2, 9 and 4 deliveries). Order 5043815’s delivery belongs to 13:00–13:15, which closed at 13:25: it is dropped, and the live count stays 2 instead of 3. With 46 minutes of allowed lateness it would have counted, but every window would also have been published 46 minutes later. Demo 3.3 section 6 traces the same rule; run it live.',['demo','flink']);

add('09 / The trade-off','Wait longer, or answer sooner?','Lab 3.3, the whole week: delivered orders per city per 15-minute window.',
'<table class="mapping compact"><thead><tr><th>Allowed lateness</th><th>Late events dropped</th><th>Windows exactly right</th><th>Median wait for a result</th></tr></thead><tbody><tr><td class="zone">0 min</td><td>140</td><td>90.3%</td><td>1.1 min</td></tr><tr><td class="zone">2 min</td><td>133</td><td>90.7%</td><td>3.2 min</td></tr><tr><td class="zone">10 min</td><td>108</td><td>92.5%</td><td>11.2 min</td></tr><tr><td class="zone">30 min</td><td>40</td><td>97.2%</td><td>31.1 min</td></tr><tr><td class="zone">60 min</td><td>0</td><td>100%</td><td>61.1 min</td></tr></tbody></table>'+label('<b>Operations</b> · 2 minutes; a slightly low count is fine',0,440,'reason',680,'need-ops')+label('<b>Finance</b> · exact totals; recompute in batch tonight',740,440,'reason',700,'need-fin'),
'<strong>Latency or completeness</strong> is a business decision, not a technical one.',
'Learners produce this table in Lab 3.3 task 4, with W = newest event time − allowed lateness over all cities. More patience, fewer dropped events, later results: no setting gives both. Engines offer escape hatches: Flink can re-publish a window when late data arrives; Spark drops state after the watermark; Kafka Streams has grace periods; or recompute in batch overnight. One stream can serve both teams: a 2-minute board for operations and a nightly recompute for finance.',['lab','flink','spark']);

add('10 / The engines','The same ideas in real engines','You build each piece by hand in Lab 3.3. Production systems give them names.',
svg(path('M165 160 L225 160','em')+path('M565 160 L635 160','em')+path('M975 160 L1045 160','em'))+art('courier',20,90,140,'Phones','','phone')+box('Log','<p>partitions · offsets<br>consumer groups · replay</p>',230,40,330,'','eng-log')+box('Stream job','<p>event time · windows<br>watermarks · state</p>',640,40,330,'silver','eng-job')+box('Output','<p>idempotent writes<br>offsets in the commit</p>',1050,40,330,'gold','eng-sink')+tool('Kafka 4','Redpanda · WarpStream · MSK',230,280,330,'t-kafka')+tool('Flink · Spark','or Kafka Streams in a service',640,280,330,'t-job')+tool('Iceberg','next session: D3_4',1050,280,330,'t-sink')+note('Streaming databases (RisingWave, Materialize): the job and the table in one',230,430,'',1150),
'<strong>Every engine</strong> implements partitioned input, saved offsets, keyed state, windows and watermarks.',
'Map each box to the board: the log is Kafka (or anything that speaks its protocol); the stream job is Flink (one event at a time, rich event-time state) or Spark Structured Streaming (small batches, the usual choice on Databricks); Kafka Streams is the same as a library inside a Java service. Spark’s withWatermark, dropDuplicatesWithinWatermark and checkpointLocation are the watermark, the seen-set and the saved offsets from this board (Demo 3.3 section 7 shows the code). The output is the next session: committing data and offsets together into Iceberg.',['kafka','flink','spark']);

const WORDS=[['Stream','events processed as they arrive'],['Event','a record of one thing that happened'],['Log','an ordered list you only add to'],['Offset','a position in the log'],['Partition','one of the logs a topic is split into'],['Key','the field that picks the partition'],['Consumer group','readers with their own saved offsets'],['At-least-once','nothing lost; repeats possible'],['Idempotent','twice has the same effect as once'],['Event time','when it happened, not when we heard'],['Window','a slice of event time to count in'],['Watermark','“everything before W has arrived”']];
add('11 / The words','Twelve words for streaming','Every one appeared on a board today. Lab 3.3 and the next session use them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>A log you replay, a key that keeps order, a clock that tells the truth:</strong> the rest follows.',
'A reference board: point at each word and ask the room which board it appeared on, and what Khalid’s events looked like there. Learners can come back to it during the lab and D3_4.');

const RECALL=[
 ['Khalid’s four events: which partition, and in what order?','All in P2 (the key is order_id), offsets 3336–3340, read in order. Order 5043590’s delivery sits between them.'],
 ['Data science replays the topic from offset 0. What changes for operations’ live board?','Nothing. Each group keeps its own offsets, and reading deletes nothing.'],
 ['The live board saves its offset, then works, and crashes after 3338. What happens to Khalid’s delivery?','It is never read: the restart begins at 3341. Save after the work instead, and make repeats harmless.'],
 ['Order 5043895’s delivery arrives twice. How do you count it once?','Count each event_id once: skip ids already counted. The output is idempotent.'],
 ['Order 5043815 was delivered at 13:11 and arrived at 13:57, with 5 minutes of allowed lateness. Is it counted?','Not live: 13:00–13:15 was published at 13:25 with 2. It needs 46 minutes of lateness, or a nightly recompute.']];
add('12 / Recall','Your turn: answer before you look','Five questions on Qafila’s courier events. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer: the guess that turns out wrong is what makes the right answer stick. The next slide fills in all five answers in place.');

add('12 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>A log, offsets, an id for repeats, event time and a watermark:</strong> the whole session in one line.',
'Go back to the board for any answer the room missed: partitions and the key (10–11), readers and replay (12–13), the crash (14–17), repeats (18–19), late events and watermarks (22–25). Question 5: order 5043815 arrived 46 minutes after it happened, so only an allowed lateness of 46 minutes or more would have kept its window open.');

add('Lab 3.3 · 25 min','Streaming from first principles','Build a tiny Kafka and a tiny Flink in plain Python, and feed them a week of Qafila courier events.',
`<div class="references"><div><h3>Lab 3.3 · 25 minutes</h3><p>1 · A 4-partition log, keyed by order · 5′</p><p>2 · Offsets, a crash, an idempotent count · 6′</p><p>3 · Event time: three formats · 5′</p><p>4 · Windows and watermarks: the trade-off · 7′</p><p>5 · Replay from offset 0 · 2′</p><p class="note">✱ Exactly-once: results and offsets in one transaction. Find Khalid’s order 5043883 in your log: P2, offsets 3336–3340.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D3_3_streaming_fundamentals.ipynb</a><a href="${sources.demo}" target="_blank" rel="noopener">Demo 3.3 · the board, run on the data</a><a href="${sources.streaming101}" target="_blank" rel="noopener">Further reading · Streaming 101</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next, after a 10-minute break: Session 4 · streams into the lakehouse.',
'Hand over to the lab. A quick first check after task 1: the lab’s log puts order 5043883 in partition 2 at offsets 3336, 3338, 3339 and 3340, exactly as on the board. Everything on the board is in the demo notebook with the real data; if you skipped sections 3 and 6 live, point learners to them. The stretch task (results and offsets in one DuckDB transaction) is the bridge to D3_4, where the same idea is one Iceberg commit.',['lab','demo']);

Story.start({brand:'QAFILA / STREAMING STORY',sources});
})();
