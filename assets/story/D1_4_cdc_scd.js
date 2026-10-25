/* D1_4 Change data capture and history, told on one evolving board (engine: story.js), one idea per board.
   The running example is customer 100898, the only customer in September's feed that arrival order gets wrong. Khalid
   (107371, from the course introduction) signs up during September, and customer 100710 is erased.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every row and number is real: data/raw/cdc (the 1 September snapshot and September's change feed), app.customers and
   app.orders in data/raw/qafila_oltp.sqlite, and Lab 1.4's solution (labs/solutions/D1_4_cdc_merge_scd.ipynb).
   Feed times are UTC, as the feed records them. Mobile numbers are masked on the board. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,tool,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={debezium:'https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events',merge:'https://duckdb.org/docs/stable/sql/statements/merge_into',scd2:'https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/',intro:'00_intro.html',windows:colab('00_sql_windows.ipynb'),lab:colab('D1_4_cdc_merge_scd.ipynb')};

// Customer 100898's events, as the connector delivered them on 2 September: arrival number, lsn, what changed.
// e670c is the redelivery before the board reveals it as one.
const EV={e670:[333,5006670,'17:15 · email changed'],e086:[347,5007086,'18:49 · Dammam → Jeddah'],e670b:[360,5006670,'17:15 · email changed, re-sent'],e670c:[360,5006670,'17:15 · email changed']};
const ev=(id,x,y,w,cls='',key=id)=>rec(id,x,y,w,`lsn ${EV[key][1]}`,EV[key][2],'',`chg ${cls}`);
const arr=(id,x,y,w)=>label(`arrived #${EV[id][0]}`,x,y-26,'off',w,`a-${id}`);
// The app's row for 100898 through the day; v1 and v2 are overwritten, and our copy starts from the snapshot.
const v1=(x,y,w,cls='src')=>rec('v1',x,y,w,'customer 100898','Dammam · Al Nuzha · icloud email','',cls);
const v2=(x,y,w,cls='src')=>rec('v2',x,y,w,'customer 100898','Dammam · Al Nuzha · yahoo email','',cls);
const now=(x,y,w)=>rec('now',x,y,w,'customer 100898','Jeddah · Al Hamra · yahoo email','','src');
const copy=(x,y,w,body='Jeddah · Al Hamra',cls='copy')=>rec('snap',x,y,w,'our copy · 100898',body,'',cls);
const order534=(x,y,w)=>rec('o534',x,y,w,'order 5019534','27 Aug · DMM-01, Dammam · delivered','98.44','order');
const fin=(x,y,w,ask='“Spend by customer city?”')=>art('analyst',x,y,w,'Marketing',ask,'f-fin');

add('Day 1 · Session 4','Change data capture and history','',
svg(path('M230 114 L230 162','em')+path('M230 244 L230 292','em'))+v1(20,40,440)+ev('e086',20,170,440)+now(20,300,440)+art('customer',500,70,210,'Customer 100898','Dammam → Jeddah','f-cust'),
'Follow every change a source makes, then keep the history.',
'About 25 minutes for the board, then Lab 1.4 (25 minutes). If window functions are new to the room, start with the 15–20 minute core of 00_sql_windows: row_number to dedupe, lead for valid_to, QUALIFY. One real customer, 100898, runs through the whole board: on 2 September they changed their email and then moved from Dammam to Jeddah. Khalid from the introduction appears once more, at sign-up. Many boards are builds: the heading stays and one piece arrives per click, so pause on each step. Every number comes from the course data and Lab 1.4. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['windows','intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 1</p><h1>Change data<br>capture<br><span class="teal">and history</span></h1><p class="lead">One customer moves city.<br>Keep up, and remember.</p>'});

add('00 / You are here','Changes, not just new rows','Still the ingest stage. Today the source also updates and deletes rows.',
platform(n=>n==='CDC + MERGE'?'fresh':'met',{'CDC + MERGE':'Changes, deletes, history'}),
'<strong>Bronze keeps every change.</strong> Silver builds two tables from them: the customers now, and their history.',
'The course map, with CDC + MERGE lit. In D1_3, dlt loaded new and updated rows with a cursor on updated_at. The app database also deletes rows, and it keeps no history. Today’s source is a feed of changes; from it we build the current state (MERGE) and the history (SCD Type 2).');

// 01 · The change
add('01 / The change','Customer 100898 changes twice in one evening','2 September, as the app recorded it (times in UTC, as the app writes them).',
svg(path('M760 95 L796 95','em','w-ch'))+art('customer',0,20,210,'Customer 100898','a customer since 2025','f-cust')+
rec('ch1',290,60,460,'2 Sep 17:15 · new email','icloud.com → yahoo.com','','chg')+rec('ch2',800,60,640,'2 Sep 18:49 · moves city','Dammam · Al Nuzha → Jeddah · Al Hamra','','chg')+
note('Each edit changes the same row of the app’s customers table.',290,175,'teal',1150)+
label('In September: <b>4,458</b> profile edits · <b>1,541</b> sign-ups · <b>159</b> erasure requests',290,330,'check',1150,'counts'),
'<strong>Customers change</strong> after they sign up. Today our copy must follow every change.',
'The introduction’s customers board, now with one real customer. At 17:15 customer 100898 changed their email; at 18:49 they moved from Al Nuzha in Dammam to Al Hamra in Jeddah. Times are UTC, as the app database and the feed record them (18:49 UTC is 21:49 in Riyadh). The September counts are the ones the introduction showed: they come from the change feed we read today.',['intro']);

add('01 / The change','The app keeps only the latest row','app.customers holds one row per customer. Each edit overwrote the one before.',
svg(path('M424 115 L476 115','em','w-v12')+path('M924 115 L976 115','em','w-v23'))+label('app.customers · customer 100898',0,0,'hand muted',600,'t-app')+label('1 Sep',0,54,'off',420,'tk1')+label('after 17:15',500,54,'off',420,'tk2')+label('after 18:49',1000,54,'off',440,'tk3')+v1(0,80,420,'src gone')+v2(500,80,420,'src gone')+now(1000,80,440)+
note('Overwritten: these values are gone from the app',0,170,'red',920)+label('<b>State</b> · how things are now. The introduction’s order row worked the same way.',0,300,'check',1440,'state')+
note('Where did 100898 live at 18:00? The app can no longer say.',0,380,'teal',1440),
'<strong>The app keeps state:</strong> every change overwrites the row, and the old values are lost.',
'This is the introduction’s “state or events” board again, for a customer instead of an order. The app database stores only the result of each edit. That is the right design for the app: it needs to know where to deliver now. It is a problem for anyone who asks about the past.',['intro']);

add('01 / The change','Our copy still says Dammam','Day 1 copied customers from a snapshot on 1 September. The teams read the copy, not the app.',
label('the app · today',0,14,'off',440,'tk-app')+now(0,40,440)+label('our copy · from the 1 Sep snapshot',1000,14,'off',440,'tk-copy')+copy(1000,40,440,'Dammam · Al Nuzha · icloud email','wrong')+label('<b style="color:#b96251">≠</b>different rows',560,22,'answer center',320,'neq')+
fin(0,190,170)+note('Every report on our copy puts 100898 in Dammam.',230,240,'red',1210)+
label('September: <b>6,158</b> changes to <b>4,542</b> customers',230,330,'check',1210,'counts')+note('Which rows changed, and how do we find out?',230,400,'teal',1210),
'<strong>A copy goes stale</strong> the moment the source changes. We need to learn every change.',
'Our copy is the 1 September snapshot (data/raw/cdc/customers_snapshot_2026-09-01.csv): 6,060 customers. By 30 September the app has 7,442, and 4,542 customers changed in some way: they signed up, edited their profile or were erased. Marketing reports spend by customer city from our copy, so every one of 100898’s orders still counts as Dammam. Ask the room how they would find out which rows changed.');

// 02 · Capturing change: three ways, one per click.
const WT='How do we learn what changed?',WS='Three ways. Each one sees customer 100898’s evening differently.';
const way1=()=>label('<b>Copy it all again, compare</b><br>a full snapshot every night',0,10,'reason',330,'r1')+rec('see1',370,10,520,'next night: one difference','Dammam → Jeddah, and a new email','','chg')+note('The 17:15 version is never seen. Reads every row, every night.',940,5,'',500);
const way2=()=>label('<b>Ask for recent rows</b><br>updated_at after the last run: dlt’s cursor (D1_3)',0,175,'reason',330,'r2')+rec('see2',370,175,520,'next run: the row as it is now','Jeddah · Al Hamra · updated 18:49','','chg')+note('Hard delete: the row is gone. 159 erasures never seen',940,160,'red',500)+note('Soft delete: just a flag, so it’s seen',940,240,'',500);
const way3=()=>label('<b>Read the database’s log</b><br>every change, as it is written',0,340,'reason',330,'r3')+ev('e670',370,340,250)+ev('e086',640,340,250)+note('Every insert, update and delete, in order, within seconds',940,335,'teal',500);
add('02 / Capturing change',WT,WS,way1(),
'Way 1: <strong>compare full copies.</strong> Simple, but heavy, and it sees only the net change.',
'A build: one way per click. Snapshot diff: copy the whole table every night and compare with yesterday’s copy. Fine for small tables, but it reads every row every time, and it sees only the net change per night: the 17:15 version, with the new email but the old city, never shows up.');

add('02 / Capturing change',WT,WS,way1()+way2(),
'Way 2: <strong>ask for rows changed since the last run.</strong> Cheap, but blind to hard deletes.',
'Query-based: WHERE updated_at > the last cursor. This is exactly what dlt’s incremental cursor did in D1_3. It is cheap and it works for many tables, but it misses hard deletes (the row is gone, so nothing matches), any change that doesn’t touch updated_at, and every version between two runs. A soft delete is different: the app keeps the row and sets is_deleted = 1, an ordinary update, so the cursor sees it (492 in September). Every consumer then has to filter those rows out.');

add('02 / Capturing change',WT,WS,svg(outline(350,318,1090,112,'teal','best'))+way1()+way2()+way3(),
'<strong>Change data capture (CDC):</strong> learning every change a source makes. Reading its log is the complete way.',
'Log-based CDC reads the database’s own transaction log (PostgreSQL logical replication, the MySQL binlog). It sees both of 100898’s edits, deletes included, in commit order, and it doesn’t query the tables at all. The next board shows what that log is.',['debezium']);

add('02 / The log','The database writes every change to its log','Before it changes a table, the database appends the change to a log. 100898’s edits sit among everyone else’s.',
label('THE LOG · in the order the changes were committed →',0,0,'hand muted',900,'log-title')+
rec('lg1',0,50,265,'lsn 5006617','102425 · update · 17:03','','src')+rec('lg2',292,50,265,'lsn 5006654','103435 · delete · 17:14','','src')+rec('e670',584,50,265,'lsn 5006670','100898 · update · 17:15','','chg')+label('… other changes …',876,72,'caption center',265,'lg-gap')+rec('e086',1168,50,272,'lsn 5007086','100898 · update · 18:49','','chg')+
label('<b>Log</b><br>the database writes each change here first, so it can recover after a crash',0,200,'reason',440,'d-log')+label('<b>lsn</b> · log sequence number<br>a change’s position in the log: higher means later',500,200,'reason',440,'d-lsn')+tool('Debezium','a CDC connector: reads the log and sends each change on as an event',1000,195,440,'t-deb')+
note('The log already exists, so reading it adds almost no load to the app.',0,390,'teal',1440),
'<strong>Log-based CDC:</strong> a connector reads the database’s log and turns each change into an event.',
'Real lsns from the feed. Every change to every table goes into the log in commit order, deletes included (customer 103435 was erased at 17:14). The lsn, log sequence number, is the position: it only grows. Debezium is the common open-source connector; it usually sends the events through Kafka, which Day 3 covers. Managed options are Fivetran, AWS DMS and Datastream.',['debezium']);

const CT='One change, as the connector writes it',CS='Debezium writes customer 100898’s move as a change event (unchanged fields shortened).';
const evRows=[['op','u'],['before','{ city: Dammam, district: Al Nuzha, … }'],['after','{ city: Jeddah, district: Al Hamra, … }']];
add('02 / A change event',CT,CS,
tbl('ev-t',0,0,820,'change event · customers','150px 1fr',evRows,'mono')+
label('<b>op</b> · what happened<br>c create · u update · d delete',880,0,'reason',560,'k-op')+label('<b>before, after</b> · the whole row<br>name, mobile and national ID too',880,115,'reason',560,'k-img'),
'<strong>Change event:</strong> one change, carrying the whole row before and after it.',
'The real event from data/raw/cdc/customers_changes.jsonl, with the unchanged fields elided. A delete has only before; an insert only after. (Debezium also uses r for rows read during its first snapshot.) The images are full rows, so personal data (name, mobile, national ID) travels through the pipeline: remember that on Day 4.',['debezium']);

add('02 / A change event',CT,CS,
tbl('ev-t',0,0,820,'change event · customers','150px 1fr',[...evRows,{c:['source.lsn','5007086'],cls:'hit'},['ts_ms','1788374988697 · 2 Sep 18:49:48']],'mono')+
label('<b>op</b> · what happened<br>c create · u update · d delete',880,0,'reason',560,'k-op')+label('<b>before, after</b> · the whole row<br>name, mobile and national ID too',880,115,'reason',560,'k-img')+label('<b>source.lsn</b> · its place in the log<br>the true order of the changes',880,230,'reason',560,'k-lsn')+label('<b>ts_ms</b> · the commit time<br>two changes can share a millisecond',880,345,'reason',560,'k-ts'),
'Every event carries its <strong>lsn</strong>, the true order of the changes. The next board shows why that matters.',
'lsn orders every change in the database, which ts_ms cannot do: milliseconds collide in a busy database. We use ts_ms later as the moment a version starts. In Kafka the message key is the primary key, customer_id.',['debezium']);

// 03 · The feed
const FT='How the events reach us',FS='September’s feed holds 6,346 events. Here are customer 100898’s, in the order they arrived.';
add('03 / The feed',FT,FS,
label('ARRIVAL ORDER →',0,0,'hand muted',600,'arr-title')+arr('e670',0,90,440)+arr('e086',490,90,440)+arr('e670b',980,90,460)+ev('e670',0,90,440)+ev('e086',490,90,440)+ev('e670b',980,90,460,'','e670c')+
note('Compare #360 with #333.',980,180,'teal',460),
'The feed lists events in <strong>arrival order</strong>. Look at #360 before the next click.',
'Arrival is the position in the file, as the connector delivered it. Give the room 20 seconds: what do #333 and #360 have in common? (The same lsn and the same content.)',['lab']);

add('03 / The feed',FT,FS,
label('ARRIVAL ORDER →',0,0,'hand muted',600,'arr-title')+arr('e670',0,90,440)+arr('e086',490,90,440)+arr('e670b',980,90,460)+ev('e670',0,90,440)+ev('e086',490,90,440)+ev('e670b',980,90,460,'dup')+note('lsn 5006670 again: re-sent after the connector restarted, and after a newer change',980,180,'red',460)+
label('<b>188</b> events delivered twice (rows − distinct lsn)',0,300,'check',800,'st-dup')+label('<b>3,492</b> arrived after an event with a higher lsn',0,360,'check',800,'st-ooo')+note('Normal for CDC: restarts re-send, partitions reorder',880,300,'teal',560),
'<strong>At-least-once delivery:</strong> an event can arrive twice, and late. For CDC that is normal, not a bug.',
'The third event is a redelivery of the first: same lsn, same content, sent again after a connector restart, and it arrives after the move. Connectors promise at-least-once: they would rather send an event twice than lose it. Lab 1.4 task 1 counts both kinds across the feed: 6,346 events, 6,158 distinct lsns, and 3,492 events that arrived after one with a higher lsn.',['lab']);

add('04 / Predict','Apply them in arrival order','Overwrite our copy with each event’s after image, in arrival order. Where does customer 100898 live at the end?',
arr('e670',980,40,460)+arr('e086',980,160,460)+arr('e670b',980,280,460)+ev('e670',980,40,460)+ev('e086',980,160,460)+ev('e670b',980,280,460,'dup')+'<button class="quiz-option" data-answer="A" style="top:15px"><span class="letter">A</span><span>Jeddah · Al Hamra</span></button><button class="quiz-option" data-answer="B" style="top:120px"><span class="letter">B</span><span>Dammam · Al Nuzha</span></button><button class="quiz-option" data-answer="C" style="top:225px"><span class="letter">C</span><span>Two rows, one per city</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Choose an answer, then say which event is applied last.</p>',
'Each event carries the whole row, not only what changed.',
'Allow 45 seconds. B is correct: #360 is applied last, and its after image is the whole row as it was at 17:15, city Dammam included. A is the common answer: people think of events as “what changed”. C cannot happen here: each event overwrites the same row.',[],
{quiz:{answer:'B',right:'Yes. #360 arrives last, and its after image still says Dammam.',wrong:'Follow the arrival numbers: which event is applied last, and what city is in its after image?'}});

const OT='Arrival order or log order?',OS='Apply each event’s after image in turn: the last one applied wins.';
const byArrival=()=>label('<b>By arrival</b>',0,35,'reason',220,'lane-a')+arr('e670',240,30,270)+arr('e086',530,30,270)+arr('e670b',820,30,270)+ev('e670',240,30,270)+ev('e086',530,30,270)+ev('e670b',820,30,270,'dup')+copy(1130,30,310,'Dammam · Al Nuzha ✗','wrong');
add('04 / Order by lsn',OT,OS,svg(path('M1092 65 L1126 65','danger','w-ra'))+byArrival()+note('#360 is applied last. Its after image is the whole row as it was at 17:15: Dammam.',240,140,'red',1200),
'By arrival, the re-sent event wins: <strong>our copy goes back to Dammam.</strong>',
'Step 1 of the reveal. The quiz answer, drawn: apply the three after images in arrival order and the stale 17:15 row is the last one written.');

add('04 / Order by lsn',OT,OS,svg(path('M1092 65 L1126 65','danger','w-ra')+path('M802 285 L1126 285','em','w-rl'))+byArrival()+
label('<b>By lsn</b><br>repeats dropped',0,250,'reason',220,'lane-l')+ev('s670',240,250,270,'','e670')+ev('s086',530,250,270,'','e086')+rec('res-l',1130,250,310,'our copy · 100898','Jeddah · Al Hamra ✓','','copy')+
note('Exactly 1 of 7,442 customers in September’s feed, and no error',0,390,'red',700)+note('By lsn: 0 differences from the app',780,390,'teal',660),
'<strong>Trust the log position, not the arrival order.</strong> Then repeats and late events do no harm.',
'Step 2. Customer 100898 is the only customer in September’s feed whose last arrival is not their last change, which is exactly why this bug survives testing: the totals look right and nothing fails. Lab 1.4 task 3’s reconciliation catches it (missing 1, extra 1). In the lab, try it: rebuild latest ordered by arrival and rerun the check.',['lab']);

const LT='Keep only the last change per customer',LS='Applying changes needs one row per customer. Two steps get there.';
const dedupe=()=>label('Feed · 6,346 events',0,0,'hand muted',340,'h1')+label('Distinct lsn · 6,158',550,0,'hand muted',340,'h2')+
ev('e670',0,40,340)+ev('e086',0,140,340)+ev('e670b',0,240,340,'dup')+label('✕',428,258,'ptr red',30,'x-dup')+ev('s670',550,90,340,'','e670')+ev('s086',550,190,340,'','e086');
const dedupeWires=path('M342 75 C440 75 450 125 546 125','em','w-d1')+path('M342 175 C440 175 450 225 546 225','em','w-d2')+path('M342 275 C400 275 410 275 420 275','danger','w-d3');
add('05 / Latest per key',LT,LS,svg(dedupeWires)+dedupe()+
label('SELECT DISTINCT ON (lsn) * FROM changes',0,350,'code',1440,'sql')+note('Step 1: drop the repeats. They are identical, so any copy may stay.',0,410,'teal',1440),
'Step 1: <strong>drop repeated events</strong>, one per lsn.',
'Lab 1.4 task 2, first half. DISTINCT ON (lsn) keeps one row per lsn: redeliveries are identical, so it doesn’t matter which copy survives. 188 rows go.',['lab','windows']);

add('05 / Latest per key',LT,LS,svg(dedupeWires+path('M892 225 C990 225 1000 175 1096 175','em','w-l1')+path('M892 125 L940 125','danger','w-d4'))+dedupe()+label('Per customer · 4,542',1100,0,'hand muted',340,'h3')+label('✕',948,108,'ptr red',30,'x-old')+ev('latest',1100,140,340,'','e086')+
label('QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) = 1',0,350,'code',1440,'sql')+note('Skip it and DuckDB refuses: “MERGE INTO command cannot affect the same target row more than once”',0,410,'red',1440),
'Step 2: <strong>keep the highest lsn per customer.</strong> Only the newest change decides the current row.',
'Second half: row_number over each customer, newest lsn first, keeps one row per key. A delete has no after image, so the key comes from before: coalesce(after.customer_id, before.customer_id). The error on the slide is DuckDB 2.0’s real message when the source has two rows for one key: the next statement, MERGE, needs exactly one.',['lab','windows']);

// 06 · MERGE: one example customer per outcome. Left: the latest change; right: our copy, the 1 September snapshot.
const MT='MERGE: apply each change to our copy',MS='Match each customer’s latest change to our copy (the 1 September snapshot) on customer_id.';
const mLeft=()=>label('LATEST CHANGE',0,0,'hand muted',400,'m-l')+rec('latest',0,50,400,'100898 · u · lsn 5007086','Jeddah · Al Hamra','','chg')+rec('m371',0,180,400,'107371 · c · lsn 5107395','Khalid signs up · Riyadh · Al Malqa','','chg')+rec('m710',0,310,400,'100710 · d · lsn 5051530','erased · after: null','','chg')+label('OUR COPY',1040,0,'hand muted',400,'m-r');
const MROW=[85,215,345],mWire=(i,cls)=>path(`M404 ${MROW[i]} L1034 ${MROW[i]}`,cls,`w-m${i}`);
const c898=(done)=>rec('snap',1040,50,400,'customer 100898',done?'Jeddah · Al Hamra':'Dammam · Al Nuzha','','copy');
const c371=(done)=>rec('c371',1040,180,400,'customer 107371',done?'Khalid · Riyadh · Al Malqa':'no row','',done?'copy':'copy stale');
const c710=(done)=>rec('c710',1040,310,400,'customer 100710','Riyadh · Al Naseem','',done?'copy gone':'copy');
const branch=(i,text)=>label(text,500,MROW[i]-33,'reason chip',440,`br${i}`);
add('06 / MERGE',MT,MS,svg(mWire(0,'thin dashed')+mWire(1,'thin dashed')+mWire(2,'thin dashed'))+mLeft()+c898(false)+c371(false)+c710(false)+[['found'],['not found'],['found']].map(([t],i)=>label(t,640,MROW[i]-22,'ptr chip',160,`mt${i}`)).join(''),
'MERGE starts by looking up each change’s key in the target: <strong>found</strong> or <strong>not found</strong>.',
'A build: one customer per click, one per outcome. Three real customers from the feed: 100898 (our move), Khalid 107371 from the introduction, who signed up on 26 September, and customer 100710, who was erased on 13 September. Our copy is the 1 September snapshot, so it has 100898 and 100710 but not Khalid. Ask the room what should happen to each before clicking on.',['merge']);

add('06 / MERGE',MT,MS,svg(mWire(0,'em')+mWire(1,'thin dashed')+mWire(2,'thin dashed'))+mLeft()+branch(0,'<b>found → UPDATE</b><br>every column, from the after image')+c898(true)+c371(false)+c710(false)+label('not found',640,MROW[1]-22,'ptr chip',160,'mt1')+label('found',640,MROW[2]-22,'ptr chip',160,'mt2'),
'<strong>Found, and a change:</strong> UPDATE the row with the event’s after image.',
'100898 exists in the snapshot and its latest change is an update, so its row becomes Jeddah · Al Hamra. Every column is overwritten, not only the city: the after image is the whole row.',['merge']);

add('06 / MERGE',MT,MS,svg(mWire(0,'em')+mWire(1,'em')+mWire(2,'thin dashed'))+mLeft()+branch(0,'<b>found → UPDATE</b><br>every column, from the after image')+branch(1,'<b>not found → INSERT</b><br>a new row: Khalid, from the introduction')+c898(true)+c371(true)+c710(false)+label('found',640,MROW[2]-22,'ptr chip',160,'mt2'),
'<strong>Not found:</strong> INSERT a new row. Update or insert by key is called an <strong>upsert</strong>.',
'Khalid signed up on 26 September, after the snapshot, so the feed holds a create event (op c) and our copy has no row: MERGE inserts it. Two days later comes order 5043883, the introduction’s order. dlt’s merge disposition in D1_3 was this same upsert.',['merge','intro']);

add('06 / MERGE',MT,MS,svg(mWire(0,'em')+mWire(1,'em')+mWire(2,'danger'))+mLeft()+branch(0,'<b>found → UPDATE</b><br>every column, from the after image')+branch(1,'<b>not found → INSERT</b><br>a new row: Khalid, from the introduction')+branch(2,'<b>found, op = d → DELETE</b><br>the row goes')+c898(true)+c371(true)+c710(true),
'<strong>Found, and a delete:</strong> DELETE the row. One statement, three outcomes.',
'Customer 100710 changed district and mobile number on 6 September and was erased on 13 September. Only the latest change matters for the current state, and it is the delete, so the earlier updates are never applied. An updated_at query would never have seen this delete.',['merge']);

add('06 / MERGE','All of September in one statement','The same three branches, run on every customer’s latest change.',
tbl('merge-t',0,0,1440,'MERGE INTO customers USING latest USING (customer_id)','1fr 260px',[{c:['','customers'],cls:'h'},["WHEN MATCHED AND op = 'd' THEN DELETE",'139'],['WHEN MATCHED THEN UPDATE SET …','2,862'],["WHEN NOT MATCHED AND op &lt;&gt; 'd' THEN INSERT …",'1,521']],'mono')+
label('<b>6,060</b> in the snapshot − <b>139</b> + <b>1,521</b> = <b>7,442</b> ✓ the app’s count today',0,240,'check',1440,'m-sum')+
note('20 customers signed up and were erased within September: not found, op = d, so nothing to do.',0,310,'',1440)+note('Run it twice on the same changes: the second run changes nothing.',0,390,'teal',1440),
'<strong>MERGE</strong> updates, inserts and deletes by key, in one statement. DuckDB, Spark, Iceberg, Snowflake and BigQuery all run it.',
'The branches are checked top to bottom for each source row. The counts are Lab 1.4’s: of 4,542 latest changes, 139 delete a snapshot row, 2,862 update one, 1,521 insert a new customer, and 20 are deletes of customers created during September, which no branch matches. Run twice on the same batch, the MERGE changes nothing the second time: it is idempotent, so a re-run after a failure is safe.',['merge','lab']);

add('07 / Reconcile','Did we get it right? Compare with the app','Every column of every row, in both directions.',
box('app.customers','<div class="data-row"><span>rows</span><span>7,442</span></div>',0,20,380,'','rc-src','the app database, 30 September')+label('<b>✓</b> rows: 7,442 = 7,442',450,30,'check',560,'ck1')+label('<b>✓</b> missing (app EXCEPT copy): 0',450,95,'check',560,'ck2')+label('<b>✓</b> extra (copy EXCEPT app): 0',450,160,'check',560,'ck3')+box('Our copy','<div class="data-row"><span>rows</span><span>7,442</span></div>',1060,20,380,'silver','rc-copy','snapshot + MERGE')+
note('Order by arrival instead: missing 1, extra 1. It’s customer 100898.',0,300,'red',1100)+note('Equal counts aren’t enough: both orders give 7,442 rows.',0,370,'teal',1100),
'<strong>Reconcile:</strong> compare the copy with its source, row by row. The one test that catches 100898.',
'Lab 1.4 task 3: EXCEPT compares whole rows, so one wrong column in one row shows up as one missing and one extra. Equal counts alone would have passed: 7,442 rows either way. In production the same check runs on a schedule against a live source, so it compares as of the same lsn. D2_2 reconciles the fact table the same way.',['lab']);

// 08 · History
const HT='Marketing asks: how much did Dammam customers spend?',HS='Each order counted by where its customer lived at the time. Order 5019534 is customer 100898’s, from 27 August.';
const histJoin=()=>svg(path('M702 95 L996 95','danger','w-o'))+fin(0,30,200)+order534(260,60,440)+label('JOIN ON customer_id',740,40,'code',260,'j1')+copy(1000,60,440)+note('98.44 SAR from a Dammam customer, counted as Jeddah',1000,150,'red',440);
add('08 / History',HT,HS,histJoin(),
'Joined to our copy, <strong>a Dammam customer’s order counts as Jeddah</strong>: that is where 100898 lives now.',
'Order 5019534 is real: delivered on 27 August from DMM-01, 98.44 SAR. Joined to the current-state copy, it counts toward Jeddah customers, because that’s where the customer lives today. Sometimes that’s the right question (“where do our customers live now?”). For “how much did Dammam customers spend?”, we need the city they lived in when they ordered. If someone asks “why not use the store?”: the store answers where we sold, which is a different question. In September, 10% of delivered orders (2,173 of 21,005) came from a store in a different city from the one the customer lived in.',['lab']);

add('08 / History',HT,HS,histJoin()+label('before 17:15',260,264,'off',540,'tk-v1')+label('before 18:49',840,264,'off',600,'tk-v2')+v1(260,290,540,'src gone')+v2(840,290,600,'src gone')+note('MERGE overwrote these rows. The answer was in them.',260,390,'',1180),
'<strong>MERGE keeps only now.</strong> Questions about the past need the versions it overwrote.',
'Our copy went through the same overwrites as the app. The 1 September row, Dammam, is exactly what marketing needs for an August order, and the MERGE replaced it.');

// SCD2, version by version (slides 27–29); the same ids return on the as-of join.
const VER=[['v1','Dammam · Al Nuzha','2025-08-14 04:31','2026-09-02 17:15'],['v2','Dammam · Al Nuzha','2026-09-02 17:15','2026-09-02 18:49'],['v3','Jeddah · Al Hamra','2026-09-02 18:49','null · current']];
const versions=(n=3)=>VER.slice(0,n).map(([id,head,from,to],i)=>label(`version ${i+1}`,480*i,14,'off',440,`vn${i}`)+rec(id,480*i,40,440,head,`from ${from}<br>to ${i===n-1?'null · current':to}`,'','copy')).join('');
const VT='Keep every version instead',VS='Add a row for each change. A version is valid from its change until the next one.';
add('09 / History',VT,VS,versions(1)+note('From the 1 September snapshot. Nothing has changed it yet.',0,160,'',440),
'Version 1 comes from the snapshot. While nothing changes it, it is the <strong>current</strong> version.',
'A build of Lab 1.4 task 4. Snapshot rows become the first versions, valid from their updated_at: 100898 signed up on 14 August 2025 and hadn’t changed since. valid_to null means current.',['lab']);

add('09 / History',VT,VS,svg(path('M700 196 L700 146','em','w-e1'))+versions(2)+ev('e670',480,200,440)+note('Same city: only the email changed',0,160,'',440),
'A change <strong>closes</strong> the current version and <strong>opens</strong> a new one.',
'The 17:15 event starts version 2 at its commit time, and version 1 now ends there. Version 2 has the same city, because the change was to the email: an SCD2 row records every change, not only the ones a report cares about.',['lab']);

add('09 / History',VT,VS,svg(path('M700 196 L700 146','em','w-e1')+path('M1180 196 L1180 146','em','w-e2'))+versions(3)+ev('e670',480,200,440)+ev('e086',960,200,440)+
label('valid_to = lead(valid_from) OVER (PARTITION BY customer_id ORDER BY lsn)',0,330,'code',1440,'sql')+note('Each version ends where the next begins: no gaps, no overlaps. Ordered by lsn, so arrival order can’t spoil it.',0,400,'teal',1440),
'<strong>Three versions,</strong> and exactly one is valid at any moment.',
'lead() over lsn closes each version where the next begins, so out-of-order arrival doesn’t matter, and the duplicate was already dropped. A delete closes the last version and starts nothing.',['lab','windows']);

add('09 / History','A slowly changing dimension','A dimension describes things, such as customers. Its rows change now and then. How much history to keep is a choice.',
tbl('types',0,0,900,'Slowly changing dimensions (SCD)','80px 1fr 1fr',[{c:['type','keeps','answers'],cls:'h'},['0','the first value','city at sign-up'],['1','the latest value: our MERGE copy','city now'],{c:['2','a row per version','city at order time'],cls:'hit'}],'prose')+
label('<b>12,059</b> versions for 7,601 customers',960,10,'check',480,'sc1')+label('<b>7,442</b> current: one per live customer ✓',960,75,'check',480,'sc2')+note('Many teams keep Type 1 and Type 2 side by side.',960,150,'teal',480)+
note('Dimensions return on Day 2, when we model the warehouse.',0,300,'',1440),
'<strong>SCD Type 2:</strong> one row per version, with valid_from and valid_to. Type 1 is our MERGE copy.',
'Lab 1.4 task 4’s counts: 12,059 versions for 7,601 customers (the 159 erased customers have versions but no current one), and 7,442 current rows, one per customer in the app. Type 0 never changes (the sign-up city); Type 1 overwrites (the copy we built with MERGE); Type 2 keeps every version. The choice can differ per column: history for city, overwrite for a typo in a name.',['scd2','lab']);

add('09 / As-of join','Join each order to the version valid at its time','Order 5019534 was placed on 27 August. Which of customer 100898’s versions was valid then?',
svg(path('M220 248 L220 146','em','w-asof'))+versions()+order534(0,250,440)+label('✓ Dammam',240,180,'ptr teal',150,'asof-ok')+note('27 August falls inside version 1: 100898 lived in Dammam then.',520,265,'teal',920)+
label('ON o.customer_id = v.customer_id AND o.created_at &gt;= v.valid_from AND (o.created_at &lt; v.valid_to OR v.valid_to IS NULL)',0,410,'code',1440,'sql'),
'<strong>As-of join:</strong> match each event to the version that was valid when it happened.',
'Order 5019534 (27 August) falls inside version 1, so the as-of join gives Dammam, where the customer lived then. The join condition is on the board: the order time must be at or after the version’s start and before its end, or the version must still be open.',['lab']);

add('09 / As-of join','Marketing gets two answers, both correct','September’s delivered revenue by customer city, at order time and by current city (SAR).',
fin(0,30,200,'“Which one do I report?”')+tbl('by-city',300,0,1140,'September revenue by customer city','1fr 1fr 1fr 120px',[{c:['city','at order time','current city','diff'],cls:'h'},['Dammam','625,733','661,985','+5.8%'],['Jeddah','1,024,139','1,028,157','+0.4%'],['Riyadh','1,680,589','1,640,319','−2.4%']],'sums')+
note('650 customers moved city in September. The total stays 3,330,461 SAR; only the split moves.',300,250,'',1140)+note('If campaign budgets are split by customer city, a few percent is real money.',300,350,'teal',1140),
'<strong>As-of:</strong> where customers lived when they ordered. <strong>Current:</strong> where they live now. Where we sold comes from the store.',
'Lab 1.4 task 5. The totals are equal (3,330,461 SAR); 650 movers shift money between cities. Which number is right depends on the question, so the model should offer both. Orders of erased customers fall out of both joins: Day 2’s star schema keeps them with an Unknown row.',['lab']);

// 10 · Erasure
const ET='Compliance asks: is customer 100710 gone?',ES='Customer 100710 asked to be erased; the app deleted the row on 13 September. Our history still has it.';
const erased=()=>art('steward',0,20,190,'Compliance','“Erased everywhere?”','f-cmp')+label('version 1',230,14,'off',390,'xn0')+label('version 2',630,14,'off',390,'xn1')+label('version 3 · closed by the delete',1030,14,'off',410,'xn2')+
rec('x1',230,40,390,'Riyadh · Al Naseem','mobile 05•••••868<br>to 2026-09-06 11:38','','copy')+rec('x2',630,40,390,'Riyadh · Al Yasmin','mobile 05•••••868<br>to 2026-09-06 22:07','','copy')+rec('x3',1030,40,410,'Riyadh · Al Yasmin','mobile 05•••••360<br>to 2026-09-13 02:32','','copy');
add('10 / Erasure',ET,ES,erased()+note('Closed, not gone: three rows still hold the old mobile numbers',230,150,'red',1210),
'<strong>A delete closes the last version.</strong> The older versions still hold personal data.',
'Customer 100710 had three versions in September (a district move and a new mobile number, both on 6 September) and asked to be erased on 13 September. The SCD2 build closes the last version at the delete and keeps the rest: their old mobile numbers are still there.',['lab']);

add('10 / Erasure',ET,ES,erased()+note('Closed, not gone: three rows still hold the old mobile numbers',230,150,'red',1210)+
label('<b>DELETE</b> every version of every erased customer: 200 versions, 159 customers',230,250,'check',1210,'er1')+label('<b>and</b> in the bronze feed and in old snapshots (Lab 4.3)',230,310,'check',1210,'er2')+label('<b>✓</b> 11,859 versions remain: history kept for everyone else',230,370,'check',1210,'er3'),
'<strong>History meets erasure:</strong> under the PDPL, erased means every version, every layer, every snapshot.',
'The PDPL gives every data subject the right to have their data destroyed, so Lab 1.4 deletes every version of the 159 erased customers. The change feed in bronze holds the same data, and so do old snapshots: Lab 4.3 handles that on a lakehouse table.',['lab']);

const WORDS=[['CDC','learning every change a source makes'],['Log','where the database writes each change first'],['lsn','a change’s position in the log'],['Change event','one change: op, before, after, lsn'],['At-least-once','an event may arrive twice, or late'],['MERGE','update, insert or delete by key'],['Reconcile','prove the copy matches its source'],['Upsert','update the row if the key exists, insert it if not'],['SCD Type 2','one row per version, valid_from to valid_to'],['As-of join','join to the version valid at the time']];
add('11 / The words','Ten words for change and history','Every one appeared on a board today. The lab uses them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<5?0:740,(i%5)*100,'check',700,`w${i}`)).join(''),
'<strong>Order by lsn:</strong> most of the other words depend on it.',
'A reference board: point at each word and ask the room which board it appeared on, and what customer 100898 looked like there.');

const RECALL=[
 ['Customer 100710 is deleted in the app. Why does a query on updated_at never see it?','The row is gone, so no updated_at is left to find. Only the log records the delete.'],
 ['100898’s events arrive as #333, #347 and #360. Which one sets the current row?','lsn 5007086 (#347): the highest lsn once the repeat is dropped. Never simply the last arrival.'],
 ['Khalid (107371) signed up on 26 September. Which MERGE branch handles that change?','NOT MATCHED → INSERT: the 1 September snapshot has no row for 107371.'],
 ['How many SCD2 versions does 100898 have, and which one is current?','Three: two in Dammam, then Jeddah · Al Hamra from 2 Sep 18:49, with valid_to null.'],
 ['Order 5019534 (27 Aug, DMM-01): which city does the as-of join give? The current join?','As-of: Dammam, the version valid on 27 August. Current: Jeddah.']];
add('12 / Recall','Your turn: answer before you look','Five questions on today’s customers. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer: the guess that turns out wrong is what makes the right answer stick. The next slide fills in all five answers in place.');

add('12 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>Capture every change, order by lsn, MERGE the latest, keep the versions:</strong> the whole session in one line.',
'Walk through any answer the room missed by going back to its board: the three ways (6–8), arrival vs. lsn (14–16), MERGE (19–23), the versions (27–29) and the as-of join (31).');

add('Lab 1.4 · 25 min','From a CDC feed to an SCD2 dimension','Start from the 1 September snapshot, replay September’s changes, and end exactly where the app is.',
`<div class="references"><div><h3>Lab 1.4 · 25 minutes</h3><p>1 · Read the feed; find duplicates and late events · 4′</p><p>2 · Latest change per key; MERGE · 7′</p><p>3 · Reconcile with the app (0 differences) · 4′</p><p>4 · Build the SCD2 history · 7′</p><p>5 · As-of join: revenue by city at order time · 3′</p><p class="note">✱ Make the MERGE incremental: the feed in 4 batches. Find customer 100898 in the feed during task 1.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D1_4_cdc_merge_scd.ipynb</a><a href="${sources.windows}" target="_blank" rel="noopener">Lab 0 · SQL window functions</a><a href="${sources.debezium}" target="_blank" rel="noopener">Further reading · Debezium change events</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Tomorrow, Day 2: from bronze to silver.',
'Hand over to the lab. Suggest finding customer 100898 in the feed during task 1, and trying the arrival-order MERGE in task 3 to see the one difference appear. The stretch task shows why real CDC sinks remember deletes: a delete in an early batch, followed by an older update in a later batch, re-inserts the customer unless the sink remembers the delete’s lsn.',['lab','windows']);

Story.start({brand:'QAFILA / CDC STORY',sources});
})();
