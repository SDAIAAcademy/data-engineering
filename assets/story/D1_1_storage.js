/* D1_1 Storage for analytics, told on one evolving board (engine: story.js), one idea per board.
   One lesson over Day 1's first two sessions: part 1 (rows vs. columns, DuckDB) before the DuckDB walkthrough and Lab 1.1,
   part 2 (columns in a file: Parquet, row groups, statistics, partitions) before Lab 1.2.
   It opens where 00_story's OLTP/OLAP boards stopped: the app takes orders, Finance reads them all. How does OLAP read fast?
   The running example is Khalid's order 5043883 (00_intro.js) and its neighbours 5043882 and 5043884, as the app stores
   them (data/raw/qafila_oltp.sqlite, times in UTC). Part 2's numbers are what Lab 1.2's solution prints on the course data:
   3,450,900 order lines (×20), the CSV and Parquet sizes, task 3's 35 row groups of 100,352 rows, their footer and date ranges,
   the bytes per column chunk (parquet_metadata), 61 and 732 files. Part 1's column strips return as whole-file columns cut into row groups.
   Times (ms) are one laptop run: say "on this laptop". */
(() => {
'use strict';
const {art,label,note,svg,tool,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={parquet:'https://parquet.apache.org/docs/file-format/',duckdb:'https://duckdb.org/why_duckdb',story:'00_story.html',walk:colab('tools/duckdb.ipynb'),walkRead:'../labs/tools/html/duckdb.html',lab1:colab('D1_1_platform_tour.ipynb'),lab2:colab('D1_2_files_parquet.ipynb')};
const H=(...c)=>({c,cls:'h'});

// Three orders as the app stores them: 9 columns, times as UTC text. Khalid's is the middle row.
const COLS=['order_id','customer_id','store_id','status','payment','promo','fee','created_at','updated_at'];
const W=[100,95,90,110,70,84,70,140,140];
const ROWS=[['5043882','100476','RUH-01','delivered','card','—','9.00','09-28 14:03','09-28 15:09'],
 ['5043883','107371','RUH-05','delivered','card','—','9.00','09-27 22:35','09-27 23:07'],
 ['5043884','105277','RUH-01','delivered','card','—','9.00','09-28 17:49','09-28 18:56']];
const NEW=['5043885','107302','JED-01','delivered','card','FREEDEL','0.00','09-28 08:05','09-28 08:33'];
const NEED=[2,3];   // Finance's question: delivered orders per store
const cell=(id,v,x,y,w,cls='')=>label(v,x,y,`cell ${cls}`,w,id);
// Row layout: a header of names, then one strip per order. Column layout: one strip per column, its name on the left.
const RX=COLS.map((_,c)=>290+W.slice(0,c).reduce((a,b)=>a+b+6,0));
const rows=(cls=()=>'')=>COLS.map((n,c)=>cell(`h${c}`,n,RX[c],40,W[c],'n'+(cls(-1,c).includes('r')?' r':''))).join('')
 +ROWS.map((r,i)=>r.map((v,c)=>cell(`c${i}-${c}`,v,RX[c],90+i*50,W[c],cls(i,c))).join('')).join('');
const colY=c=>10+c*48,colX=(i,c)=>450+i*(W[c]+8);
const cols=(cls=()=>'',extra=[])=>COLS.map((n,c)=>cell(`h${c}`,n,290,colY(c),150,'n'+(cls(-1,c).includes('r')?' r':cls(-1,c).includes('x')?' x':''))).join('')
 +[...ROWS,...extra].map((r,i)=>r.map((v,c)=>cell(`c${i}-${c}`,v,colX(i,c),colY(c),W[c],cls(i,c))).join('')).join('');
const need=(i,c)=>NEED.includes(c)?'r':'';
const onlyNeed=(i,c)=>NEED.includes(c)?'r':'x';
const finance=(detail='“Delivered orders per store?”')=>art('analyst',1250,40,170,'Finance',detail,'f-fin');

// Part 2: Lab 1.2's file and its row groups. Dates are UTC, as the lab writes them.
const DAY0=Date.UTC(2026,7,1),PX=18;   // 1 August 00:00 at x=200, 18 px per day
const tx=s=>{const [d,t]=s.split(' '),[m,dd]=d.split('-').map(Number),[h,mi]=t.split(':').map(Number);return +(200+PX*(Date.UTC(2026,m-1,dd,h,mi)-DAY0)/864e5).toFixed(1);};
// The sorted file's 35 row groups, [min, max] of created_at (Lab 1.2 task 3, 100,352 rows each). In the shuffled file every row group spans
// 08-01 02:57–09-30 20:57 (to the minute; the differences are under a pixel), so it is drawn that way.
const SORTED=[['08-01 02:57','08-03 07:42'],['08-03 07:42','08-05 10:09'],['08-05 10:09','08-07 06:06'],['08-07 06:06','08-08 20:08'],['08-08 20:08','08-11 05:43'],['08-11 05:43','08-13 06:20'],['08-13 06:20','08-14 17:14'],['08-14 17:14','08-16 16:43'],['08-16 16:43','08-18 17:19'],['08-18 17:19','08-20 14:15'],['08-20 14:15','08-22 07:12'],['08-22 07:12','08-24 05:23'],['08-24 05:23','08-26 05:09'],['08-26 05:09','08-27 17:54'],['08-27 17:54','08-29 12:58'],['08-29 12:59','08-31 11:32'],['08-31 11:32','09-02 10:19'],['09-02 10:19','09-03 20:18'],['09-03 20:18','09-05 15:17'],['09-05 15:17','09-07 13:08'],['09-07 13:08','09-09 10:08'],['09-09 10:08','09-10 19:17'],['09-10 19:17','09-12 14:10'],['09-12 14:10','09-14 11:07'],['09-14 11:07','09-16 07:52'],['09-16 07:52','09-17 18:09'],['09-17 18:09','09-19 09:40'],['09-19 09:40','09-21 05:47'],['09-21 05:47','09-22 19:14'],['09-22 19:14','09-23 18:55'],['09-23 18:55','09-25 09:11'],['09-25 09:11','09-26 18:20'],['09-26 18:20','09-28 15:36'],['09-28 15:36','09-30 10:01'],['09-30 10:01','09-30 20:57']];
const SHUFFLED=SORTED.map(()=>['08-01 02:57','09-30 20:57']);
const BAND=[tx('09-23 00:00'),tx('09-24 00:00')];
const ranges=groups=>{const hit=([a,b])=>tx(b)>=BAND[0]&&tx(a)<BAND[1];return svg(`<rect x="${BAND[0]}" y="18" width="${BAND[1]-BAND[0]}" height="342" fill="#f3c76a" opacity=".45"/>`
 +groups.map((g,i)=>`<line x1="${tx(g[0])}" x2="${Math.max(tx(g[1]),tx(g[0])+3)}" y1="${30+i*9}" y2="${30+i*9}" stroke="${hit(g)?'#c46558':'#9fb0bb'}" stroke-width="5" stroke-linecap="round"/>`).join('')
 +`<line x1="200" x2="${tx('09-30 23:59')}" y1="362" y2="362" stroke="#61717c" stroke-width="2"/>`)
 +[['1 Aug','08-01 00:00'],['15 Aug','08-15 00:00'],['1 Sep','09-01 00:00'],['15 Sep','09-15 00:00'],['30 Sep','09-30 00:00']].map(([t,d],i)=>label(t,tx(d)-50,370,'tick',100,`tk${i}`)).join('')
 +label('23 Sep',BAND[0]-40,-12,'ptr',100,'band')+label('35 row groups<br>one line each',0,130,'caption',180,'rg-cap')
 +label(`<b>${groups.filter(hit).length} of 35</b>row groups to read`,0,410,'answer',420,'to-read');};

// ---------------------------------------------------------------- Part 1 · Rows and columns
add('Day 1 · Sessions 1–2','Storage for analytics','',
art('db',40,40,220,'Rows','','store')+art('warehouse',380,40,220,'Columns','','cover-cols')+svg(Story.path('M285 150 L360 150','em')),
'Why analytics stores data by column, and what that changes on disk.',
'One lesson over two sessions. Part 1 (about 12 minutes): rows vs. columns, then the DuckDB walkthrough (15) and Lab 1.1 (25). Part 2 after the break (about 15 minutes): columns in a file, Parquet, row groups, statistics and partitions, then Lab 1.2 (25). It starts where the story stopped: the app takes orders (OLTP), Finance reads them all (OLAP). Today: how an OLAP database reads fast. The three rows on the cover are real: Khalid’s order 5043883 and its neighbours, as the app stores them. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['story'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 1</p><h1>Storage for<br><span class="teal">analytics</span></h1><p class="lead">Khalid’s order,<br>stored two ways.</p>'});

add('00 / You are here','Where analytical data sits','Two pieces of the store stage: an engine that keeps columns, and a file that keeps columns.',
platform(n=>n==='DuckDB'||n==='Parquet'?'fresh':'met',{DuckDB:'A column database in your process',Parquet:'Columns in a file'}),
'<strong>Part 1:</strong> DuckDB and Lab 1.1. <strong>Part 2:</strong> Parquet and Lab 1.2.',
'The course map from the story with two cells lit. DuckDB is the course’s SQL engine: it runs the labs, it is dlt’s destination and dbt’s engine. Parquet is the file under almost everything that follows: Polars and Spark write it, and an Iceberg table (Day 3) is made of Parquet files. Both are column stores; that is today’s idea.');

add('01 / Where we stopped','The app takes orders. Finance reads them all.','The story’s last question: how does an analytical database read so much, so fast?',
rec('w-insert',0,40,520,'01:35 · Khalid’s order','the app writes 1 order and 2 lines','3 rows','order')+rec('w-change',0,140,520,'02:07 · the status changes','the app updates 1 row','1 row','chg')
 +label('<b>OLTP</b> · many small writes',0,250,'reason',520,'n-oltp')
 +label('<b>45,766 orders</b>read for one chart',700,40,'answer',420,'r-read')+label('<b>OLAP</b> · a few big reads',700,250,'reason',420,'n-olap')+finance('“Revenue per store per day?”')
 +note('The answer is in how the data sits on disk.',0,380,'teal',1100),
'<strong>Same orders, different work.</strong> The app’s database is built for the first; today is about the second.',
'A recap of the story’s two kinds of work (00_story boards 3–4), with the same numbers. Don’t re-teach it; ask the room for the two words and what each side needs. The rest of part 1 opens the database and looks at how the orders sit on disk.',['story']);

const RT='The app keeps each order together',RS='A row store: one order’s values side by side, then the next order. Khalid’s is the middle row.';
add('02 / Rows on disk',RT,RS,
rows()+art('db',30,60,180,'App database','a row store','store')+note('Show Khalid’s order: one place on disk, one read. ✓',290,270,'teal',920),
'<strong>Row store:</strong> each row stored together. One order, one read: right for the app.',
'A build: two boards. These are the real rows, as SQLite stores them (times in UTC, as the app writes them). On disk a row store writes order 5043882’s nine values, then 5043883’s, then 5043884’s. The app’s work fits: show an order, add an order, change its status. Each touches one row in one place.');

add('02 / Rows on disk',RT,RS,
rows(need)+art('db',30,60,180,'App database','a row store','store')+finance()+note('Finance needs 2 of 9 columns. A row store reads whole rows: all 9 values, for every one of 45,766 orders.',290,270,'red',920),
'<strong>A row store reads whole rows,</strong> even when the question needs two columns.',
'Finance’s question: delivered orders per store. It needs store_id and status, highlighted. But the values of one column are spread across every row, so the database reads every row, with all nine values, to collect them. 45,766 orders now; every year adds more, and wide tables have 50 columns, not 9.');

add('03 / Columns on disk','Store each column together','The same values, regrouped: all the order_ids, then all the customer_ids, and so on.',
cols()+art('warehouse',30,60,180,'A column store','DuckDB, Snowflake','store')+finance(),
'<strong>Column store:</strong> each column stored together. Same values, a different order on disk.',
'Watch the values move: nothing is added or lost, they are only regrouped. Each strip is one column for the three orders. On disk, one column’s values for many rows sit next to each other. Ask before the next click: which strips does Finance’s question need?');

add('03 / Columns on disk','Read only the columns you name','Finance’s question reads two strips. The other seven are never touched.',
cols(onlyNeed)+art('warehouse',30,60,180,'A column store','DuckDB, Snowflake','store')+finance()+note('2 of 9 columns read: about a fifth of the data',940,200,'teal',300),
'A column store <strong>reads only the columns a query names.</strong>',
'store_id and status are read; order_id, the customer, the times and the rest stay on disk. The wider the table, the bigger the win: Lab 1.2’s table has 13 columns, and Finance’s revenue by city reads 3. In the walkthrough, DuckDB reads the four columns Finance’s question names and skips the rest.',['duckdb']);

// Compression: the store_id of 12 orders in a row (5043882–5043893), as text, then as dictionary codes in order of first appearance.
const STORES=['RUH-01','RUH-05','RUH-01','JED-01','JED-01','RUH-04','RUH-06','RUH-04','RUH-02','JED-02','JED-03','JED-03'];
const DICT=[...new Set(STORES)];
add('03 / Columns on disk','Similar values sit side by side','A column holds one kind of value, and many repeat. Store each value once, then small numbers.',
cell('cn-a','store_id',60,40,130,'n')+STORES.map((s,i)=>cell(`st${i}`,s,200+i*96,40,88)).join('')
 +cell('cn-b','as codes',60,110,130,'n r')+STORES.map((s,i)=>cell(`cd${i}`,String(DICT.indexOf(s)),200+i*96,110,88,'r')).join('')
 +cell('cn-c','dictionary',60,180,130,'n')+DICT.map((s,i)=>cell(`dc${i}`,`${i} = ${s}`,200+i*144,180,136)).join('')
 +label('<b>0.44 bytes</b>per row for store_id in Lab 1.2’s file: 12 stores in 3.45 M rows',0,280,'answer',440,'b-store')+label('<b>3.25 bytes</b>per row for order_id: every value is different',490,280,'answer',440,'b-order')+label('<b>80 MB → 13 MB</b>the same orders, SQLite vs. DuckDB (walkthrough)',980,280,'answer',460,'b-file'),
'<strong>Compression:</strong> a column of repeats shrinks to almost nothing. Fewer bytes on disk, fewer bytes to read.',
'The store_id of twelve orders in a row, from 5043882. Only 8 distinct values here, 12 in the whole table. Dictionary encoding stores each store name once and the column becomes small codes, which compress further (runs of the same code, then ZSTD). A row store can’t do this well: a row mixes an ID, a name, a number and a date. The codes here are illustrative, in order of first appearance, as Parquet assigns them. The two byte counts are Lab 1.2 task 2; the file sizes are the walkthrough’s §1.',['parquet']);

add('04 / The trade-off','Columns make small writes harder','A new order is one row. A column store writes it in nine places.',
cols(i=>i===3?'w':'',[NEW])+art('warehouse',30,60,180,'A column store','DuckDB, Snowflake','store')+note('Order 5043885: 9 places to write. Change its status: rewrite part of a strip.',1080,60,'red',340)+note('A row store: one row, one place.',1080,300,'teal',340),
'<strong>Each job gets its own store:</strong> rows for the app (OLTP), columns for analytics (OLAP), and a copy between them.',
'The next order the app took, 5043885 at 08:05 UTC (a free-delivery promo), added to the column layout: one value at the end of each strip. Column stores take data in big batches, so a load copies many rows at once (Lab 1.3). They are poor at the app’s job of thousands of single-row writes and updates. That is why the app keeps its row store and analytics gets a copy: the warehouse from the story.');

add('05 / A learner decision','Which query gains most from a column store?','All three run on Qafila’s orders table, 9 columns, 45,766 rows.',
finance('“Which one is mine?”')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>Show order 5043883, every column</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>Count delivered orders per store</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Insert the next order</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Commit to one, then say what each query reads.</p>',
'Think about what each query reads, not how big it sounds.',
'Allow 45 seconds. B is right: it reads 2 of 9 columns across every row, so it skips most of the data and the repeats compress. A reads one whole row: a row store does that in one place, a column store in nine. C writes one row: the trade-off board.',[],{quiz:{answer:'B',right:'Yes: 2 of 9 columns, every row. A and C are OLTP work.',wrong:'Count what it reads: one row, or a few columns of every row? Try again.'}});

add('05 / The engines','Row stores and column stores you will meet','DuckDB is the course’s column store. It runs inside your Python process, like SQLite.',
tbl('engines',0,10,1440,'Databases by the work they are built for','230px 200px 140px 1fr 1fr',[H('Database','Runs','Stores','Built for','At Qafila'),
 ['SQLite','in your process','rows','an app’s own data','the app database (the labs’ copy)'],
 ['PostgreSQL, MySQL','a server','rows','many users writing small transactions','what a real app would run'],
 {c:['DuckDB','in your process','columns','analytics on one machine','the warehouse, every lab'],cls:'hit'},
 ['Snowflake, BigQuery','a managed service','columns','analytics for a whole company','the same SQL at company scale']],'prose')
 +note('Nothing to install but a Python package; no server to run.',0,250,'teal',1440),
'<strong>DuckDB:</strong> a column store that runs inside your process. The labs run it on a laptop or in Colab.',
'Read the table by its Stores column: two row stores for the app’s work, two column stores for questions. DuckDB and SQLite both live inside the program that uses them (in-process), so there is no server to install or manage. DuckDB also processes a column in batches of about 2,048 values on every CPU core (vectorized), which the walkthrough mentions. Snowflake and BigQuery are the same idea as a shared service.',['duckdb']);

const RECALL1=[
 ['Finance counts delivered orders per store. Why does a row store read all 9 columns?','A column’s values are spread across every row, so it reads every whole row to collect two of them.'],
 ['Why does store_id cost under half a byte per row in a column store?','Its values repeat: a dictionary stores each store once, and the column becomes small codes that compress.'],
 ['Why does the app keep its row store?','Its work is single-row writes and reads, one place each; a column store would write one order in nine places.']];
add('06 / Recall','Part 1: answer before you look','Three questions on Khalid’s orders table.',
recall(RECALL1),
'<strong>Say it without the board,</strong> then see it on your laptop.',
'About 2 minutes. Give the room 20 seconds per question and take an answer before the next slide fills them in.');

add('06 / Recall','Part 1: the answers','Check yours, then open the walkthrough.',
recall(RECALL1,true),
'<strong>Rows for the app, columns for questions.</strong> Now prove it on a laptop.',
'Go back to the board for any answer the room missed: the row store (4–5), columns and compression (6–8), the trade-off (9).');

add('Walkthrough + Lab 1.1','DuckDB, then the platform tour','Ask Finance’s question of a million orders, twice. Then point DuckDB at Qafila’s real sources.',
`<div class="references"><div><h3>Walkthrough · DuckDB · 15 minutes</h3><p>1 · Finance’s question: SQLite vs. DuckDB</p><p>2–4 · Connections, files in place, tables and views, one writer</p><h3>Lab 1.1 · 25 minutes</h3><p>Query every source in place, check keys and formats, start the issue log.</p><p class="note">Part 2 after the break: the same columns, in a file.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.walk}" target="_blank" rel="noopener">Walkthrough · tools/duckdb.ipynb</a><a href="${sources.walkRead}" target="_blank" rel="noopener">Read it with every output</a><a href="${sources.lab1}" target="_blank" rel="noopener">Lab · D1_1_platform_tour.ipynb</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Predict before §1 runs: how much faster, and is DuckDB’s file bigger or smaller?',
'Hand over to the walkthrough. Its §1 is the proof of part 1: the same question on 1M orders, about a second in SQLite and tens of milliseconds in DuckDB, from a file about six times smaller. Ask for the prediction before running it. Then Lab 1.1. Come back to the next board after the break.',['lab1']);

// ---------------------------------------------------------------- Part 2 · Columns in a file
// Lab 1.2's table: Khalid's two order lines (real IDs; the other 3,450,898 lines are Qafila's ×20, shuffled).
const PCOLS=['order_id','line_no','created_at','store_id','city','status','payment_method','category','product','quantity','unit_price','discount','amount'];
const PTYPE=['bigint','bigint','timestamp','varchar','varchar','varchar','varchar','varchar','varchar','bigint','double','double','double'];
const KH=[['5043883','1','2026-09-27 22:35:37','RUH-05','Riyadh','delivered','card','Fruits & Vegetables','Sahra Cucumbers','1','4.74','0.0','4.74'],
 ['5043883','2','2026-09-27 22:35:37','RUH-05','Riyadh','delivered','card','Dates & Coffee','Al Waha Arabic Coffee','2','37.88','0.0','75.76']];
const BY_CITY=[4,5,12];   // revenue by city: city, status, amount
const csvLine=vals=>vals.map((v,c)=>BY_CITY.includes(c)?`<span class="k-key">${v}</span>`:v).join(',');
add('07 / Columns in a file','The lake keeps files. A CSV is a row store in text.','Lab 1.2’s table: 3,450,900 order lines (Qafila’s ×20), 13 columns. Khalid’s two lines, as CSV:',
label([csvLine(PCOLS),...KH.map(csvLine),'… 3,450,898 more lines'].map(l=>`<div>${l}</div>`).join(''),0,10,'code',1440,'csv')
 +label('<b>Rows</b><br>Revenue by city needs 3 values per line; the engine reads all 13',0,190,'reason',380,'why-1')+label('<b>Text</b><br>every number and date is parsed from characters, on every query',420,190,'reason',380,'why-2')+label('<b>No types</b><br>guessed from a sample, like the walkthrough’s “free”',840,190,'reason',360,'why-3')
 +art('analyst',1270,180,160,'Finance','“Revenue by city?”','f-fin'),
'<strong>Part 2:</strong> the same idea, rows vs. columns, for the files in the lake.',
'Welcome back. Part 1 was a database; the lake from the story holds files, and the most common file is a CSV. A CSV is a row store written as text: one line per row, every value as characters. Finance’s revenue by city needs city, status and amount, highlighted: three values scattered along every line, so the engine reads every whole line. Three costs follow, one per label. Lab 1.2 replicates Qafila’s 172,545 order lines 20 times (new order IDs) and shuffles them, as data often arrives; Khalid’s two lines keep their real IDs.',['lab2']);

const pct=(v,max)=>Math.max(1,Math.round(100*v/max));
const fbar=(id,name,v,max,unit,cls='')=>label(`<b>${name}</b><span class="track"><span style="width:${pct(v,max)}%"></span></span><i>${v.toLocaleString('en')} ${unit}</i>`,0,0,`bar wide ${cls}`,920,id);
const at=(html,y)=>html.replace('left:0px;top:0px',`left:0px;top:${y}px`);
add('07 / Columns in a file','The same lines as Parquet: 7.8× smaller, 16× faster','Lab 1.2 task 1 writes all 3,450,900 lines both ways, then runs Finance’s revenue by city on each file.',
label('Size on disk',0,0,'caption',400,'cap-size')+at(fbar('s-csv','CSV',399.8,399.8,'MB','bad'),35)+at(fbar('s-pq','Parquet',51.5,399.8,'MB','mine'),80)
 +label('Revenue by city, on this laptop',0,170,'caption',400,'cap-ms')+at(fbar('q-csv','CSV',726,726,'ms','bad'),205)+at(fbar('q-pq','Parquet',45,726,'ms','mine'),250)
 +note('7.8× smaller',990,55,'teal',440)+note('16× faster',990,225,'teal',440)+note('Same rows, same answer. What is different inside the file?',0,350,'',1440),
'<strong>Parquet</strong> is the lake’s file format for analytics. The next board opens one.',
'Lab 1.2 task 1, one run. Same 3,450,900 lines, same answer (the lab asserts it). Ask the question on the board before moving on; part 1 already holds the answer. Parquet here uses ZSTD compression. The lab also writes a Snappy file: 70.3 MB and the same 45 ms. ZSTD is smaller and, on a laptop SSD, about as fast; on object storage, where you pay per byte read, smaller wins. The times differ per machine; the ratio is the point. Other formats you’ll hear: JSON lines (rows, text: events and APIs), Avro (rows, binary: Kafka messages), ORC (columns, the Hive era), Arrow (columns in memory: how DuckDB, Polars and PyIceberg hand tables to each other without copying).',['parquet','lab2']);

// Khalid's two lines as column strips, in two blocks: one strip per column, its name and type on the left.
const PB=[[0,180,[0,1,2,3,4,5,6]],[790,200,[7,8,9,10,11,12]]];   // [x, value width, columns]
const pname=(c,cls='')=>cell(`pn${c}`,`${PCOLS[c]}<small>${PTYPE[c]}</small>`,0,0,230,'n'+cls);
const pcols=(cls=()=>'')=>PB.map(([x,vw,cs])=>cs.map((c,j)=>pname(c,cls(c)?' '+cls(c):'').replace('left:0px;top:0px',`left:${x}px;top:${60+j*46}px`)
 +KH.map((r,i)=>cell(`pv${i}-${c}`,r[c],x+240+i*(vw+8),60+j*46,vw,cls(c))).join('')).join('')).join('');
add('08 / Inside a Parquet file','Why: Parquet stores each column together, with its type','Khalid’s two lines inside the Parquet file: one strip per column, as in part 1’s column store.',
svg(Story.outline(-16,0,1472,400,'blue','pq-file'))+label('lines.parquet',20,12,'hand blue',400,'pq-title')+pcols(c=>BY_CITY.includes(c)?'r':'')
 +note('Revenue by city reads 3 strips of 13, already typed: nothing to parse.',0,425,'teal',1440),
'<strong>Parquet:</strong> a column store in one file, each column with its type. Part 1’s idea, on disk in the lake.',
'The same values as the CSV, regrouped: all the order_ids, then all the line numbers, and so on. Point at the small word after each name: the type is stored in the file (bigint, timestamp, varchar, double), so a reader never guesses and never parses text. Finance’s three strips are highlighted; the other ten are never read. Each strip is also compressed on its own, as in part 1: city has 3 values, so it costs almost nothing.',['parquet']);

// A whole column of 3,450,900 values as one strip. Six of the 13 columns are drawn. Row groups hold 100,352 rows (Lab 1.2 task 3).
const SCOLS=[0,2,3,4,5,12],SX=240,SW=1060,SY=i=>40+i*46,RG=100352,NROWS=3450900;
const cutX=k=>+(SX+SW*k*RG/NROWS).toFixed(1);
const strips=()=>SCOLS.map((c,i)=>pname(c).replace('left:0px;top:0px',`left:0px;top:${SY(i)}px`)+label('',SX,SY(i),'strip',SW,`sp${c}`)).join('');
const chunk=(g,i)=>`<rect x="${cutX(g)}" y="${SY(i)}" width="${cutX(g+1)-cutX(g)}" height="34" fill="#348f8d" opacity=".55"/>`;
const cuts=(marks='')=>svg(marks+Array.from({length:34},(_,k)=>`<line x1="${cutX(k+1)}" x2="${cutX(k+1)}" y1="${SY(0)-6}" y2="${SY(5)+40}" stroke="#61717c" stroke-width="1.6"/>`).join(''))
 +label('row group',0,4,'caption',200,'ru-a')+[0,1,2,3].map(g=>label(String(g),cutX(g),6,'tick',cutX(g+1)-cutX(g),`rgt${g}`)).join('')
 +label('…',cutX(4),6,'tick',cutX(34)-14-cutX(4),'rgt-more')+label('34',cutX(34)-14,6,'tick',40,'rgt34');
const LT='A big file is cut into row groups',LS='Six of the 13 columns of Lab 1.2’s file, each drawn as one strip of 3,450,900 values.';
add('08 / Inside a Parquet file',LT,LS,
strips()+label('row 1',SX,4,'caption',200,'ru-a')+label('row 3,450,900',SX+SW-200,4,'caption right',200,'ru-b')
 +note('One strip per column: the writer would hold all 3.45 M rows before writing a byte, and a reader could only take a whole column.',0,350,'red',1440),
'A whole column as one piece is <strong>too big to write or read at once.</strong>',
'A build: two boards. The strips glide in from the last board: the same columns, now for all 3,450,900 lines. Ask the room: if each column were one piece, what would the writer have to keep in memory before it could write the first strip? Everything. And a reader that wants one day would have to take a whole column.');

add('08 / Inside a Parquet file',LT,LS,
strips()+cuts(chunk(0,1))
 +label('<b>Row group</b><br>a slice of 100,352 rows. The writer fills one, writes each column’s piece of it, then starts the next: 35 in this file.',0,350,'reason',680,'e-rg')
 +label('<b>Column chunk</b><br>one column of one row group (shaded), compressed on its own. A query reads only the chunks it needs.',740,350,'reason',700,'e-ch'),
'<strong>Row group:</strong> a slice of rows. <strong>Column chunk:</strong> one column inside it. Still columns, in pieces.',
'Each strip is cut at the same rows: row group 0 is rows 1 to 100,352 of every column, and so on; the last one holds the 38,932 left over. On disk the file is row group 0’s chunks, then row group 1’s. Inside a row group the columns stay apart, so part 1 still holds. Row groups are also the unit an engine reads in parallel and the unit it can skip. Lab 1.2 task 3 sets 100,000 rows per group (DuckDB rounds to 100,352); task 2’s file uses DuckDB’s default of 122,880, so it has 29. Spark aims at about 128 MB per group. Ask: what would an engine need to know to skip a row group without opening it?',['parquet']);

add('08 / Inside a Parquet file','The footer: a min and max for every chunk','At the end of the file, a small index: the schema, then each column chunk’s smallest and largest value.',
strips()+cuts(chunk(0,1)+chunk(0,2)+chunk(1,1))+label('footer',SX+SW+16,SY(0),'foot',124,'foot')+svg(Story.path(`M${SX+SW+78} ${SY(5)+44} Q${SX+SW+78} 345 1008 352`,'em'))
 +tbl('footer',SX,330,760,'footer · read first','120px 140px 1fr 1fr',[H('row group','column','min','max'),['0','created_at','08-01 02:57','09-30 20:48'],['0','store_id','DMM-01','RUH-06'],['1','created_at','08-01 02:57','09-30 20:57']],'mono')
 +label('<b>Statistics</b><br>if a chunk’s range misses the filter, the engine skips it without reading it',1040,385,'reason',400,'e-ft'),
'<strong>Statistics:</strong> each chunk’s min and max, kept in the footer. The engine reads them first and skips what can’t match.',
'The footer rows are real: parquet_metadata on Lab 1.2 task 3’s shuffled file; the three shaded chunks are the three rows. An engine reads the footer first, then fetches only the chunks it needs. A filter like created_at on 23 September can skip any row group whose created_at max is before the 23rd or whose min is after it. Don’t say yet whether this file skips anything: that is the next board’s question.',['parquet']);

add('09 / A learner decision','How many row groups does one day read?','Finance wants revenue on 23 September. The file has 35 row groups, each with a min and max of created_at.',
finance('“Just National Day, please.”')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>1 or 2: the ones that hold 23 September</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>About a third of them</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>All 35</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Commit to one. The last board’s footer has the clue.</p>',
'The statistics are there. Are they any use?',
'Allow 45 seconds; most of the room will say A. C is right: Lab 1.2 shuffled the rows, as data often arrives, so every row group holds lines from 1 August to 30 September, and every range includes the 23rd. The footer on the last board showed it: row group 0 runs from 08-01 to 09-30. Statistics only rule out a row group whose range misses the filter. The days are UTC, as the lab writes them.',[],{quiz:{answer:'C',right:'Yes. The rows are shuffled, so every row group spans both months. The next board draws all 35.',wrong:'Look at the footer: row group 0 runs from 1 August to 30 September. Does that range include the 23rd? Try again.'}});

const ST='Sort, and the statistics start to skip',SS='Each line is one row group’s min–max of created_at, read from the footer (Lab 1.2 task 3). The band is 23 September.';
add('10 / Skipping',ST,SS,ranges(SHUFFLED)+label('Shuffled',1340,10,'hand muted',100,'which'),
'<strong>Shuffled:</strong> every row group covers both months, so every one overlaps the day. 35 of 35 read.',
'A build: two boards. The shuffled file first: every line runs from 1 August to 30 September, so every line crosses the band and must be read. This is the quiz’s answer, drawn.',['lab2']);

add('10 / Skipping',ST,SS,ranges(SORTED)+label('Sorted',1340,10,'hand teal',100,'which'),
'<strong>Sort by the filter column</strong> and the ranges become narrow: 2 of 35 row groups read, the same answer.',
'The same rows, sorted by created_at before writing. Each row group now covers about two days, and only the two that touch 23 September are read: groups 29 (22 Sep 19:14 to 23 Sep 18:55) and 30. On this laptop the query took 14 ms instead of 59, and the sorted file is also smaller (22.7 MB vs. 52.0): sorted values compress better. Table formats (Day 3) offer sort orders and clustering for exactly this.',['lab2']);

const DATES=['08-01','…','09-22','09-23','09-24','…','09-30'];
add('11 / Partitions','Folders by date: skip before opening a file','Lab 1.2 task 4 adds order_date (created_at’s date) and writes one folder per day. The date sits in the folder name.',
label(['<div><span class="dir">lake/lines/</span></div>',...DATES.map(d=>d==='…'?'<div>&nbsp;&nbsp;…</div>':`<div${d==='09-23'?' class="hit"':''}>&nbsp;&nbsp;<span class="dir">order_date=2026-${d}/</span>data_0.parquet</div>`)].join(''),0,20,'tree',640,'tree')
 +label('<b>Why, if sorting already skips?</b><br>No other file is even opened, and a day’s load can replace one folder.',0,250,'reason',640,'why-part')
 +label("WHERE order_date = '2026-09-23'",720,30,'code',720,'q-part')+label('<b>1 of 61 files</b>opened, before any footer is read',720,90,'answer',600,'one-file')
 +label("WHERE created_at on the same day",720,250,'code',720,'q-ts')+label('<b style="color:#b96355">61 of 61 files</b>opened: the engine can’t tell which folder holds the day',720,310,'answer',700,'all-files'),
'<strong>Partition:</strong> a folder per value. Engines skip folders only when you filter on the partition column itself.',
'Lab 1.2 task 4 and the stretch. order_date is a new column, created_at cast to a date, and it lives in the folder name (Hive-style: order_date=2026-09-23), not inside the file. Filter on order_date and the engine opens one folder; ask for the same day through created_at and it opens all 61 and checks their footers. dbt models and dashboards often filter on the timestamp and lose the benefit. Iceberg’s hidden partitioning (Day 3) removes the trap. The folder dates are UTC, as in the lab.',['lab2']);

const sq=n=>'<span></span>'.repeat(n);
add('11 / Partitions','Too many folders: the small-files problem','“Partition by date and store, for faster store reports.” Lab 1.2 task 5 measures it.',
label('<b>1 file</b> · sorted, 22.7 MB',0,0,'caption',600,'l1')+label(sq(1),0,32,'files one',900,'f1')
 +label('<b>61 files</b> · one per day, 35.4 MB',0,110,'caption',600,'l2')+label(sq(61),0,142,'files',900,'f2')
 +label('<b>732 files</b> · one per day and store, 31.6 MB: about 43 kB each',0,220,'caption',900,'l3')+label(sq(732),0,252,'files wrap',900,'f3')
 +label('Full scan, on this laptop',1000,0,'caption',440,'cap-scan')+label('<b>58 ms</b>one file',1000,40,'answer',440,'t1')+label('<b>99 ms</b>61 files',1000,140,'answer',440,'t2')+label('<b style="color:#b96355">414 ms</b>732 files',1000,250,'answer',440,'t3')
 +note('Every file costs an open, a footer read and, on S3, a request.',0,420,'red',1440),
'<strong>Partition by what you load and filter by,</strong> usually a date. Keep files large; sort for the rest.',
'Each small square is one real file Lab 1.2 wrote. 732 files of about 43 kB: the work per file (list, open, read the footer, schedule) outweighs the data. Store filters were already fast in one file, thanks to dictionary encoding and statistics. Typical targets: files of 128 MB to 1 GB. Streaming creates small files by design; compaction rewrites them into big ones (Day 3).',['lab2']);

// What the engine reads for revenue on 23 September: whole files, then the compressed chunks of the columns it needs (parquet_metadata).
const FUNNEL=[['f-csv','CSV',399.8,'every line, as text','bad'],['f-pq','Parquet',51.5,'<b>Compression</b>: typed, repeats stored once',''],['f-col','Columns',21.2,'<b>Columns</b>: created_at and amount, 2 of 13',''],['f-st','Statistics',0.25,'<b>Statistics</b>: sorted file, 2 of 35 row groups',''],['f-pt','Partitions',0.17,'<b>Partitions</b>: 1 of 61 files, amount only','mine']];
add('12 / Four ways to read less','The fastest data is the data you never read','Finance’s revenue on 23 September: the megabytes the engine reads as each trick is added.',
FUNNEL.map(([id,name,v,what,cls],i)=>at(fbar(id,name,v,399.8,'MB',cls),i*62)+label(what,960,i*62,'check',480,`${id}-what`)).join('')
 +note('SELECT * undoes columns. A shuffled file undoes statistics. A filter on created_at undoes partitions.',0,330,'red',1440)
 +label('<b>Pushdown</b> · the engine pushes the query’s columns and filter into the scan, so the skipping happens while it reads.',0,430,'reason',1440,'push'),
'<strong>Read less:</strong> 399.8 MB of CSV down to under 0.3 MB, for the same answer.',
'A recap before the lab, on one question. The first two bars are whole files (Lab 1.2 task 1); the others are the compressed size of the column chunks the engine needs (parquet_metadata): created_at and amount in the ZSTD file, the same two in the 2 row groups of the sorted file, and amount alone in the one folder for 23 September. The footer reads add a little. Engines call the first and third tricks projection and predicate pushdown; Lab 1.2’s EXPLAIN ANALYZE shows both in the scan box. Each trick has its trap, on the red line.');

const WORDS=[['OLTP','many small writes: the app'],['OLAP','a few big reads: the questions'],['Row store','each row stored together'],['Column store','each column stored together'],['Dictionary encoding','each value once, then codes'],['In-process','a database inside your program'],['Parquet','a column store in a file'],['Row group','a slice of rows in a file'],['Statistics','min and max per column chunk'],['Partition','a folder per value'],['Small files','many tiny files: slow reads'],['Pushdown','columns and filters applied in the scan']];
add('13 / The words','Twelve words for storage','Every one appeared on a board today. The labs use them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>Columns first:</strong> most of the other words follow from them.',
'A reference board: point at each word and ask which board it appeared on.');

const RECALL2=[
 ['Why is the Parquet file 7.8× smaller than the CSV, with the same rows?','Columns compress: repeats sit together and get a dictionary; values are binary and typed, not text.'],
 ['Why does a Parquet file have row groups, if it stores columns?','A whole column is too big to write or read at once. Each row group holds every column for a slice of rows.'],
 ['A one-day query reads all 35 row groups. What do you change?','Sort by created_at when writing: then 2 of 35 are read, chosen by the footer’s min and max.'],
 ['The table is partitioned by order_date, yet a one-day query opens 61 files. Why?','The filter is on created_at, not order_date. Filter on the partition column.'],
 ['Why not partition by date and store?','732 tiny files: a full scan is several times slower. Partition by date; let statistics handle the store.']];
add('14 / Recall','Part 2: answer before you look','Five questions on Lab 1.2’s file.',
recall(RECALL2),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. The next slide fills in the answers in place.');

add('14 / Recall','Part 2: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL2,true),
'<strong>Columns, compression, statistics, partitions:</strong> four ways to read less.',
'Go back to the board for any answer the room missed: Parquet (16–17), row groups and the footer (18–20), sorting (22–23), partitions (24–25).');

add('Lab 1.2 · 25 min','Parquet in practice','Store the same 3.45 M lines four ways and measure what changes.',
`<div class="references"><div><h3>Lab 1.2 · 25 minutes</h3><p>1 · CSV vs. Parquet: size and scan time · 6′</p><p>2 · Read the footer: row groups, statistics · 5′</p><p>3 · Sorted vs. shuffled: row groups skipped · 5′</p><p>4 · Partition by date; query one day · 5′</p><p>5 · Over-partition on purpose · 4′</p><p class="note">✱ Prove the partition trap in the plan: 1 file vs. 61.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab2}" target="_blank" rel="noopener">Lab · D1_2_files_parquet.ipynb</a><a href="${sources.parquet}" target="_blank" rel="noopener">Further reading · the Parquet file format</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next, after a 20-minute break: Session 3 · batch ingestion with dlt.',
'Hand over to the lab. Every exercise is SQL: parquet_metadata is a table like any other. Task 2 reads the footer of the task 1 file, which uses DuckDB’s default row groups: expect 29 of them, not 35, and store_id at 0.44 bytes per row. Task 3 rewrites the table with smaller row groups: the 35 from the boards.',['lab2','parquet']);

Story.start({brand:'QAFILA / STORAGE STORY',sources});
})();
