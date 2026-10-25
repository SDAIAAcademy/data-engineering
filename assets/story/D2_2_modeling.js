/* D2_2 Dimensional modeling, told on one evolving board (engine: story.js), one idea per board.
   The running example is Khalid's order 5043883 from the course introduction (00_intro.js): two lines and a 9 SAR fee.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every row and number is real: data/raw/qafila_oltp.sqlite, api/payments.jsonl, events/delivery_events.jsonl and the
   star schema Lab 2.2's solution builds (labs/solutions/D2_2_dimensional_modeling.ipynb). Only the stock levels are illustrative. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,tool,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={kimball:'https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/',dbt:'https://docs.getdbt.com/docs/build/models',intro:'00_intro.html',lab:colab('D2_2_dimensional_modeling.ipynb')};

const fee=(id,x,y,cls='')=>label('fee 9.00',x,y,`fee ${cls}`,110,id);
const K=v=>`<em class="k">${v}</em>`,BAD=v=>`<em class="k bad">${v}</em>`;
const H=(...c)=>({c,cls:'h'});

// Khalid's two order lines: [line_no, product_id, name, quantity, unit_price, net_amount, product_key].
const LINES={l1:[1,1104,'Sahra Cucumbers',1,'4.74','4.74',104],l2:[2,1193,'Al Waha Arabic Coffee',2,'37.88','75.76',193]};
const line=(id,x,y,w,body=([,,name,q,price])=>`${name} · ${q} × ${price}`,amount=true,cls='')=>{const l=LINES[id];return rec(id,x,y,w,`5043883 · line ${l[0]}`,typeof body==='function'?body(l):body,amount?l[5]:'',`line ${cls}`);};
const order=(x,y,w,body,amount='')=>rec('ord',x,y,w,'order 5043883',body,amount,'order');

// The question (slides 3–5): one row per follow-up question. A row holds the question, its total and what Khalid's order adds.
const ASK=[['Add up every order line?','7,343,425 SAR','172,545 lines, whatever happened to the order','both lines','80.50'],
 ['But 5,347 orders never reached a customer. Leave them out?','6,494,684 SAR','delivered orders only','delivered, so it counts','80.50'],
 ['Is the delivery fee a sale too?','6,843,884 SAR','delivered, plus 349,200 SAR of delivery fees','lines + the 9.00 fee','89.50']];
const asks=n=>art('analyst',0,60,190,'Finance','“How much did we sell?”','f-fin')+ASK.slice(0,n).map(([q,t,d,kb,ka],i)=>note(q,240,i*165+5,'teal',330)+label(`<b>${t}</b>${d}`,600,i*165,'answer',480,`a${i+1}`)+rec(`k${i+1}`,1110,i*165+10,330,'Khalid’s order',kb,ka,'order')).join('');

// Two grains, two tiers (measures): the order on top, its lines below, the fee chip beside the order.
const tiers=(extra='')=>svg(outline(288,0,1150,96,'blue','grain-o')+outline(288,176,1150,182,'teal','fct')+extra)+label('<b>Order grain</b><br>one row per order',0,20,'reason',270,'tier-o')+label('<b>Line grain</b><br>one row per order line',0,215,'reason',270,'tier-l')+order(300,13,600,'RUH-05 · delivered')+line('l1',300,190,600)+line('l2',300,275,600)+fee('fee',920,30);

// The fan-out (slides 16–18): the two inputs on the left, the joined rows on the right.
const joinIn=()=>tbl('j-items',0,0,520,'fct_order_items','120px 90px 1fr',[H('order_id','line_no','net_amount'),[K('5043883'),'1','4.74'],[K('5043883'),'2','75.76']],'mono')+tbl('j-orders',0,250,520,'orders','120px 1fr',[H('order_id','delivery_fee'),[K('5043883'),'9.00']],'mono');
const joined=(sum=false)=>tbl('joined',700,60,740,'fct_order_items JOIN orders','120px 90px 1fr 1fr',[H('order_id','line_no','net_amount','delivery_fee'),['5043883','1','4.74',BAD('9.00')],['5043883','2','75.76',BAD('9.00')],...(sum?[{c:['SUM','','80.50 ✓',BAD('18.00 ✗')],cls:'total'}]:[])],'mono');
const joinWires=path('M524 80 C610 80 610 140 694 140','em','w-j1')+path('M524 330 C620 330 630 150 694 148','danger','w-j2')+path('M524 336 C630 336 640 178 694 176','danger','w-j3');

// The star (dimensions, one join): the fact in the middle, a dimension at each corner.
const DIMS={
 date:fade=>tbl('dim-date',0,0,380,'dim_date','100px 1fr',[['20260928','Monday 28 Sep · weekday'],{c:['','month · week · National Day'],cls:'more'}],fade),
 product:(fade,hit='')=>tbl('dim-product',1060,0,380,'dim_product','50px 1fr',[{c:['104','Cucumbers · Fruits & Veg.'],cls:hit},{c:['193','Arabic Coffee · Dates & Coffee'],cls:hit}],fade),
 store:fade=>tbl('dim-store',0,300,380,'dim_store','50px 1fr',[['11','Al Naseem · Riyadh']],fade),
 customer:fade=>tbl('dim-customer',1060,300,380,'dim_customer','70px 1fr',[['7214','Riyadh · Al Malqa'],{c:['','no name, mobile or national ID'],cls:'more'}],fade)};
const starWires=(used=()=>'em')=>path('M418 185 C395 185 405 70 384 70',used('date'),'w-date')+path('M418 355 C395 355 405 355 384 355',used('store'),'w-store')+path('M1022 185 C1045 185 1035 70 1056 70',used('product'),'w-product')+path('M1022 355 C1045 355 1035 375 1056 375',used('customer'),'w-customer');
const fact=()=>outline(420,110,600,300,'teal','fct');
const factLines=body=>label('fct_order_items',440,120,'hand teal',300,'fct-title')+line('l1',440,185,560,body('l1'),true)+line('l2',440,290,560,body('l2'),true);

// Missing customers (keys): order 5000037's customer was erased from the app; Khalid's line still finds its row.
const custLines=(lost,known='customer_id 107371',erased='customer_id 104353')=>line('l1',0,20,560,known)+rec('l37-1',0,140,560,'5000037 · line 1',erased,'12.84',`line ${lost}`)+rec('l37-2',0,225,560,'5000037 · line 2',erased,'43.54',`line ${lost}`);
const custDim=(unknown=false)=>tbl('dim-customer',860,20,580,'dim_customer','150px 150px 1fr',[H('customer_key','customer_id','city'),{c:['7214','107371','Riyadh'],cls:'hit'},['7215','107372','Riyadh'],...(unknown?[{c:['-1','-1','Unknown'],cls:'unk'}]:[])],'mono');

// Fact types (slides 26–29).
const cell=(id,x,y,head,body,amount,cls)=>rec(id,x,y,230,head,body,amount,cls);
const sales=()=>label('<b>Sales</b><br>RUH-05, delivered lines per day',0,10,'reason',290,'ft-t')+cell('t1',320,15,'26 Sep','121 lines','4,645.48','line')+cell('t2',570,15,'27 Sep','116 lines','5,603.11','line')+cell('t3',820,15,'28 Sep','88 lines','4,400.65','line')+note('Add them: 14,649.24 ✓',1080,25,'teal',360);
const accRow=(cells,cls='')=>tbl('acc',260,0,1180,'fct_deliveries · one row per order','120px 1fr 1fr 1fr 1fr 1fr 1fr',[H('as of','order_id','placed','assigned','picked_up','delivered','minutes'),{c:cells,cls}],'mono');
const ops=()=>art('operations',0,40,200,'Operations','“Which orders miss 30 minutes?”','f-ops');

add('Day 2 · Session 2','Dimensional modeling','',
svg(path('M58 112 C58 150 62 172 78 172','thin')+path('M58 112 C58 220 62 258 78 258','thin')+path('M470 225 C510 225 530 240 548 262','em'))+order(40,40,380,'RUH-05 · delivered')+fee('fee',430,57)+line('l1',80,140,380,'Cucumbers · 1 × 4.74')+line('l2',80,225,380,'Arabic coffee · 2 × 37.88')+art('analyst',490,230,190,'“How much did we sell?”','','asker'),
'Decide what one row means before deciding how to count sales.',
'About 25 minutes for the board, then Lab 2.2 (25 minutes). Yesterday ended with the app’s tables landed and cleaned. Today we design the gold layer: tables shaped around the questions the teams ask. The opening story’s journey left gold as a promise between Khalid’s clean record and the answer (Riyadh, 28 September: 52,090.75 SAR); this session fills it in. Khalid’s order from the introduction, 5043883 (28 September, Riyadh), runs through the whole board: two lines and a 9 SAR delivery fee. The introduction promised that the order line would matter on Day 2; this is where. Many boards are builds: the heading stays and one piece arrives per click, so pause on each step. Every number comes from the course data and Lab 2.2’s star schema; the only invented numbers are the stock levels. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['kimball','intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 2</p><h1>Dimensional<br><span class="teal">modeling</span></h1><p class="lead">Khalid’s order,<br>one row at a time.</p>'});

add('00 / You are here','Clean rows still need a shape','Silver holds clean records. Gold arranges them around the questions people ask.',
platform(n=>n==='dbt'?'fresh':'met',{dbt:'Design the gold layer'}),
'<strong>Modeling designs the gold layer.</strong> dbt, next session, builds what we design today.',
'The course map from the story, with dbt lit because the models we design today become its marts in D2_3. This session needs no tool: dimensional modeling is a design method, and Lab 2.2 builds the result in plain SQL in DuckDB. Doing it by hand first shows which decisions are ours and which a tool automates.',['dbt']);

const QT='Finance asks: how much did we sell?',QS='August and September, the same order lines. Every follow-up question changes the total.';
add('01 / The question',QT,QS,asks(1),
'Answer 1: <strong>add up every line.</strong> Simple. But is a cancelled order a sale?',
'A build: three boards, one follow-up question each. Start with the obvious answer: add quantity × unit_price − discount over every order line, 1 August to 30 September: 7,343,425 SAR. Khalid’s order adds its two lines, 80.50. Ask the room before the next click: is anything wrong with this number?');

add('01 / The question',QT,QS,asks(2),
'Answer 2: <strong>only orders that reached a customer.</strong> Next: is the delivery fee a sale?',
'5,347 orders never became a sale: 3,266 cancelled, 964 refunded, 1,117 failed delivery. Leave them out and the total drops by about 850,000 SAR. Khalid’s order was delivered, so it stays at 80.50.');

add('01 / The question',QT,QS,asks(3),
'Three totals, all correct. <strong>The question is incomplete</strong> until someone says what counts.',
'Add the 9 SAR fees of the delivered orders (349,200 SAR) and the total rises again; Khalid’s order now counts 89.50, which is what he paid. Nobody made a mistake: each total answers a slightly different question. Ask the room which one belongs on the CEO’s dashboard, and why. The next board writes the answer down.');

add('01 / The definition','Write the definition down, once','Every team that asks “how much did we sell?” gets the number from the same rule.',
svg(path('M195 150 L410 150','em','w-fin')+path('M1245 150 L1030 150','em','w-ops'))+art('analyst',0,40,190,'Finance','','f-fin')+art('operations',1250,40,190,'Operations','','f-ops')+
box('Product revenue','<p>the net amount of <b>delivered</b> order lines</p><p>Delivery fees: a separate number</p><p>City: the <b>store’s</b> city</p>',420,20,600,'gold','definition','written in the model, not in each report')+
label('<b>6,494,684 SAR</b>for everyone, every time',420,330,'answer center',600,'a2'),
'<strong>Business definition:</strong> the agreed meaning of a number. A model writes it down once.',
'Answer 2 glides to the middle: it is Lab 2.2’s definition. Product revenue counts delivered lines only; fees are reported beside it, not inside it. “By city” needs a decision too: the customer’s city and the store’s city differ on 7,899 of 44,726 orders with a known customer, so we choose the store’s. If a customer moves, do past sales move too? Here no (Type 1); keeping history was SCD2 in D1_4. Once the rule lives in the model, finance and operations stop arguing about numbers and start arguing about the business.');

add('02 / The source','Khalid’s order, as the app stores it','Day 1 landed the app’s tables as they are. The sale is spread over four of them.',
tbl('t-orders',0,0,700,'raw.orders','110px 105px 120px 100px 1fr',[H('order_id','status','customer_id','store_id','delivery_fee'),[K('5043883'),'delivered','107371',K('RUH-05'),'9.00']],'mono')+
tbl('t-items',0,170,700,'raw.order_items','110px 80px 120px 90px 1fr',[H('order_id','line_no','product_id','quantity','unit_price'),[K('5043883'),'1',K('1104'),'1','4.74'],[K('5043883'),'2',K('1193'),'2','37.88']],'mono')+
tbl('t-products',760,0,680,'raw.products','110px 1fr 190px',[H('product_id','name_en','category'),[K('1104'),'Sahra Cucumbers','Fruits & Vegetables'],[K('1193'),'Al Waha Arabic Coffee','Dates & Coffee']])+
tbl('t-stores',760,205,680,'raw.stores','110px 1fr 120px',[H('store_id','name_en','city'),[K('RUH-05'),'Qafila Al Naseem','Riyadh']])+
note('The line keeps the price Khalid paid: 37.88. The coffee’s list price today is 37.97.',0,410,'teal',1440),
'<strong>The app splits a sale across tables:</strong> names in products, cities in stores, quantity and price on the line.',
'The same rows the introduction showed, now as the warehouse holds them (raw.*, loaded by dlt in Lab 1.3). The app is built to take orders, so each fact lives in exactly one place: good for the app, awkward for questions, which need several tables at once. order_id + line_no is the source’s primary key. net_amount is not a source column: we derive it. products.unit_price is today’s list price, so historical sales always use order_items.unit_price.');

// Why model (slide 8): the app's line can't answer the questions alone. O marks a value from the order.
const O=v=>`<em class="o">${v}</em>`;
const srcPair=()=>tbl('t-items',0,0,640,'raw.order_items','100px 70px 100px 80px 95px 1fr',[H('order_id','line_no','product_id','quantity','unit_price','discount'),[K('5043883'),'1','1104','1','4.74','0.00'],[K('5043883'),'2','1193','2','37.88','0.00']],'mono')+
tbl('t-orders',700,0,740,'raw.orders','100px 175px 85px 110px 1fr',[H('order_id','created_at (UTC)','store_id','customer_id','status'),[K('5043883'),'2026-09-27 22:35','RUH-05','107371','delivered']],'mono');
const ASKS=[['When?','weekends, National Day','created_at'],['Where?','which store, which city','store_id'],['Who?','new or returning customers','customer_id'],['Does it count?','delivered lines only','status']];
add('02 / The source','The questions need the order too','The line says what was bought. The teams also ask when, where, who, and whether it counts.',
srcPair()+ASKS.map(([q,d,col],i)=>label(`<b>${q}</b><br>${d}`,i*370,200,'reason',330,`ask${i}`)+note(`Not on the line: raw.orders.${col}`,i*370,280,'red',330)).join('')+
note('Every report would redo the join, the filter and the clock: 22:35 UTC on 27 Sep is 28 Sep in Riyadh.',0,400,'',1440),
'<strong>The app is shaped for OLTP: taking orders, not answering questions.</strong> Left alone, every report rebuilds the same joins and fixes.',
'Why we model, part two. The opening story named the two kinds of work: OLTP takes orders, OLAP asks about them. Day 1 gave the questions their own copy (the warehouse) and stored it by column; today we change its shape. Part one was the three totals: the same question gave different numbers until someone wrote the definition down. Part two is the shape. The app splits a sale for its own needs (board 7): the line keeps what was bought, the order keeps when, where, who and the status. Every question today needs both: the definition counts delivered lines only (status), purchasing asks by category and by weekend (the date), finance by city (the store). Ask the room where each answer lives. Left to each analyst, every report rebuilds the same joins, the same delivered filter and the same time-zone fix, and some get one wrong: forget the shift and Khalid’s order lands on 27 September, a Sunday, instead of Monday the 28th.');

// The destination (slide 9): the star before its pieces have names. Same ids as the full star (slide 22), so it returns there.
const PREVIEW={
 date:()=>tbl('dim-date',0,0,380,'date · when?','1fr',[['Monday 28 Sep · weekday'],{c:['month · week · National Day'],cls:'more'}],'prose'),
 product:()=>tbl('dim-product',1060,0,380,'product · what?','1fr',[['Cucumbers · Fruits & Veg.'],['Arabic Coffee · Dates & Coffee']],'prose'),
 store:()=>tbl('dim-store',0,300,380,'store · where?','1fr',[['Al Naseem · Riyadh']],'prose'),
 customer:()=>tbl('dim-customer',1060,300,380,'customer · who?','1fr',[['Riyadh · Al Malqa'],{c:['signed up: new or returning'],cls:'more'}],'prose')};
add('02 / The model','Data modeling: shape the tables for the questions','The sales sit in the middle with their numbers. What people ask about sits around them, one join away.',
svg(fact()+starWires())+label('the sales',440,120,'hand teal',300,'fct-title')+line('l1',440,185,560)+line('l2',440,290,560)+PREVIEW.date()+PREVIEW.product()+PREVIEW.store()+PREVIEW.customer()+
note('Today we build it, one decision at a time.',420,428,'teal',600),
'<strong>Data model:</strong> tables shaped for questions. <strong>Star schema:</strong> the events in the middle, descriptions around.',
'The destination, shown before we build it. The sales sit in the middle (Khalid’s two lines), and each corner answers one question from the last board: when (date), what (product), where (store), who (customer). Whether a line counts travels on the line itself. Don’t define the pieces yet: the session builds them one decision at a time. First, what one row in the middle stands for (grain); then which numbers belong there (measures, and the fan-out trap); then why the descriptions sit outside (dimensions) and how they connect (keys). Kimball calls this a dimensional model; the star is its usual shape.',['kimball']);

const GT='One row per what?',GS='The middle of the star could hold these sales three ways. Where does Khalid’s order end up in each?';
const grains=()=>label('<b>One row per order</b>',0,0,'reason',440,'g-order')+order(0,70,440,'RUH-05 · 2 lines','80.50')+label('<b>One row per order line</b>',500,0,'reason',440,'g-line')+line('l1',500,70,440)+line('l2',500,155,440)+label('<b>One row per store and day</b>',1000,0,'reason',440,'g-day')+rec('day',1000,70,440,'RUH-05 · 28 Sep','25 delivered orders','4,400.65','day');
add('03 / Grain',GT,GS,grains()+note('Which table would you build? Purchasing will ask about products.',0,400,'teal',1440),
'Before naming a single column, finish the sentence: <strong>“one row per ___”</strong>.',
'Ask the room to pick one before the next click. All three are legitimate tables; the question is which one the teams’ questions need.',['kimball']);

add('03 / Grain',GT,GS,svg(outline(485,-12,470,310,'frame','pick'))+grains()+note('One row: which products? Gone',0,170,'red',440)+note('Two rows: every product kept',500,250,'teal',440)+note('Khalid’s order is one of 25 inside this number',1000,170,'red',440)+note('Lines add up to orders or days. A day never splits back into lines.',0,400,'',1440),
'<strong>Grain:</strong> what one row of a table stands for. Take the finest grain the questions need: here, the order line.',
'The order table loses the products; the daily table loses the order itself (RUH-05 delivered 25 orders worth 4,400.65 SAR on 28 September). Both are fine summaries, and both can be built from lines later. So the sales fact is one row per order line. “Sales data” or “one transaction” is not a grain: it must name the thing one row stands for.',['kimball']);

add('03 / Grain','Our fact table: one row per order line','fct_order_items. Its key is order_id + line_no, and a key is a claim the data must pass.',
svg(outline(300,0,820,330,'teal','fct'))+label('fct_order_items',330,15,'hand teal',400,'fct-title')+label('one row per order line',760,22,'muted',330,'fct-grain')+line('l1',330,75,760,'product 1104 · 1 × 4.74 − discount 0')+line('l2',330,160,760,'product 1193 · 2 × 37.88 − discount 0')+label('<b>80.50</b> SAR',330,260,'sum',760,'sum')+
label('<b>The key</b><br>order_id + line_no',0,75,'reason',270,'key')+box('Test the claim','<div class="data-row"><span>lines</span><span>172,545</span></div><div class="data-row"><span>keys</span><span>172,545</span></div>',1140,0,300,'silver','test','✓ the grain holds')+
note('net_amount = quantity × unit_price − discount',330,380,'teal',760),
'<strong>Fact table:</strong> one row per business event, at the declared grain. Test it: as many rows as distinct keys.',
'Khalid’s two rows in the fact. The source discounts are zero, so the net amounts are 4.74 and 75.76: 80.50 SAR. Lab 2.2 task 1 counts rows and distinct (order_id, line_no) pairs: 172,545 each, so the grain holds. Test that the key columns are never null as well. Why not order_id + product_id? One order can hold the same product on two lines.',['lab']);

// Why a new table (slide 13): the source line keeps its rows, and gains what its order knows.
add('03 / Grain','Same rows as raw.order_items. Why a new table?','Because each line also needs what its order knows: when, where, who, and whether it counts.',svg(path('M320 145 L320 238','em','w-items')+path('M1070 112 C1070 190 820 180 800 238','em','w-orders'))+srcPair()+
tbl('fct-rows',0,250,1040,'fct_order_items','100px 65px 130px 80px 110px 100px 95px 80px 1fr',[H('order_id','line_no','date (Riyadh)','store_id','customer_id','status','product_id','quantity','net_amount'),['5043883','1',O('2026-09-28'),O('RUH-05'),O('107371'),O('delivered'),'1104','1',K('4.74')],['5043883','2',O('2026-09-28'),O('RUH-05'),O('107371'),O('delivered'),'1193','2',K('75.76')]],'mono')+
label('<b>172,545 rows</b>the source’s rows: each line has one order',1090,250,'answer',350,'same-rows')+note('<span class="o-key">blue</span> from the order · <span class="k-key">green</span> worked out once',0,400,'',1040)+note('The 9.00 fee stays on the order. Next board.',1090,400,'teal',350),
'<strong>Same rows, more columns:</strong> each line carries what its order knows. The model joins once, so no report has to.',
'The fact is order_items joined to orders, once, in the model. Every line has exactly one order, so the join adds columns, not rows: still 172,545, which the reconcile board proves (Lab 2.2 section 3). The date is already in Riyadh time, the status travels with each line so “delivered only” is a simple filter, and net_amount is worked out once instead of in every report. The delivery fee does not come along: it happens once per order, and the next board shows why that matters. Two more reasons not to report from raw.* directly: it has the app’s shape and changes when the app changes, and it is bronze, the landing copy. In section 06, store_id and customer_id become keys to dimensions.',['lab']);

add('04 / Measures','Every number has its own grain','Net amounts happen once per line. The 9 SAR delivery fee happens once per order.',
tiers()+note('The fee sits with the order, not with any one line.',300,390,'teal',1140),
'<strong>Measure:</strong> a number you add up. It belongs where it happens once: the fee per order, the net amount per line.',
'Khalid pays his fee once, however many lines his order has. So the fee is a measure of an order-grain fact (dbt’s fct_orders in D2_3), not of the line fact. Quantity, discount and net amount happen per line. You could allocate the fee to lines (say, in proportion to line value), but that is a business rule with its own checks; copying the full fee onto each line is not allocating it. Next: what happens if someone does exactly that.');

add('05 / A learner decision','Finance wants revenue including fees','A colleague joins orders onto the order lines and adds up net_amount + delivery_fee. What does Khalid’s order add?',
art('analyst',1100,40,270,'Finance','“Products plus fees, please.”','f-fin')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>89.50: the lines plus one fee</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>98.50: the lines plus two fees</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>80.50: the fee is lost in the join</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: after the join, how many rows does the order have?</p>',
'The query is valid SQL. It runs without an error, and a dashboard would show its result.',
'Let learners commit before anyone clicks. B is right, and most people pick A: they picture the order once, not the joined rows. The next three boards show why, one step at a time.',['lab'],{quiz:{answer:'B',right:'Yes. The next boards show why, step by step.',wrong:'Picture the joined rows: how many does Khalid’s order get? Try again.'}});

add('05 / Fan-out','A join makes one row per match','Khalid’s order matches two lines. Each joined row carries the order’s columns, the fee included.',
svg(joinWires)+joinIn()+joined()+note('The fee belongs to the order, but now it sits on every line.',700,250,'red',740),
'<strong>A join repeats</strong> the one side’s columns once for every match on the many side.',
'Step 1 of the reveal. The join is on order_id. The order row matches two line rows, so the result has two rows, and each copies the order’s columns: here, delivery_fee 9.00 twice. This is exactly what a join is supposed to do. The problem appears only when someone adds the copied column up.');

add('05 / Fan-out','Now add the columns up','SUM cannot know that the two 9.00s are the same fee.',
svg(joinWires)+joinIn()+joined(true)+label('<b style="color:#b96251">98.50</b>what the query reports',700,300,'answer',340,'q-total')+rec('pay',1080,310,360,'pay_9040078','captured from Khalid’s card','89.50','copy'),
'<strong>Fan-out:</strong> joining a coarser grain onto a finer one repeats its numbers. Nothing fails; the total is just bigger.',
'Step 2. SUM(net_amount) is right, 80.50, because net_amount was already at line grain. SUM(delivery_fee) is 18.00 for a 9.00 fee. 80.50 + 18.00 = 98.50, while the payment provider captured 89.50 (payment pay_9040078 from the introduction). Nothing in the SQL is wrong; the grain is.',['lab']);

add('05 / Fan-out','At Qafila’s scale, nobody notices','The same query over every delivered order. No error, no warning: just a bigger number on the dashboard.',
art('analyst',0,40,200,'Finance','“Revenue is up?”','f-fin')+label('<b>6,843,884 SAR</b>products + fees, each fee counted once',300,0,'answer',560,'s-right')+label('<b style="color:#b96251">7,812,473 SAR</b>the same, through the fan-out query',300,165,'answer',560,'s-wrong')+note('968,589 SAR of fees that were never charged',300,340,'red',640)+
note('Where else could it bite?',960,10,'teal',480)+rec('w-pay',960,70,480,'payments','several attempts per order','','src')+rec('w-evt',960,170,480,'delivery events','about four per order','','src'),
'<strong>The fan-out hides in plain sight:</strong> an order has 3.8 lines on average, so its fee is counted 3.8 times.',
'Step 3. Lab 2.2 task 5 reproduces this: the naive query says 7,812,473 SAR, the right answer is 6,843,884 SAR. 349,200 SAR of real fees become 1,317,789 SAR. This is the most common way dashboards end up wrong, because nothing fails. The same trap waits wherever a table has several rows per order: payment retries, delivery events.',['lab']);

const FT='Roll the lines up, then add the fee',FS='Bring both sides to one row per order before combining them.';
const fixBase=()=>label('fct_order_items · one row per line',0,10,'hand teal',460,'fl-title')+line('l1',0,90,440)+line('l2',0,175,440)+label('fct_orders · one row per order',560,10,'hand blue',600,'fo-title')+order(560,132,330,'items_amount','80.50');
add('05 / The fix',FT,FS,
svg(outline(-12,70,465,200,'teal','fct')+outline(545,70,895,200,'blue','grain-o')+path('M442 125 C500 125 500 162 552 162','em','w-r1')+path('M442 210 C500 210 500 170 552 170','em','w-r2'))+fixBase()+note('GROUP BY order_id: two lines become one row, 80.50',0,330,'teal',1100),
'Step 1: <strong>add the lines up</strong> to one row per order. Now both sides share a grain.',
'Group the lines by order_id: items_amount = 4.74 + 75.76 = 80.50. The order side is already one row per order. Now a join cannot repeat anything: one row meets one row.');

add('05 / The fix',FT,FS,
svg(outline(-12,70,465,200,'teal','fct')+outline(545,70,895,200,'blue','grain-o')+path('M442 125 C500 125 500 162 552 162','em','w-r1')+path('M442 210 C500 210 500 170 552 170','em','w-r2')+path('M1022 167 L1056 167','em','w-t1'))+fixBase()+fee('fee',906,149)+rec('tot',1060,132,370,'order 5043883','items + fee','89.50','day')+label('<b>89.50</b> SAR ✓ what Khalid paid',1060,290,'sum',380,'sum')+note('dbt’s fct_orders is built this way (D2_3)',0,330,'teal',1000),
'<strong>Combine facts at a shared grain:</strong> add the lines up first, join second.',
'Step 2: join the rolled-up lines to the order and add the fee once: 89.50, the amount the payment provider captured. The dbt project’s fct_orders does exactly this with its lines CTE. SUM(DISTINCT delivery_fee) looks like a shortcut but is wrong: two different orders that both pay 9.00 would count one fee. Lab 2.2 task 5 asks for this fix.');

add('06 / Dimensions','Why not keep the names on the line?','Put everything people ask about on each fact row, and the same words repeat thousands of times.',
tbl('wide',0,0,1440,'fct_order_items, with every description on the row','130px 130px 1fr 190px 130px 90px 100px 80px',[H('order · line','date','product','category','store','city','district','net'),['5043883 · 1','Mon 28 Sep','Sahra Cucumbers','Fruits & Vegetables','Al Naseem','Riyadh','Al Malqa','4.74'],['5043883 · 2','Mon 28 Sep','Al Waha Arabic Coffee','Dates & Coffee','Al Naseem','Riyadh','Al Malqa','75.76']])+
label('<b>8,650</b>lines repeat “Al Naseem, Riyadh”',0,230,'answer',440,'n-store')+label('<b>31,437</b>lines repeat “Fruits & Vegetables”',500,230,'answer',440,'n-cat')+label('<b>1</b>renamed category: all of its lines must change',1000,230,'answer',440,'n-rename')+
note('And “weekend” or “National Day” is in no column at all.',0,400,'',1440),
'Descriptions repeat on every line. <strong>Store each one once</strong>, and keep only a key on the line.',
'This wide row is what many people build first, and it works for one report. But the store’s name and city are copied onto every one of its 8,650 lines, and a category onto every line of every product in it; when a description changes, thousands of rows change. And some descriptions people want, such as “weekend” or “National Day”, are not stored anywhere: they have to be worked out from the date. The next board moves the descriptions out. (A wide table like this, built on top of the star, is the “one big table” of the lab’s stretch task: a convenience, not the source of truth.)');

add('06 / Dimensions','Descriptions move out to dimensions','The star from the start, filled in: the fact keeps keys and numbers, and each description is stored once around it.',
svg(fact()+starWires())+factLines(id=>({l1:'keys 20260928 · 104 · 11 · 7214',l2:'keys 20260928 · 193 · 11 · 7214'})[id])+label('date · product · store · customer',700,128,'caption',300,'key-order')+DIMS.date('')+DIMS.product('')+DIMS.store('')+DIMS.customer(''),
'<strong>Dimension:</strong> a table of descriptions to filter and group by. <strong>Star schema:</strong> the fact in the middle, one join to each dimension.',
'The star from board 9 returns, now with what the wide row taught. Each fact row holds four keys and its measures; each description is stored once. The keys are surrogate keys the warehouse assigns (Lab 2.2 numbers them in source-ID order; Khalid, customer 107371, is key 7214); the source IDs stay in the dimension. The date key is the Riyadh date of the order: the app wrote 27 September 22:35 UTC, which is 28 September 01:35 in Riyadh, a Monday. dim_date also flags weekends and National Day (23 September). Why a table, when date functions could work these out? Because the calendar is a business decision, written down once like the revenue definition. Qafila’s weekend is Friday and Saturday (dayofweek IN (5, 6) in DuckDB, where Sunday is 0); an analyst who types the familiar IN (0, 6) counts Sunday, a working day, and the query runs without an error. Some dates can’t be worked out at all: Ramadan and the two Eids follow the Hijri calendar and move every year, so they have to be listed in the table. And a LEFT JOIN from dim_date keeps the days a store sold nothing, which questions about average daily revenue need. dim_customer keeps city, district and signup date, and no direct identifiers: analytics doesn’t need Khalid’s name or mobile number.',['kimball']);

add('06 / One join','Purchasing asks: what sells, by category?','Join the fact to dim_product, group by its category, add up net_amount.',
svg(fact()+starWires(d=>d==='product'?'em':'thin')+path('M1250 162 L1250 296','em','w-result'))+factLines(id=>`product_key ${LINES[id][6]}`)+DIMS.date('faint')+DIMS.product('','hit')+DIMS.store('faint')+tbl('result',1060,300,380,'Result','1fr auto',[['Fruits & Vegetables','4.74'],['Dates & Coffee','75.76']],'gold sums')+
note('On every delivered line, Dates & Coffee leads: 1.75 M SAR',420,428,'teal',1020),
'<strong>Star queries all look alike:</strong> group by dimension columns, add up fact measures.',
'The product key joins each line to exactly one product row, so nothing fans out: the dimension is at a coarser grain, but it brings descriptions, not numbers. Its category does the grouping. Run on every delivered line, the same query puts Dates & Coffee first (1,748,436 SAR), then Meat & Poultry (1,478,681 SAR): Lab 2.2 section 4. Purchasing pairs this with the supplier price lists from the introduction to find products that lose margin. By city means joining dim_store; by weekend, dim_date.',['lab']);

add('07 / Keys','An inner join silently drops sales','Some customers asked to be erased. Their orders stay, but their customer_id matches no row in dim_customer.',
svg(path('M562 55 C700 55 720 110 856 110','em','w-c243')+path('M562 175 C650 175 680 190 740 190','danger dashed','w-c37a')+path('M562 260 C650 260 680 200 740 200','danger dashed','w-c37b'))+custLines('lost')+label('✕',748,178,'ptr red',30,'miss')+custDim()+
note('customer_key: the warehouse’s own ID, a <b>surrogate key</b>',860,175,'',580)+art('steward',1270,250,150,'Compliance','erases on request','f-cmp')+note('Inner join: 1,040 orders, 3,930 lines and 163,787 SAR disappear, with no error',0,340,'red',800),
'<strong>Missing references are normal</strong>, here erased customers. The model must still keep the sale.',
'Order 5000037 from JED-04 on 1 August: its customer 104353 was later erased from the app (a PDPL erasure request, like the 159 in September from the introduction), so the inner join finds no customer row and drops both lines. Khalid’s line still finds key 7214. Across the data: 1,040 orders, 3,930 lines, 163,786.82 SAR of revenue, gone without any error, and the totals by city quietly stop matching finance. The two IDs: customer_key belongs to the warehouse (it stays stable, and SCD2 needs one per version), customer_id to the app.',['lab']);

add('07 / Keys','An “Unknown” row keeps every sale','Add one member with key −1 before loading facts. A line without a match points there.',
svg(path('M562 55 C700 55 720 110 856 110','em','w-c243')+path('M562 175 C700 175 720 176 856 176','em','w-c37a')+path('M562 260 C700 260 720 184 856 184','em','w-c37b'))+custLines('','customer_id 107371 → key 7214','customer_id 104353 → key −1')+custDim(true)+
label('LEFT JOIN dim_customer … COALESCE(customer_key, -1)',0,340,'code',800,'sql')+note('3,930 lines stay, grouped under “Unknown”: count them and watch the number',0,400,'teal',800),
'<strong>Unknown member:</strong> a dimension row for “no match”. A LEFT JOIN plus a fallback key, and no sale disappears.',
'Lab 2.2 inserts (-1, -1, \'Unknown\', \'Unknown\', NULL) into dim_customer before building the fact, and the fact uses COALESCE(c.customer_key, -1). Every line keeps a valid reference, inner joins are safe again, and “Unknown” shows up honestly in reports. The lab checks that the -1 rows exist (3,930 lines). Monitor that count: a sudden jump means a source problem, not a burst of privacy requests.',['lab']);

const AT='Can you add it up across days?',AS='Two tables, one row per day each. Watch what a total means.';
add('08 / Fact types',AT,AS,sales()+note('Sales are events: each day adds new ones.',0,400,'',1440),
'<strong>Transaction fact:</strong> one row per event. Adding across days is always meaningful.',
'Transaction fact: RUH-05’s real delivered lines, summed per day (Khalid’s coffee is in the 28 September number). New events add rows, and any sum over any days is meaningful: 14,649.24 SAR for the three days. Before the next click, ask: does every daily number behave like this?',['kimball']);

add('08 / Fact types',AT,AS,sales()+
label('<b>Stock</b><br>Arabic coffee on RUH-05’s shelf',0,215,'reason',290,'ft-s')+cell('s1',320,220,'26 Sep','in stock','40','day')+cell('s2',570,220,'27 Sep','in stock','34','day')+cell('s3',820,220,'28 Sep','in stock','30','day')+label('Illustrative: Qafila’s data has no stock table',320,300,'caption',730,'c-s')+note('Add them: 104? There were never 104.',1080,230,'red',360)+
note('Sales are events: each day adds new ones. Stock is a level: each day restates it.',0,400,'',1440),
'<strong>Periodic snapshot:</strong> a level per day. It adds across stores on one day, never across days.',
'Periodic snapshot: one row per product, store and day with the level at that moment. Stock is semi-additive: add across stores on one date, but never across dates; for a period, take the last value (30) or the average. The stock numbers are illustrative: there is no inventory table.',['kimball']);

const CT='One row per order, filled in as it moves',CS='Operations asks: how long from order to door? Khalid’s delivery, as one row.';
add('08 / Fact types',CT,CS,ops()+accRow(['01:36','5043883','01:35',K('01:36'),'—','—','—'])+label('The row exists from the start. Empty columns wait for their event.',260,115,'caption',1180,'acc-cap'),
'The courier is assigned at 01:36: <strong>the same row</strong> gets its next column.',
'An accumulating snapshot has one row per order. It appears when the order is placed (01:35:37) and the courier app’s events fill it in: assigned 01:36:34.');

add('08 / Fact types',CT,CS,ops()+accRow(['01:54','5043883','01:35','01:36',K('01:46'),K('01:54'),K('19.3')],'hit')+label('Still one row: updated, not added to.',260,115,'caption',1180,'acc-cap')+label('<b>19.3 minutes</b>inside the 30-minute promise',260,190,'answer',560,'sla')+note('Across orders, average the minutes; never add them.',860,205,'teal',580),
'<strong>Accumulating snapshot:</strong> one row per process, updated as each milestone arrives.',
'Picked up 01:46:49, delivered 01:54:55: the row is complete, and operations can ask how long each step took and which stores miss the 30 minutes. Durations are not additive: average them, or count the orders over 30 minutes. The streaming session (D3_3) builds the live version. Lab 2.2 builds only the transaction fact.',['kimball']);

add('09 / Reconcile','Prove the model before anyone trusts it','Compare fct_order_items with raw.order_items: the same rows, the same money.',
box('Source','<div class="data-row"><span>rows</span><span>172,545</span></div><div class="data-row"><span>net SAR</span><span>7,343,424.53</span></div>',0,20,400,'bronze','recon-src','raw.order_items')+label('<b>✓</b> rows: 172,545 = 172,545',470,30,'check',520,'ck1')+label('<b>✓</b> money: 7,343,424.53 = 7,343,424.53',470,95,'check',520,'ck2')+label('<b>✓</b> key: order_id + line_no is unique',470,160,'check',520,'ck3')+label('<b class="warn">!</b> 3,930 lines on Unknown: explained, watched',470,225,'check',520,'ck4')+box('Model','<div class="data-row"><span>rows</span><span>172,545</span></div><div class="data-row"><span>net SAR</span><span>7,343,424.53</span></div>',1040,20,400,'gold','recon-fct','star.fct_order_items')+note('Check both. Equal counts can hide one lost row and one duplicate; equal money can hide errors that cancel out.',0,340,'',1200),
'<strong>Reconcile:</strong> compare rows and money with the source, under the same filters, every time the model is built.',
'Lab 2.2 section 3 runs exactly these checks; the fact keeps every source line, including the 142 with zero or negative quantity, so it must match the raw table to the halala. A fan-out would break the row count; an inner join that drops erased customers would break both. A curated mart that drops rows (dbt’s quarantines them in Lab 2.4) is reconciled against the same filtered source, not all raw rows.',['lab']);

add('10 / The recipe','Four decisions, then the build','The same steps for any business process. Next session, dbt turns them into models and tests.',
svg(path('M332 110 L366 110','em')+path('M702 110 L736 110','em')+path('M1072 110 L1106 110','em'))+box('1 · Process','<p>Delivered sales</p>',0,20,330,'','r1','what the business measures')+box('2 · Grain','<p>One row per order line</p>',370,20,330,'','r2','test it')+box('3 · Measures','<p>quantity, net_amount</p>',740,20,330,'silver','r3','fees in fct_orders')+box('4 · Dimensions','<p>date, product, store, customer</p>',1110,20,330,'gold','r4','plus an Unknown member')+tool('dbt','next session: these designs become models, tests and docs',0,300,640,'t-dbt')+label('<b>Then reconcile</b><br>rows and money against the source',740,300,'reason',700,'r-recon'),
'<strong>The design is yours:</strong> a tool can build a star, but only you can say what one row means.',
'The four steps are Kimball’s design process: choose the business process, declare the grain, identify the facts, identify the dimensions. Grain comes before measures and dimensions because it decides which of them fit. In D2_3 the same model moves into dbt: stg_ models for silver, dim_ and fct_ models for gold, with the grain as a unique test.',['kimball','dbt']);

const WORDS=[['Grain','what one row stands for'],['Fact table','one row per event, with its measures'],['Measure','a number you add up'],['Dimension','descriptions to filter and group by'],['Star schema','a fact with one join to each dimension'],['Fan-out','a join that repeats a coarser number'],['Surrogate key','the warehouse’s own ID for a row'],['Unknown member','the dimension row for “no match”'],['Snapshot','a level or a process, restated per row'],['Reconcile','prove rows and money match the source']];
add('11 / The words','Ten words for modeling','Every one appeared on a board today. dbt and the labs use them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<5?0:740,(i%5)*100,'check',700,`w${i}`)).join(''),
'<strong>Grain first:</strong> most of the other words follow from it.',
'A reference board: point at each word and ask the room which board it appeared on, and what Khalid’s order looked like there. Learners can come back to it during the dbt session.');

const RECALL=[
 ['Finish the sentence for fct_order_items: one row per ___. How do you test it?','One order line. Count rows and distinct (order_id, line_no): 172,545 each.'],
 ['Khalid pays a 9 SAR delivery fee. Which table holds it?','fct_orders. The fee happens once per order, so it lives at order grain.'],
 ['Someone joins orders onto lines and adds each line’s fee. What does Khalid’s order give?','98.50, not 89.50: the fee is counted twice. Add the lines up first, then join.'],
 ['Order 5000037’s customer was erased. Which customer_key do its lines get?','−1, the Unknown member (LEFT JOIN + COALESCE). No sale is dropped.'],
 ['Coffee stock reads 40, 34, 30 on three days. Can you add them up?','Not across days: a level adds across stores on one day. On the last day the stock is 30.']];
add('12 / Recall','Your turn: answer before you look','Five questions on Khalid’s order. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer: the guess that turns out wrong is what makes the right answer stick. The next slide fills in all five answers in place.');

add('12 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>Grain first,</strong> then measures at their own grain, then dimensions with an Unknown member.',
'Walk through any answer the room missed by going back to its board: grain (10–13), measures and fan-out (14–20), keys (24–25), fact types (26–29). Question 3: 80.50 + 2 × 9 = 98.50 instead of 80.50 + 9 = 89.50. Question 5 uses the illustrative stock levels.');

add('Lab 2.2 · 25 min','A star schema for Qafila','State the grain, build the model, then prove its rows and money match the source.',
`<div class="references"><div><h3>Lab 2.2 · 25 minutes</h3><p>1 · Declare the grain and test it · 4′</p><p>2 · Four dimensions and an Unknown member · 7′</p><p>3 · Build the fact and reconcile it · 6′</p><p>4 · Answer questions from the star · 5′</p><p>5 · Reproduce and fix the fan-out · 3′</p><p class="note">✱ One big table vs. star: size and speed. Find Khalid’s order 5043883 in your fact: two rows, 80.50.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D2_2_dimensional_modeling.ipynb</a><a href="${sources.kimball}" target="_blank" rel="noopener">Further reading · Kimball’s modeling techniques</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next, after a 20-minute break: Session 3 · dbt.',
'Hand over to the lab. A quick first check once the fact is built: SELECT * FROM star.fct_order_items WHERE order_id = 5043883 returns Khalid’s two lines with keys 104/193, 11 and 7214. The notebook uses schema star with singular names (dim_product) and surrogate keys; the dbt project in Lab 2.3 uses plural names (dim_products) and the source IDs for its Type 1 dimensions. Neither implements SCD2 here: that was D1_4. Section 4 asks two questions the dimensions make easy: weekends vs. weekdays, and the National Day uplift by city.',['lab','kimball']);

Story.start({brand:'QAFILA / MODELING STORY',sources});
})();
