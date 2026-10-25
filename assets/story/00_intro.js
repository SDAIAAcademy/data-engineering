/* Course introduction: the Qafila business before the tools, one idea per board (engine: story.js).
   The business only: who does what, what each step records (down to one order's rows and keys), who needs what. Where the data goes is the next story (00_story).
   Order 5043883 and every number are real course data: data/raw/qafila_oltp.sqlite, api/payments.jsonl,
   events/delivery_events.jsonl, suppliers/prices_2026-W39.csv, cdc/ and reviews.csv. Times are Riyadh time. */
(() => {
'use strict';
const {art,label,note,path,svg,box,rec,tbl,recall,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={setup:colab('00_setup_check.ipynb'),windows:colab('00_sql_windows.ipynb'),story:'00_story.html',repo:'https://github.com/SDAIAAcademy/data-engineering'};

// The order lane: four stations, one per step. A station's figure keeps its id, so it stays put or glides between boards.
const CX=[180,540,900,1260],FW=200;
const STATIONS=[['customer','f-cust'],['payment','f-pay'],['operations','f-store'],['courier','f-courier']];
const station=(i,title,detail)=>art(STATIONS[i][0],CX[i]-FW/2,0,FW,title,detail,STATIONS[i][1]);
const arrows=n=>[...Array(n).keys()].map(i=>path(`M${CX[i]+95} 80 L${CX[i+1]-95} 80`,'em',`w-s${i}`)).join('');
const STEP=[['1 · Order','the customer'],['2 · Pay','payment provider'],['3 · Pick + pack','the dark store'],['4 · Deliver','a courier']];
const lane=()=>STEP.map(([t,d],i)=>station(i,t,d)).join('');
// Order 5043883 as the steps happen (slides 4–6): one card per step under its station.
const CARDS=[['o-card','order 5043883','2 lines + fee','89.50','order'],['p-card','captured','by card, 01:35','89.50','copy'],['s-card','handed over','to the courier, 01:46','','chg'],['d-card','delivered','at the door, 01:54','','chg']];
const cards=n=>CARDS.slice(0,n).map(([id,h,b,a,c],i)=>rec(id,CX[i]-150,300,300,h,b,a,c)).join('');
const khalid=()=>art('customer',CX[0]-FW/2,0,FW,'Khalid','Al Malqa, Riyadh','f-cust');
// The five teams (slides 13–14): figure, team, its question.
const TEAMS=[['analyst','Finance','How much did we earn yesterday, after refunds?','f-fin'],['operations','Operations','Which stores miss the 30 minutes?','f-store'],['stakeholders','Purchasing','Which products lose margin?','f-pur'],['scientist','Data science','How many orders tomorrow evening?','f-ds'],['steward','Compliance','Who may see Khalid’s national ID?','f-cmp']];
const teams=()=>TEAMS.map(([k,t,q,id],i)=>art(k,50+i*290,0,180,t,q,id)).join('');

add('Day 1 · Opening','Know the business before the tools','',
svg(path('M245 120 C300 130 320 150 345 175','em')+path('M470 300 C520 270 545 230 560 190','em'))+art('customer',30,0,210,'A customer','','cover-cust')+art('courier',300,185,190,'A courier','','cover-courier')+art('stakeholders',500,0,220,'The teams','','cover-teams'),
'Before any tool: what Qafila does, what each step records, and who needs what.',
'About 30 minutes, then the setup check. Welcome everyone, introduce yourself and the TA. Quick poll: who writes SQL weekly? Who has scheduled a pipeline (cron, Airflow, anything)? Who has used Spark, dbt or Kafka? This deck is only about the business: the order process, what each step records, who needs which answer and how fast. Each board makes one point. Every tool in the week answers one of these needs; where the data should live is the next story. One real order, 5043883, runs through the deck. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',[],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · COURSE INTRODUCTION</p><h1>Know the<br>business before<br>the <span class="teal">tools</span></h1><p class="lead">One company. One order.<br>Who needs what, and why.</p>'});

add('01 / The business','Qafila delivers groceries in 30 minutes','A fictional company in Riyadh, Jeddah and Dammam. Everything in this course happens there.',
svg(path('M300 110 C520 40 880 40 1110 110','em','w-promise'))+art('customer',90,0,210,'You order','','f-cust')+art('courier',1120,0,210,'It arrives','','f-courier')+label('<b>30 min</b>the promise',600,20,'answer center',240,'promise')+
label('<b>12</b>dark stores',150,330,'answer center',300,'k-stores')+label('<b>356</b>products',570,330,'answer center',300,'k-products')+label('<b>750</b>orders a day',990,330,'answer center',300,'k-orders'),
'<strong>Quick commerce:</strong> groceries delivered in minutes, from small stores close to the customer.',
'Qafila is fictional and all its data is synthetic: no real company, no real person. It sells groceries through an app and promises delivery within 30 minutes. 12 dark stores (Riyadh 6, Jeddah 4, Dammam 2), 356 products, 45,766 orders over August and September 2026, about 750 a day. Ask the room: what has to go right for an order to arrive in 30 minutes?');

add('01 / One order','Step 1: Khalid orders','At 01:35 on 28 September, Khalid in Al Malqa, Riyadh, opens the app.',
khalid()+label('HIS BASKET · order 5043883',420,0,'hand muted',560,'b-title')+
rec('o-l1',420,50,520,'1 × cucumbers','Sahra','4.74','line')+rec('o-l2',420,135,520,'2 × Arabic coffee','Al Waha, 37.88 each','75.76','line')+rec('o-fee',420,220,520,'delivery fee','','9.00','order')+label('to pay <b>89.50</b>',420,305,'sum',520,'o-sum')+
note('An <b>order</b> is one basket.',1010,55,'teal',430)+note('Each product in it is an <b>order line</b>.',1010,140,'teal',430),
'<strong>Order, order line:</strong> one purchase, and one product inside it.',
'Khalid is a synthetic customer. He signed up two days earlier; this is his first order. Two order lines: 1 × Sahra Cucumbers at 4.74 and 2 × Al Waha Arabic Coffee at 37.88, so items 80.50, plus a 9.00 delivery fee: 89.50. Store RUH-05, Qafila Al Naseem. The order line matters on Day 2, when we decide what one row of a table stands for.');

add('01 / One order','Step 2: the money is taken','Not by Qafila. A payment provider charges Khalid’s card.',
svg(arrows(1))+khalid()+station(1,'Payment','another company')+cards(2)+
note('A payment has a <b>status</b>: captured, failed or refunded.',760,40,'teal',660)+note('Finance checks that every payment matches its order.',760,175,'',660),
'<strong>Payment status:</strong> captured means the money was taken. It can still change: a refund comes later.',
'Real payment pay_9040078: 89.50 SAR by card, captured 20 seconds after the order. Qafila does not hold card details; a payment provider does, and tells Qafila the result. Over two months 1,582 payment attempts failed; some captured payments were refunded later. Checking that the amount paid matches the order is reconciliation.');

add('01 / One order','Step 3: the dark store packs the bag','The nearest store, RUH-05 Al Naseem, picks both products from its shelves.',
svg(arrows(2))+khalid()+station(1,'Payment','another company')+station(2,'Dark store','RUH-05 Al Naseem')+cards(3)+
note('A <b>dark store</b>: a small warehouse with no shoppers, only pickers.',1110,30,'teal',330),
'<strong>Dark store:</strong> a store built for delivery only. Qafila has 12, so every customer is close to one.',
'The order goes to the nearest dark store. A picker collects the cucumbers and the coffee and packs the bag; a courier is assigned at 01:36 and collects it at 01:46. The dark store holds stock, so it is also where suppliers deliver (two boards from now). Riyadh 6, Jeddah 4, Dammam 2.');

add('01 / One order','Step 4: a courier rides to the door','Delivered at 01:54, 19 minutes after Khalid pressed “order”.',
svg(arrows(3))+khalid()+station(1,'Payment','another company')+station(2,'Dark store','RUH-05 Al Naseem')+station(3,'Courier','CR-RIY-076')+cards(4)+
label('<b>19 minutes</b>from order to door. The promise is 30.',380,405,'answer center',680,'sla'),
'<strong>SLA</strong> (service level agreement): the promise a team is measured against. Qafila’s is 30 minutes.',
'The courier app reports each step: assigned 01:36, picked up 01:46, out for delivery 01:48, delivered 01:54:55. From the order at 01:35:37 that is 19.3 minutes, inside the 30-minute promise. Operations lives on this number: which stores and which hours miss it?');

add('01 / The process','Four steps, four players, every order','The same path about 750 times a day. This is Qafila’s main business process.',
svg(arrows(3)+path('M60 340 L1400 340','axis','w-time'))+lane()+[['01:35','ordered'],['01:35','paid'],['01:46','handed over'],['01:54','delivered']].map(([t,w],i)=>label(`<b>${t}</b>${w}`,CX[i]-130,365,'answer center',260,`t${i}`)).join(''),
'<strong>Business process:</strong> the repeatable steps a company follows to serve a customer.',
'One picture of the whole flow. Each step has a different player: the customer in the app, a payment provider, the dark store team, a courier. Ask which step fails most often, and who would notice first. Keep this picture: on the next boards we look behind it (suppliers, customers), then at what each step writes down.');

add('01 / Behind the shelf','Suppliers fill the shelves, at a cost','Every week each supplier sends Qafila its new prices.',
svg(path('M330 80 L620 80','em','w-sup'))+art('files',130,0,FW,'Supplier','a price list every week','f-sup')+label('restocks',410,35,'hand teal',150,'restocks')+art('operations',620,0,FW,'Dark store','RUH-05 Al Naseem','f-store')+
label('<b>37.88</b>Qafila sells the coffee for',100,330,'answer center',320,'n-sell')+label('<b>−</b>',430,330,'answer center',80,'n-minus')+label('<b>29.87</b>the supplier charges',520,330,'answer center',320,'n-cost')+label('<b>=</b>',850,330,'answer center',80,'n-eq')+label('<b class="teal">8.01</b>left for Qafila',940,330,'answer center',320,'n-margin'),
'<strong>Margin:</strong> what we sell for, minus what it cost us. The price and the cost come from different places.',
'Real numbers: Al Waha Arabic Coffee sells for 37.88 in the app; SUP-DATES-QSM charges 29.87 in its W39 price list (valid from 21 September). The margin on one pack is 8.01. The purchasing team watches margins: when a supplier raises a price, which products start losing money?');

add('01 / Customers','A customer is more than one order','Khalid signed up on 26 September and ordered two days later. Customers keep changing after that.',
svg([0,1,2,3].map(k=>path(`M${452+k*245} 125 L${473+k*245} 125`,'em')).join(''))+art('customer',0,40,FW,'A customer','','f-cust')+
[['signs up','Khalid: 26 Sep'],['orders','Khalid: 28 Sep'],['reviews','stars and a comment'],['edits profile','new mobile, district'],['erasure request','a right under the PDPL']].map(([h,b],k)=>rec(`lc${k}`,230+k*245,90,220,h,b,'','chg')).join('')+
label('In September: <b>1,541</b> sign-ups · <b>4,458</b> profile edits · <b>159</b> erasure requests',230,280,'check',1210,'lc-counts')+
note('<b>Personal data</b>: anything that identifies a person, like a name, a mobile or a national ID. The PDPL protects it.',230,350,'',1210),
'<strong>Customers change</strong> after they sign up: profiles move, reviews arrive, some ask to be forgotten.',
'Khalid’s own story is short: signed up 26 September 12:51, first order 28 September. Other customers show the rest: in September 1,541 signed up, 4,458 profile edits happened and 159 customers asked to be erased (counted from the change feed in Lab 1.4). Reviews come in Arabic and English. PDPL is Saudi Arabia’s Personal Data Protection Law: it limits who may see personal data and gives people the right to have it destroyed.');

add('02 / The records','Every step leaves a record','Each player writes down what it did, in its own system.',
svg(arrows(3)+CX.map(x=>path(`M${x} 272 L${x} 296`,'em')).join(''))+lane()+
rec('r-order',20,300,320,'order','in the app database','','src')+rec('r-pay',380,300,320,'payment','from the payment provider','','src')+rec('r-evt',740,300,680,'delivery events','sent by the courier app, one per step','','src')+
rec('r-cust',20,410,320,'customer changes','sign-ups, edits, erasures','','src')+rec('r-rev',380,410,320,'reviews','written in the app','','src')+rec('r-price',740,410,680,'price lists','a CSV file from each supplier, every week','','src'),
'<strong>Source system:</strong> software that runs one part of the business and records what happens as it goes.',
'The order lane again; now look under it. The app database records the order, the payment provider records the payment, the courier app sends an event for each delivery step. Beside the order: customer changes, reviews and the suppliers’ weekly price files. Six records in five systems, each owned by a different team, each with its own format and clock (the app database writes UTC, the courier app Riyadh time). In the data: 45,766 orders, 41,785 payment records, 22,518 delivery events, 6,346 customer changes, 3,000 reviews, 4 weekly price files. The problems hidden in them are Lab 1.1’s exercise: do not open data/raw/DATA_ISSUES.txt before it.');

add('02 / The records','Some records keep now, others keep what happened','The same order, as the app database and the courier app record it.',
art('db',30,0,150,'App database','','r-db')+rec('o-row',230,45,440,'order 5043883','status: delivered','','src')+note('Only the latest status. Each change overwrote the one before.',230,150,'',440)+
art('courier',760,0,150,'Courier app','','f-courier')+rec('e1',960,0,480,'01:36 assigned','courier CR-RIY-076','','chg')+rec('e2',960,85,480,'01:46 picked up','leaves RUH-05','','chg')+rec('e3',960,170,480,'01:48 out for delivery','on the road','','chg')+rec('e4',960,255,480,'01:54 delivered','at the door','','chg')+note('Every step, kept in order.',960,350,'',480)+
label('<b>State</b> · how things are now',230,440,'check',440,'n-state')+label('<b>Events</b> · what happened, and when',960,440,'check',480,'n-evt'),
'<strong>State or events:</strong> most sources keep only the state. History survives only where someone records events.',
'The app database keeps one row per order, and every new status overwrote the old one: it now says delivered, nothing more. The courier app is the opposite: each step is a new event, kept in time order. Both are normal. Day 1 copies state and turns its changes into events (CDC, D1_4); Day 3 processes events as they arrive (D3_3).');

add('02 / A learner decision','Operations asks: when was the bag picked up?','Order 5043883. Where do you look?',
art('operations',1100,40,270,'Operations','“When did the courier collect it?”','ops')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>The order’s row in the app database</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>The delivery events from the courier app</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>The payment record</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Choose an answer, then say why the other two cannot answer it.</p>',
'The current state cannot tell you about the past. Only a record of what happened can.',
'Allow 30 seconds. B is right: the picked_up event at 01:46. A is the common wrong answer: people expect the app database to hold everything, but its row says only delivered. C knows only money. The lesson for the week: if a question is about history, we must keep the history, because most sources do not.',[],{quiz:{answer:'B',right:'Yes: the picked_up event, 01:46. The order row now says only “delivered”.',wrong:'That record has no pick-up time: the order row was overwritten at each step, and the payment knows only money. Try again.'}});

// Order 5043883's rows in the source tables (slides 13–14): one map. orders and order_items hold the centre; the tables
// that point to the order fill the left (13), the tables the order points to fill the right (14). Each key sits on the
// edge that faces its arrow. K marks a key value; on says which keys a board marks.
const K=(v,on=true)=>on?`<em class="k">${v}</em>`:v;
const H=(...c)=>({c,cls:'h'});
const where=(name,system)=>`${name}<span class="src">${system}</span>`;
const orders=(on={})=>tbl('t-orders',505,40,465,where('orders','app database · 45,766 rows'),'95px 95px 110px 90px',[H('order_id','status','customer_id','store_id'),[K('5043883',on.o),'delivered',K('107371',on.c),K('RUH-05',on.s)]],'mono');
const items=(on={})=>tbl('t-items',505,250,465,where('order_items','app database · 172,545 rows'),'95px 85px 100px 105px',[H('order_id','quantity','unit_price','product_id'),[K('5043883',on.o),'1','4.74',K('1104',on.p)],[K('5043883',on.o),'2','37.88',K('1193',on.p)]],'mono');
const pointsIn=(fade=false)=>{const w=fade?'thin':'em',c=fade?'mono faint':'mono';return {
 wires:path('M444 90 C474 90 472 124 499 124',w,'w-pay')+path('M444 338 C480 338 472 140 499 140',w,'w-evt')+path('M570 248 L570 160',w,'w-items'),
 tables:tbl('t-pay',0,0,440,where('payments','payment provider · 41,785 rows'),'125px 1fr 90px',[H('payment_id','amount','order_id'),['pay_9040078','89.50',K('5043883',!fade)]],c)+
  tbl('t-events',0,200,440,where('delivery_events','courier app · 22,518 rows'),'165px 1fr 90px',[H('event_type','event_time','order_id'),...[['assigned','01:36:34'],['picked_up','01:46:49'],['out_for_delivery','01:48:20'],['delivered','01:54:55']].map(([e,t])=>[e,t,K('5043883',!fade)])],c)};};

add('02 / The records','One order, four tables, one key','Inside each system the records are rows in tables. Every row of this order carries its id.',
svg(pointsIn().wires)+orders({o:1})+items({o:1})+pointsIn().tables+
note('Eight rows, one order_id:<br>one order, two lines,<br>one payment, four events.',1035,110,'teal',405),
'<strong>Key:</strong> a column whose value names one row. The same order_id in four tables ties the order’s records together.',
'The record cards of the last boards, opened up. The order and its lines hold the middle; the tables on the left point to it. Inside each system a record is a row in a table: one row per order, per order line, per payment, per delivery event. Point at the counts: 45,766 orders but 172,545 lines, because an order has several lines. The payments and delivery events arrive as JSON lines files; they are shown here as rows, with a few columns each. Event times are Riyadh time on 28 September (the app writes UTC). order_id is the key of orders: one row each. In the other tables it repeats, once per line or event, and every arrow follows it back to the order. Ask: how many rows does order 5043883 have across the four tables? (Eight.) Whether every order has its payment and events is one of Lab 1.1’s checks.');

add('02 / The records','Other keys name who and what','The order row does not repeat Khalid’s name or the store’s address. It stores their ids.',
svg(pointsIn(true).wires+path('M976 124 C1006 124 1000 90 1029 90','em','w-cust')+path('M976 138 C1006 138 1000 235 1029 235','em','w-store')+path('M976 340 C1004 340 1000 380 1029 380','em','w-p1')+path('M976 372 C1004 372 1000 412 1029 412','em','w-p2'))+
orders({c:1,s:1})+items({p:1})+pointsIn(true).tables+
tbl('t-customers',1035,0,405,where('customers','app database · 7,442 rows'),'110px 1fr 1fr',[H('customer_id','full_name','district'),[K('107371'),'خالد التميمي','Al Malqa']])+
tbl('t-stores',1035,145,405,where('stores','app database · 12 rows'),'90px 1fr',[H('store_id','name_en'),[K('RUH-05'),'Qafila Al Naseem']])+
tbl('t-products',1035,290,405,where('products','app database · 356 rows'),'105px 1fr',[H('product_id','name_en'),[K('1104'),'Sahra Cucumbers'],[K('1193'),'Al Waha Arabic Coffee']])+
note('Follow an id to the row it names:<br>that is a join.',505,425,'teal center',465),
'<strong>Join:</strong> follow a key to the row it names. customer_id finds Khalid, store_id the dark store, product_id each product.',
'Same map: the order stays in the middle, the left side fades, and the right side fills with what the order points to. The details of a customer, a store or a product live once, in their own table; every order names them by id. Following an id to its row is a join, the operation behind almost every answer on the next boards. One detail worth showing: the line keeps the price Khalid paid (37.88), while the product row holds today’s price (37.97). The line records what happened; the product row is state. Lab 1.1 checks these links, for example orders whose customer_id has no customer row.');

add('03 / Who needs what','Every team asks its own question','The people who run Qafila decide with data. Each question is about orders like Khalid’s.',
teams(),
'<strong>Data consumers:</strong> the teams who ask the questions. Our work is judged by their answers.',
'Five teams, one question each. Finance closes the books: revenue after refunds. Operations runs the dark stores and watches the 30-minute promise. Purchasing buys from suppliers and watches margins. Data science forecasts demand so stores have stock and couriers on shift. Compliance protects personal data under the PDPL. Ask the room for a sixth team (marketing: do new customers come back?).');

const NEEDS=[['order','payment'],['order','delivery events'],['order lines','price lists'],['order history','delivery events'],['customer data','every copy']];
add('03 / Who needs what','Each answer needs records from several steps','Under each team: the records its question needs.',
teams()+NEEDS.map((rs,i)=>rs.map((r,k)=>rec(`need${i}-${k}`,30+i*290,345+k*68,220,r,'','','src slim')).join('')).join(''),
'<strong>Almost every answer joins records</strong> that different systems wrote. Bringing them together is the data team’s job.',
'Read one team at a time. Finance needs orders and payments; operations, orders and delivery events; purchasing, order lines and price lists (the margin board); data science, months of orders and delivery times; compliance, wherever personal data sits, including every copy the data team makes. No question is answered by one system alone. How the data team brings the records together is the next story.');

add('03 / How fast','How old can the answer be?','Some teams need minutes. Others need yesterday, exact.',
svg(path('M0 330 L1425 330','axis','axis')+[110,400,980,1270].map(x=>path(`M${x} 255 L${x} 320`,'thin')).join(''))+
art('payment',45,40,130,'Fraud check','','f-risk')+art('operations',335,40,130,'Operations','','f-store')+art('analyst',915,40,130,'Finance','','f-fin')+art('stakeholders',1205,40,130,'Purchasing','','f-pur')+
['seconds','minutes','hours','next morning','next week'].map((t,i)=>label(t,10+i*290,345,'tick',200,`tk${i}`)).join('')+
note('Data science is the odd one out: not fresh, but long. It needs months of history.',0,420,'',1440),
'<strong>Freshness:</strong> how old an answer may be before it is useless. It decides between a nightly batch and a live stream.',
'Walk the axis left to right. A fraud check must answer before the payment is captured: seconds. Operations must see orders about to miss 30 minutes while there is still time to act: minutes. Finance closes yesterday’s numbers the next morning, exact. Purchasing reacts when the weekly price lists arrive. Data science needs long history rather than fresh data. A batch collects records and processes them on a schedule; a stream processes each event as it arrives. Most of the week is batch; Day 3 adds streaming.');

const WORDS=[['Business process','the repeatable steps that serve a customer'],['Order line','one product inside an order'],['SLA','the promise a team is measured against'],['Margin','what we sell for, minus what it cost us'],['Personal data','anything that identifies a person'],['Source system','software that runs a step and records it'],['State','a record of how things are now'],['Event','a record of something that happened'],['Key','a column whose value names one row'],['Join','following a key to the row it names'],['Data consumer','a team that asks questions of the data'],['Freshness','how old an answer may be']];
add('04 / The words','Twelve words for the week','Every deck and notebook uses them. You met each one today.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>The business first:</strong> every tool this week helps one of these teams get its answer.',
'A reference board: point at each word and ask the room which board it appeared on. Learners can come back to it during the week.');

add('05 / The week','Each day answers some of the questions','Five days, one platform for Qafila. Every lab adds a piece.',
[['Day 1 · Land','copy every record in',['stakeholders'],'for everyone'],['Day 2 · Model','exact, tested numbers',['analyst'],'Finance, Purchasing'],['Day 3 · Scale','history and live events',['operations'],'Operations, Data science'],['Day 4 · Operate','run, watched, protected',['steward'],'Compliance'],['Day 5 · Capstone','your team’s platform, demoed',['engineer'],'the whole week']].map(([t,p,ks,who],i)=>box(t,`<p>${p}</p>${ks.map(k=>`<div class="art day-art ${k}" aria-hidden="true"></div>`).join('')}`,i*293,0,268,i===4?'gold day':'day',`day${i+1}`,who)).join('')+
label('<b>You should know</b> · Python, SQL (joins, GROUP BY), the command line, Git. No Spark, dbt, Kafka, cloud or Docker needed.',0,430,'check',1440,'k-know'),
'<strong>Every lab adds a piece</strong> for one of the teams. Day 5 puts the pieces together.',
'Map each day to the teams it serves. Day 1 copies every source in, changes included: everyone depends on that copy. Day 2 cleans, models and tests: the exact numbers finance and purchasing need. Day 3 scales up into a lakehouse and adds streaming for operations, and the history data science needs. Day 4 runs it all on a schedule, watches it and protects personal data for compliance, and serves it to an AI assistant. Day 5: teams present a working platform with a live demo, design decisions and technical Q&A. You leave with 16 working notebooks, a dbt project, a lakehouse and a capstone repo.');

add('05 / The week','Learn it, then build it','Two kinds of session. Both end in a lab on the Qafila data.',
box('Concept sessions','<p>a story board like this one,<br>a quiz, then the lab</p>',0,0,690,'silver','k-concept','Parquet · CDC + SCD · modeling · streaming · streams into tables · contracts · governance · data for AI')+box('Tool sessions','<p>no slides: we run the tool’s<br>walkthrough notebook together</p>',750,0,690,'','k-tool','DuckDB · dlt · Polars · dbt · Pandera · Iceberg · Spark · Dagster')+
label('<b>🤔 Predict, run, read</b><br>write your guess before you run the cell',0,300,'reason',440,'h-predict')+label('<b>✏️ Fill the blanks</b><br>replace every ___, run the check: ✅',500,300,'reason',440,'h-ex')+label('<b>Solutions</b><br>every lab has one in labs/solutions/',1000,300,'reason',440,'h-sol'),
'<strong>Learn it, then build it:</strong> a short board or a notebook walkthrough, then about 25 minutes of lab.',
'Concept sessions are drawn on a board like this one, because ideas such as grain, CDC and watermarks are pictures. Tool sessions have no slides: we run the tool’s notebook together (the problem it solves, how it thinks, then its features one at a time). In every notebook: predict what a cell returns, run it, read why. Exercises are commented templates: replace every ___ and run the check cell until it shows ✅. Every lab and walkthrough has a solution in labs/solutions/. The course page links each session to its slides or notebook, its lab and the solution.');

add('05 / The week','Run the setup before you need it','Google Colab or your own laptop. One notebook checks everything.',
label('<b>1 · Open the repo</b><br>in Google Colab, or locally with uv',0,0,'reason',640,'s1')+label('<b>2 · Run 00_setup_check.ipynb</b><br>installs the packages, generates the data, tests every tool',0,110,'reason',640,'s2')+label('<b>3 · In every notebook</b><br>Setup first, Checkpoint questions last; ✱ Stretch cells are optional',0,220,'reason',640,'s3')+
label('<div style="white-space:pre">git clone https://github.com/SDAIAAcademy/data-engineering\ncd data-engineering/labs\nuv sync                        # creates .venv\nuv run python prepare_data.py  # data/raw/\nuv run jupyter lab</div>',720,0,'code tbl',720,'setup-code')+
label('The data is synthetic and the same for everyone: no real person’s data, national IDs and IBANs are random.',720,215,'caption',720,'s-data')+
note('No Java on your laptop? Run the Spark notebooks (tools/spark and Lab 3.2) on Colab: the notebook installs Java.',0,380,'red',1440),
'<strong>Run it now:</strong> by Session 1.1 every tool should already work on your machine.',
'Have everyone open the setup check now, on Colab or locally, while you take questions. prepare_data.py generates everything in data/raw/ from a fixed seed: identical data for everyone, offline, and no real personal data (national IDs and IBANs have valid check digits but are random). Every notebook runs on a laptop CPU except Spark, which needs Java 17+: run tools/spark and Lab 3.2 on Colab.',['setup','repo']);

const RECALL=[
 ['Name the four steps of a Qafila order, and who does each.','Order (the customer, in the app), pay (a payment provider), pick + pack (the dark store), deliver (a courier).'],
 ['What is Qafila’s SLA, and did order 5043883 meet it?','30 minutes from order to door. Yes: delivered in 19 minutes.'],
 ['The coffee’s margin: where do the two numbers come from?','The price (37.88) from the app, the cost (29.87) from the supplier’s weekly price list: 8.01.'],
 ['When was the bag picked up, and which record says so?','01:46, a delivery event from the courier app. The order’s row says only “delivered”.'],
 ['Which team needs minutes, and which needs months of history?','Operations (and the fraud check) need minutes or seconds. Data science needs months of history.']];
add('06 / Recall','Your turn: answer before you look','Five questions on the business. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> the business comes first, the tools follow from it.',
'About 3 minutes. Read each question, give the room 20 seconds, take an answer, then move on. The next slide fills in all five answers in place.');

add('06 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>Steps, records, teams, freshness:</strong> the whole introduction in one line.',
'Go back to the board for any answer the room missed: the steps (3–7), the margin (8), the records, the quiz and the keys (10–14), freshness (17).');

add('Next · the story','The teams need the records together','Where should Qafila keep them? Warehouse, lake or lakehouse: the next story.',
`<div class="references"><div><h3>Up next</h3><p>The story: where Qafila keeps its records, and how order 5043883 travels from the app to a dashboard.</p><p>Then Session 1.1: DuckDB, the engine every lab runs on.</p><p class="note">Keep four questions in mind: what happened, where it was recorded, who needs it, how fresh.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.story}">The story · Warehouse, lake, lakehouse</a><a href="${sources.setup}" target="_blank" rel="noopener">Setup · 00_setup_check.ipynb</a><a href="${sources.windows}" target="_blank" rel="noopener">Lab 0 · SQL window functions, if they are new</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'The business is the map. Every tool this week answers one of its questions.',
'Hand over to the story deck. It opens with finance and operations getting different revenue numbers from the same records, and ends with order 5043883’s journey through the platform.',['story','setup','windows']);

Story.start({brand:'QAFILA / COURSE INTRODUCTION',sources});
})();
