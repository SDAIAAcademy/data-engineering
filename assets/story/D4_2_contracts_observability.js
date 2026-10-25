/* D4_2 Observability and data contracts, told on one evolving board (engine: story.js), one idea per board.
   The running example is Khalid's payment pay_9040078 (00_intro.js): 89.50 SAR by card for order 5043883, captured at
   01:35:57 on 28 September. It arrives in a feed Qafila didn't build, from a payment provider, so Day 2's tests don't cover it.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every row, count and date is real: data/raw/api/payments.jsonl, the app's orders, the dbt project's manifest, and the
   numbers Lab 4.2 prints (its incident on 29 September is the lab's simulated partial load). Times are Riyadh time;
   daily volumes are UTC days, as in the lab. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={odcs:'https://bitol-io.github.io/open-data-contract-standard/latest/',quality:'https://bitol-io.github.io/open-data-contract-standard/latest/data-quality/',sla:'https://bitol-io.github.io/open-data-contract-standard/latest/service-level-agreement/',cli:'https://cli.datacontract.com/',freshness:'https://docs.getdbt.com/reference/resource-properties/freshness',openlineage:'https://openlineage.io/docs',intro:'00_intro.html',d22:'D2_2_modeling.html',lab:colab('D4_2_contracts_observability.ipynb')};
const H=(...c)=>({c,cls:'h'}),SANS=v=>`<span style="font-family:var(--sans)">${v}</span>`,BAD=v=>`<em class="k bad">${v}</em>`,K=v=>`<em class="k">${v}</em>`;
// Code on a board: leading spaces become non-breaking, so YAML keeps its indentation.
const code=(lines,x,y,w,id)=>label(lines.map(l=>l.replace(/^ +/,s=>'&nbsp;'.repeat(s.length))).join('<br>'),x,y,'code',w,id);

// Khalid's payment and the provider behind it.
const provider=(x=0,y=20,w=190)=>art('payment',x,y,w,'Payment provider','another company','f-pay');
const finance=(x=1230,y=20,w=190,detail='“Can we trust this feed?”')=>art('analyst',x,y,w,'Finance',detail,'f-fin');
const khalidPay=(x,y,w=520,cls='copy',head='pay_9040078 · order 5043883',body='card · captured · 28 Sep 01:35:57')=>rec('pay-k',x,y,w,head,body,'89.50',cls);
// Three of the 16 payments with amount <= 0 (pay_9040081 is three orders after Khalid's).
const BADPAY=[['pay-b1','pay_9040081 · order 5043886','card · refunded · 28 Sep 18:58','−15.41'],['pay-b2','pay_9021647 · order 5023632','wallet · captured · 3 Sep 19:03','−124.56'],['pay-b3','pay_9004094 · order 5004473','apple_pay · captured · 7 Aug 22:12','−0.21']];
const badRows=(x,y,w=560,n=3)=>BADPAY.slice(0,n).map(([id,h,b,a],i)=>rec(id,x,y+i*90,w,h,b,a,'wrong')).join('');

// Orders per UTC day, 1 Aug – 30 Sep, as Lab 4.2 loads them: on 29 Sep a load job silently kept 308 of the day's 765 orders.
const DAILY=[623,622,623,627,629,795,782,643,644,634,641,653,817,812,648,657,662,666,659,839,830,674,675,672,681,687,851,861,685,689,693,701,700,881,874,704,713,709,718,708,907,891,735,719,731,723,738,915,933,738,745,739,759,1423,938,952,752,762,761,308,758];
// Day 0 is Saturday 1 August; Thursdays and Fridays are Qafila's weekend peaks.
const THUFRI=i=>i%7===5||i%7===6,NATDAY=53,INCIDENT=59;
const NAIVE=[12,13,19,20,26,27,33,34,40,41,47,48,53,54,55,59],SMART=[NATDAY,INCIDENT];
const CX0=10,STEP=16.3,CW=12,BASE=380,SCALE=0.24;
const cx=i=>+(CX0+STEP*i).toFixed(1);
const chart=(marks=[],tone=()=>'')=>DAILY.map((v,i)=>{const h=Math.round(v*SCALE);return label(`<span style="height:${h}px"></span>`,cx(i),BASE-h,`vcol ${tone(i)}`,CW,`d${i}`);}).join('')+
 marks.map(i=>label('',cx(i)+2,BASE-Math.round(DAILY[i]*SCALE)-22,'vmark',13,`m${i}`)).join('')+
 svg(path(`M0 ${BASE+2} L1010 ${BASE+2}`,'axis','c-axis'))+
 [[0,'1 Aug'],[14,'15 Aug'],[31,'1 Sep'],[45,'15 Sep'],[59,'29 Sep']].map(([i,t])=>label(t,cx(i)-40,BASE+12,'tick',100,`ct${i}`)).join('');
const peaks=i=>i===INCIDENT?'bad':THUFRI(i)?'peak':'';

add('Day 4 · Session 2','Contracts and monitors','',
svg(path('M180 130 C200 160 210 190 230 215','em')+path('M560 240 C580 210 590 180 600 160','em'))+provider(0,0,180)+khalidPay(170,230,400,'copy','pay_9040078','order 5043883 · card · captured')+finance(540,0,180,''),
'Trust data you didn’t build: write the promise down, check it, and watch every table over time.',
'About 25 minutes for the board, then Lab 4.2 (25 minutes). Until now every check was on code we wrote: dbt tests and Pandera schemas (Day 2). Today the data comes from someone else, Qafila’s payment provider, and changes without asking. Two tools: a data contract (the promise, written down and executable) and monitors (what happens to every table over time). Khalid’s payment from the introduction runs through the board: 89.50 SAR by card, pay_9040078. Every number comes from the course data and Lab 4.2. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 4</p><h1>Contracts<br><span class="teal">and monitors</span></h1><p class="lead">Khalid’s 89.50, a promise in YAML,<br>and alerts nobody mutes.</p>'});

add('00 / You are here','Checks on data you didn’t build','The contract sits at the payments feed; observability runs along the whole platform.',
platform(n=>n==='Contracts'||n==='Observe'?'fresh':'met',{Contracts:'The feed’s promise',Observe:'Every table, every day'}),
'<strong>Tests check our code.</strong> Contracts and monitors check what others send us, and what changes over time.',
'The course map with two pieces lit. Pandera (D2_4) checked files at the door and dbt tests checked our models. The contract box sits in the Test column, at the payments feed; the Observe band runs under every stage, because freshness and volume matter for every table, not just one. Orchestration (D4_1) runs the checks; governance (D4_3) is next.');

const QT='Finance asks: can we trust the payments feed?',QS='It comes from the payment provider, another company. Qafila didn’t write a line of it.';
const question=()=>svg(path('M200 110 L290 110','em','w-q1')+path('M830 110 L1210 110','em','w-q2'))+provider()+
 rec('feed',300,60,520,'api/payments.jsonl','one row per payment attempt · daily','41,785','src')+finance()+
 note('Khalid’s payment is one of these rows.',300,170,'teal',520);
add('01 / The question',QT,QS,question(),
'Day 2’s tests cover <strong>our models</strong>. Nothing yet covers <strong>the feed</strong> they start from.',
'A build: two boards. Finance reconciles every payment with its order (00_intro, step 2), so the feed is the start of their revenue numbers. The provider sends it through an API: Lab 1.3 loaded it with dlt, 41,785 attempts over August and September. Ask the room: what could the provider change tomorrow that would break Finance’s report, without anyone at Qafila touching code?',['intro']);

add('01 / The question',QT,QS,question()+
 label('<b style="color:#b96355">16 payments of zero or less</b><br>since 1 August · −0.21 to −124.56 SAR',300,250,'reason',560,'fail-amt')+label('<b style="color:#b96355">29 Sep: 308 orders loaded, not 765</b><br>a load stopped early, with no error',300,340,'reason',560,'fail-vol')+
 note('No test failed. No test was written for either.',300,440,'red',900),
'Two real problems, both silent: <strong>wrong values</strong> in a feed, and <strong>missing rows</strong> on one day.',
'Both are in the course data. The 16 non-positive payments trace back to order lines with negative quantities (the ones Lab 2.4 quarantined): the provider charged a negative amount and nobody objected. The 29 September load is Lab 4.2’s simulated incident: a job that loaded only 40% of the day and still reported success. Neither breaks a query, so neither raises an error. The first needs a promise someone can check (a contract); the second needs something that watches the table over time (a monitor).',['lab']);

const ROWT='What Finance assumes about every row',ROWS='Khalid’s payment, field by field. Each column on the right is a promise nobody wrote down.';
const FIELDS=[['payment_id','pay_9040078','unique: one row per attempt'],['order_id','5043883','joins to an order'],['amount','89.50','SAR, VAT included, more than 0'],['currency','SAR','always SAR'],['method','card','card, apple_pay or wallet'],['status','captured','captured, failed or refunded'],['card_last4','8203','four digits, as text'],['created_at','2026-09-27T22:35:57Z','ISO-8601, UTC']];
add('02 / The contract',ROWT,ROWS,
tbl('k-row',0,0,1100,'pay_9040078<span class="src">api/payments.jsonl</span>','230px 330px 1fr',[H('field','value','what Finance relies on'),...FIELDS.map(([f,v,p])=>[f,v,SANS(p)])],'mono')+provider(1200,40,200),
'<strong>Implicit expectations</strong> break silently. A <strong>data contract</strong> writes them down where both sides can check them.',
'Khalid’s real row. Finance’s reconciliation (D2_2’s revenue definition too) quietly depends on every promise in the right column: one row per attempt, positive amounts, three statuses, card digits as text. Today these live in people’s heads and in scattered SQL. A data contract is an agreement between the producer (the provider) and its consumers (Finance, the data platform), written as a file both can read and a machine can check.',['intro','d22']);

const YAML=['apiVersion: v3.1.0','kind: DataContract','id: urn:datacontract:qafila:payments-feed','version: 1.0.0','team:','  - {username: payments-provider-integration, role: owner}','support:','  - {channel: "#payments-data", tool: slack}','schema:','  - name: payments','    quality:','      - {metric: rowCount, mustBeGreaterThan: 1000}','    properties:','      - name: amount','        physicalType: DOUBLE','        required: true','slaProperties:','  - {property: latency, value: 1, unit: d}'];
const ANN=[[0,'which standard, which version'],[105,'who owns it, where to ask'],[212,'tables, columns, types, rules'],[427,'how fresh, how often']];
add('02 / The contract','The contract as a file: ODCS','The Open Data Contract Standard (Bitol, Linux Foundation AI & Data): vendor-neutral YAML. An excerpt of Lab 4.2’s.',
code(YAML,0,0,800,'yaml')+svg(ANN.map(([y],i)=>path(`M820 ${y+16} L900 ${y+16}`,'em',`w-a${i}`)).join(''))+ANN.map(([y,t],i)=>label(t,920,y,'hand teal',520,`ann${i}`)).join(''),
'<strong>ODCS:</strong> schema, quality rules, service levels and people, in one file. v3.1.0 is the current release.',
'Lab 4.2’s contract, shortened. Four parts: metadata (the standard and the contract’s own version), people (team with roles, support channel: the social half of contracts), schema (every property with its logical and physical type, required, unique, classification and quality rules), and service levels (latency: the data is at most 1 day old; frequency: daily). Quality rules use metrics such as rowCount, nullValues, invalidValues and duplicateValues, or a SQL query against {object}. ODCS v3.2 is in development; v3.1.0 is what the tools support today.',['odcs','quality','sla']);

add('02 / Where contracts go','Contracts sit where one team hands data to another','A promise between producer and consumer. Inside one team’s models, dbt tests are enough.',
svg(path('M195 130 L275 130','em','w-c1')+path('M515 130 L565 130','','w-c2')+path('M805 130 L865 130','','w-c3')+path('M1105 130 L1215 130','em','w-c4'))+provider(0,40,190)+
 box('raw.payments','<p>the feed, landed</p>',280,60,230,'bronze','b-raw')+box('stg · int','<p>our models</p>',570,60,230,'silver','b-stg')+box('fct_orders','<p>published mart</p>',870,60,230,'gold','b-fct')+finance(1230,40,190,'')+
 label('contract',190,92,'ptr sla',90,'c-in')+label('dbt tests',570,290,'ptr',230,'c-mid')+label('contract',1110,92,'ptr sla',100,'c-out')+
 note('The provider promises the feed to us.',0,380,'teal',600)+note('We promise the mart to Finance.',840,380,'teal',600),
'<strong>Contracts at the seams:</strong> a provider’s feed, a domain’s published tables, a mart other teams build on.',
'Not every model needs a contract. Inside the data team, dbt tests and model contracts (D2_4) already check what we build. A data contract earns its keep where ownership changes: the provider’s feed coming in, and the marts going out to Finance, Operations or the AI assistant (D4_4). The same YAML can describe both directions.',['odcs']);

add('03 / Validate','A contract nobody checks is documentation','Each line of the contract becomes one query on the feed. Lab 4.2’s validator writes them.',
'<table class="mapping compact"><thead><tr><th>Contract line</th><th>Query</th><th>Result</th></tr></thead><tbody>'+
 '<tr><td><code>payment_id: unique</code></td><td><code>count(*) - count(DISTINCT payment_id)</code></td><td>0 ✓</td></tr>'+
 '<tr><td><code>amount: required</code></td><td><code>count(*) WHERE amount IS NULL</code></td><td>0 ✓</td></tr>'+
 '<tr><td><code>status: validValues [captured, failed, refunded]</code></td><td><code>WHERE status NOT IN (…)</code></td><td>0 ✓</td></tr>'+
 '<tr><td><code>card_last4: pattern ^[0-9]{4}$</code></td><td><code>WHERE NOT regexp_full_match(card_last4, …)</code></td><td>0 ✓</td></tr>'+
 '<tr><td><code>type: sql · amount &lt;= 0 · mustBe: 0</code></td><td><code>SELECT count(*) FROM {object} WHERE amount &lt;= 0</code></td><td style="color:#a4473b;font-weight:600">16 ✗</td></tr></tbody></table>',
'<strong>Executable contract:</strong> every rule is a query with an expected result. Run it on every load.',
'Lab 4.2 task 2: validate() walks the contract and turns each line into a query: types against DESCRIBE, required into a null count, unique into a duplicate count, and each quality rule into its metric. The custom SQL rule replaces {object} with the table. Khalid’s row passes every rule; the feed as a whole doesn’t. In production the datacontract CLI does exactly this (datacontract test), against DuckDB, Snowflake, Databricks or a Kafka topic.',['lab','cli']);

add('03 / Validate','26 of 27 checks pass. The 27th found real rows.','Payments with amount ≤ 0 since 1 August. Khalid’s is fine; one three orders after his is not.',
khalidPay(0,0,560)+label('✓ every rule',580,22,'ptr sla',160,'k-ok')+badRows(0,110,560)+label('<b>16</b>payments with amount ≤ 0',0,400,'answer',300,'n-bad')+
 box('Breach of payments_positive_amount','<p>owner: payments-provider-integration<br>support: #payments-data</p>',780,110,660,'quarantine','breach','from the contract’s team and support'),
'<strong>A breach, not “weird data”:</strong> the contract names who fixes it and where to ask.',
'The real report: 26 of 27 checks pass, and payments_positive_amount finds 16 rows, from −0.21 to −124.56 SAR. pay_9040081 belongs to order 5043886, placed three orders after Khalid’s on 28 September. Before the contract, someone in Finance would have noticed odd totals weeks later and asked around. Now the failing rule has an owner and a channel in the same file: that social half is most of the value.',['lab']);

const V2=[['amount','renamed to amount_sar'],['card_last4','string → integer'],['status','adds the value chargeback'],['wallet_provider','optional → required'],['installments','a new column']];
add('04 / Change','The provider sends v2 of the contract','Five changes for next month. Which ones break something at Qafila?',
tbl('v2',0,0,900,'payments feed · v1.0.0 → v2.0.0','260px 1fr',[H('property','proposed change'),...V2.map(([p,c])=>[p,SANS(c)])],'mono')+provider(1100,30,220)+
 note('Judge each change from the consumer’s side: does an existing query fail, or quietly change its answer?',0,330,'teal',1000),
'<strong>Producers change their data.</strong> The question is never whether, but which changes consumers can absorb.',
'Lab 4.2 task 3 builds this v2 as a copy of v1 with five edits. Read them out and let the room sort them into breaking and safe before the next boards. A reasonable first instinct is “only removed columns break things”: the next board tests that.',['lab']);

add('04 / Change','status gets a new value, chargeback. Breaking?','Finance’s revenue logic: CASE WHEN status = ‘refunded’ THEN 0 ELSE amount END.',
art('analyst',1120,30,250,'Finance','“Our CASE knows three statuses.”','f-fin')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>No: no column was added or removed</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>Yes: chargebacks would count as revenue, silently</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Only if the type of status changes</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: what does the CASE do with a chargeback?</p>',
'Code that handles <strong>every known value</strong> mishandles a new one, often without an error.',
'Allow 45 seconds. B is right: a chargeback is money taken back, but the CASE only zeroes refunded, so every chargeback stays in revenue. No query fails, which is what makes it dangerous. A is the common misconception (only structural changes break things). C confuses type with meaning: the column is still a string. Widening the set of allowed values is a breaking change; narrowing it is safe.',[],
{quiz:{answer:'B',right:'Yes. The CASE only knows refunded, so every chargeback stays in revenue.',wrong:'Run a chargeback through the CASE by hand. Try again.'}});

add('04 / Change','Three of the five changes break something','Lab 4.2’s diff_contracts finds them. The rule: could an existing query fail, or change its answer?',
tbl('v2',0,0,1440,'payments feed · v1.0.0 → v2.0.0','230px 300px 130px 1fr',[H('property','proposed change','breaking?','what it does to Qafila'),
 {c:['amount',SANS('renamed to amount_sar'),BAD('yes'),SANS('every query on amount fails: 4 dbt models read it')],cls:'unk'},
 {c:['card_last4',SANS('string → integer'),BAD('yes'),SANS('3,728 values start with 0: ‘0522’ becomes 522')],cls:'unk'},
 {c:['status',SANS('adds chargeback'),BAD('yes'),SANS('CASE logic counts chargebacks as revenue')],cls:'unk'},
 ['wallet_provider',SANS('optional → required'),K('no'),SANS('consumers get more, never less')],
 ['installments',SANS('a new column'),K('no'),SANS('existing queries don’t select it')]],'mono'),
'<strong>Breaking:</strong> removed or renamed, retyped, newly optional, more allowed values. <strong>Safe:</strong> added, now required, fewer values.',
'The lab’s three breaking changes, each with its real effect. card_last4 is text for a reason: 3,728 of the 41,785 payments have last four digits starting with 0, like pay_9040149’s 0522, and an integer drops the zero. A required field becoming optional (the reverse of wallet_provider here) would also break consumers that never expected nulls. Lab 4.2 checks diff_contracts against exactly these sets.',['lab']);

add('04 / Change','A breaking change is a new major version','Publish v2 next to v1, give consumers time to move, then retire v1.',
label('<b>v1.0.0</b>amount, three statuses',0,60,'answer',380,'v1')+label('<b>v2.0.0</b>amount_sar, chargeback',520,60,'answer',380,'v2')+
 label('',0,190,'tl-bar',900,'bar-v1')+label('',520,262,'tl-bar',880,'bar-v2')+label('v1 served',20,196,'caption',300,'bar-v1-t')+label('v2 served',540,268,'caption',300,'bar-v2-t')+
 label('migration window',520,340,'tl-zone',380,'win')+label('v1 retired',900,340,'tl-zone',300,'retire')+
 note('diff_contracts in the provider’s CI: 3 breaking changes block the merge of v2 as a minor version.',0,420,'teal',1440),
'<strong>Semantic versioning for data:</strong> breaking changes bump the major version and come with a migration window.',
'The same rule as for APIs. A minor version (1.1.0) may only add safe changes; anything breaking is 2.0.0 and is published beside 1.0.0, often as a second table or topic. Consumers migrate within an agreed window, then v1 is retired. Running the diff in the producer’s CI turns the rule into a merge check; datacontract changelog does it from two YAML files.',['cli']);

add('05 / Who must be told','Lineage lists who reads raw.payments','dbt’s manifest knows every model and test downstream of the feed.',
svg(path('M230 90 L280 90','em','w-l1')+path('M520 90 L570 90','em','w-l2')+path('M830 90 L880 90','em','w-l3')+path('M1110 90 L1160 90','em','w-l4'))+
 box('raw.payments','<p>the feed</p>',0,30,230,'bronze','l-raw')+box('stg_payments','<p>2 tests</p>',290,30,230,'silver','l-stg')+box('int_payments<br>_per_order','<p></p>',580,30,250,'silver','l-int')+box('fct_orders','<p>4 tests</p>',890,30,220,'gold','l-fct')+box('mart_daily<br>_city_sales','<p>1 test</p>',1170,30,270,'gold','l-mart')+
 label('<b>4</b>models to update',0,320,'answer',300,'n-models')+label('<b>7</b>tests that read them',340,320,'answer',300,'n-tests')+label('<b>Finance’s dashboard</b>reads the last one',680,320,'answer',500,'n-dash'),
'<strong>Impact analysis:</strong> walk the lineage downstream of a change; every node is work, every owner gets a message.',
'Lab 4.2 task 5 walks the manifest’s child_map from source.qafila.raw.payments: stg_payments (unique and not_null on payment_id), int_payments_per_order, fct_orders (unique, not_null, the relationship to dim_customers, and the singular test assert_order_totals_match_payments) and mart_daily_city_sales (one uniqueness test). That is the migration list for v2. Column-level lineage (dbt Cloud, SQLMesh, OpenLineage-based catalogs) narrows it to the models that use amount.',['lab','openlineage']);

const PILLARS=[['Freshness','when did it last change?'],['Volume','did the expected number of rows arrive?'],['Schema','columns added, removed, retyped'],['Distribution','nulls, ranges, the mix of values'],['Lineage','what feeds it, who reads it']];
add('06 / Observability','Monitors watch what nobody promised','A contract checks what you expect. Five signals to watch on every important table, every day.',
PILLARS.map(([t,d],i)=>box(t,`<p>${d}</p>`,i*292,0,272,i<2?'silver':'','p'+i)).join('')+art('monitoring',600,250,190,'','','f-mon')+
 note('Today: freshness and volume, on the payments feed and the orders table.',0,300,'teal',560)+note('+ cost: which models and queries burn the budget? Watch it like any metric.',860,300,'',580),
'<strong>Data observability:</strong> freshness, volume, schema, distribution and lineage, watched over time.',
'Tests and contracts are assertions you wrote down in advance. Monitors learn what normal looks like and flag deviations you didn’t think to test: a table that stopped updating, a day with half the rows, a column whose null rate jumped. Schema changes are the contract diff from the last chapter; lineage is the previous board. Cost (FinOps) is the sixth signal many teams add: the most expensive model is often the least used.',[]);

add('06 / Freshness','Is the payments feed late?','The contract says latency 1 day. Lab 4.2 checks at 06:00 UTC on 1 October.',
rec('fr-last',0,20,640,'raw.payments · max(updated_at)','the newest change in the feed','30 Sep 23:59 UTC','copy')+rec('fr-now',0,120,640,'now','the morning after the last load','1 Oct 06:00 UTC','')+
 label('<b>6.0 hours</b>since the last change',0,240,'answer',360,'fr-lag')+label('<b>24 hours</b>allowed by the contract',400,240,'answer',360,'fr-sla')+label('<b>✓ fresh</b>',800,240,'answer',200,'fr-ok')+
 code(['slaProperties:','  - property: latency','    value: 1','    unit: d','    element: payments.updated_at'],760,0,560,'fr-yaml'),
'<strong>Freshness:</strong> the time since a table last changed, against what the contract promises.',
'Lab 4.2 task 4, first half: the newest updated_at in the feed is 30 September 23:59:59 UTC; at 06:00 the next morning the feed is 6 hours old, well inside the 1-day latency. The threshold comes from the contract, not from the engineer’s memory. dbt source freshness does the same for dbt sources (our _sources.yml has loaded_at_field entries), and Dagster runs freshness checks on assets.',['lab','sla','freshness']);

add('07 / Volume','Orders per day: where is the incident?','Every day of August and September, as Lab 4.2 loads them. Thursdays and Fridays are the weekend peaks.',
chart([],i=>THUFRI(i)?'peak':'')+label('<b style="color:#ac713b">amber</b> Thursdays and Fridays<br><b>the spike</b> National Day, 23 September',1080,20,'reason',360,'legend'),
'<strong>Volume:</strong> did roughly the expected number of rows arrive today?',
'Real daily order counts (UTC days) with Lab 4.2’s simulated incident: on Tuesday 29 September a load job kept only 308 of the day’s 765 orders and still reported success. Amber bars are Thursdays and Fridays, Qafila’s weekend, about 25% busier than other days. The spike on Wednesday 23 September is Saudi National Day (the WATANI26 promotion, 1,423 orders). Let the room find the incident first, then ask how a program would find it.',['lab']);

add('07 / Volume','A naive monitor cries wolf every weekend','Expected = the median of the previous 7 days. Alert when a day is more than 20% off.',
chart(NAIVE,peaks)+label('<b>16</b>alerts in two months',1080,0,'answer',360,'n-alerts')+note('14 of them are ordinary Thursdays and Fridays.',1080,110,'red',360),
'An alert that fires every week gets <strong>muted</strong>, and then it misses the real incident.',
'Lab 4.2’s naive monitor: 16 alerts, one red dot each. 14 are Thursdays and Fridays, which are always about 25% above the previous week’s median; the other two are National Day and the incident. After the second Thursday, the on-call engineer mutes the channel, and on 29 September nobody looks. A noisy monitor is worse than none: it teaches people to ignore alerts.',['lab']);

add('07 / Volume','Compare each day with the same weekday','Expected = the median of the same weekday over the previous 4 weeks. Alert at 30% off.',
chart(SMART,i=>i===INCIDENT?'bad':i===NATDAY?'cal':THUFRI(i)?'peak':'')+label('<b>2</b>alerts in two months',1080,0,'answer',360,'n-alerts')+
 note('23 Sep · +102% · on the calendar: National Day',1080,110,'',360)+note('29 Sep · −57% · the incident',1080,230,'red',360),
'<strong>Model the seasonality:</strong> weekdays, Ramadan, month-end. Then only the unusual days alert.',
'Lab 4.2’s smart monitor: for Tuesday 29 September the expected value is the median of the four previous Tuesdays, 720.5 orders; 308 is 57% below it. For National Day, the four previous Wednesdays give 704; 1,423 is 102% above. Two alerts in two months, both worth reading. Real tools fit the same idea with more care: Elementary and Soda use rolling baselines per weekday, Monte Carlo learns the seasonality for you.',['lab']);

add('07 / Route','Explain the planned, page the owner for the rest','A calendar of planned events explains National Day. The incident goes to whoever owns the table.',
rec('cal',0,20,640,'calendar · 2026-09-23','Saudi National Day promo (WATANI26)','explained','chg')+rec('inc',0,130,640,'orders · 2026-09-29','308 orders, expected 720.5','−57%','wrong')+
 svg(path('M650 165 C720 165 740 150 790 150','danger','w-route'))+box('The table’s owner','<p>from the contract’s team<br>and support channel</p>',800,90,420,'quarantine','owner')+art('engineer',1260,70,180,'','','f-eng')+
 note('Without an owner, an alert goes to everyone: so, to no one.',0,330,'teal',1440),
'<strong>Route every alert:</strong> to the owner named in the contract, with the evidence attached.',
'Lab 4.2 ends the monitor task with exactly this split: alerts on dates in CALENDAR are explained (planned promotions, holidays, a known outage); the rest are incidents. The incident message carries the table, the day, the observed and the expected value. The owner comes from the same contract that defined the checks: one file for what to check and whom to tell.',['lab']);

add('08 / The tools','The same ideas, ready-made','Everything on today’s board exists as a standard, a product or a built-in.',
'<table class="mapping compact"><thead><tr><th>On the board</th><th>In production</th></tr></thead><tbody><tr><td>a contract as a file</td><td>ODCS (Bitol) · the <code>datacontract</code> CLI: <code>test</code>, <code>changelog</code>, <code>export</code> to dbt</td></tr><tr><td>checks inside the pipeline</td><td>dbt tests and model contracts · Dagster asset checks · Soda checks</td></tr><tr><td>freshness</td><td><code>dbt source freshness</code> · Dagster freshness checks</td></tr><tr><td>volume and distribution anomalies</td><td>Elementary · Soda · Monte Carlo · Datadog Data Observability</td></tr><tr><td>lineage across tools</td><td>dbt’s manifest · OpenLineage · catalogs (DataHub, OpenMetadata, Unity Catalog)</td></tr></tbody></table>',
'<strong>You built each piece by hand in the lab;</strong> in production you configure them.',
'Map each row to its board. The datacontract CLI reads ODCS files and runs the same checks as Lab 4.2’s validator, diffs two versions for breaking changes, and exports the rules as dbt tests (the lab’s stretch task does that by hand). OpenLineage is the open standard that lets Spark, Airflow, Dagster and dbt report lineage to one catalog. The capstone pipeline already runs the payments contract as a Dagster asset check.',['cli','openlineage','freshness']);

const WORDS=[['Data contract','a producer’s promise, as a file'],['Producer, consumer','who sends the data, who relies on it'],['ODCS','the open YAML format for contracts'],['Quality rule','a metric and its expected value'],['Breach','a failing rule, with an owner'],['Breaking change','an existing query fails or changes'],['Major version','where breaking changes go'],['Lineage','what feeds a table, who reads it'],['Freshness','time since the last change'],['Volume','rows that arrived, against expected'],['Seasonality','the normal weekly rhythm'],['Alert routing','the owner gets the alert']];
add('09 / The words','Twelve words for trusting data','Every one appeared on a board today. Lab 4.2 uses them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>Write the promise down, run it on every load, and watch every table for what nobody promised.</strong>',
'A reference board: point at each word and ask the room which board it appeared on, and what it meant for Khalid’s payment or for the 29 September load.');

const RECALL=[
 ['Day 2 already has dbt tests. What does a contract on the payments feed add?','A promise from a team we don’t control, checked on their data, with an owner and a support channel for breaches.'],
 ['The contract’s SQL rule finds 16 payments with amount ≤ 0. What happens next?','It’s a breach: the contract names the owner (payments-provider-integration) and the channel (#payments-data).'],
 ['v2 adds chargeback to the allowed statuses. Breaking or not?','Breaking: code that knows three statuses mishandles the fourth. Chargebacks would count as revenue.'],
 ['card_last4 moves from string to integer. What breaks?','3,728 values start with 0, like 0522, and lose the zero. It’s a breaking change: a new major version.'],
 ['Why does a 7-day median alert every Thursday, and what fixes it?','Thursdays and Fridays are 25% busier. Compare with the same weekday over 4 weeks: 2 alerts, both real.']];
add('10 / Recall','Your turn: answer before you look','Five questions on Khalid’s feed and the monitors that watch it. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer. The next slide fills in all five answers in place.');

add('10 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>A contract, checked on every load, and monitors that know the weekly rhythm:</strong> the whole session in one line.',
'Go back to the board for any answer the room missed: contracts at the seams (7), the validation result (9), the chargeback quiz (11), breaking changes (12), the two monitors (18–19).');

add('Lab 4.2 · 25 min','Data contracts and observability','Write and enforce the payments contract, judge the provider’s v2, and build monitors that don’t cry wolf.',
`<div class="references"><div><h3>Lab 4.2 · 25 minutes</h3><p>1 · Read the contract; add the status property · 5′</p><p>2 · Validate the feed against the contract · 7′</p><p>3 · The provider proposes v2: breaking or not? · 5′</p><p>4 · Freshness and volume monitors · 6′</p><p>5 · Impact analysis with dbt lineage · 2′</p><p class="note">✱ Generate dbt tests from the contract: one source of truth, many enforcement points. Find Khalid’s payment in the feed and check it passes every rule.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D4_2_contracts_observability.ipynb</a><a href="${sources.odcs}" target="_blank" rel="noopener">Reference · Open Data Contract Standard v3.1</a><a href="${sources.cli}" target="_blank" rel="noopener">Tool · the datacontract CLI</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next: governance. Khalid’s national ID is in the warehouse too: who may see it?',
'Hand over to the lab. A quick first check after task 2: 26 of 27 checks pass, and the failing one is payments_positive_amount with 16 rows. After task 4, the smart monitor must alert on 29 September only, with 23 September explained by the calendar.',['lab']);

Story.start({brand:'QAFILA / CONTRACTS AND MONITORS',sources});
})();
