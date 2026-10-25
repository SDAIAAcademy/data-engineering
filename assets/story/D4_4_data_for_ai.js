/* D4_4 Data for AI, told on one evolving board (engine: story.js), one idea per board.
   The running example is a question from Operations, asked in Arabic: what was the average basket in Jeddah on National Day?
   Khalid's order 5043883 (00_intro.js) shows why "revenue" needs one definition: 80.50 SAR of items, 89.50 with the
   delivery fee. His two Al Waha Arabic Coffees are the first product the board embeds.
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every number is real course data and what Lab 4.4 prints with its rule-based planner (no LLM needed): the three Riyadh
   revenues, the search scores, the retrieval scores, the four blocked plans and the 80% eval. The deck ends with the
   capstone kickoff (labs/capstone/README.md). */
(() => {
'use strict';
const {art,label,note,path,svg,box,tool,rec,tbl,recall,platform,add}=Story;
const repo='https://github.com/SDAIAAcademy/data-engineering/blob/main/labs/';
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={dbtsl:'https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl',metricflow:'https://docs.getdbt.com/docs/build/about-metricflow',array:'https://duckdb.org/docs/stable/sql/functions/array',vss:'https://duckdb.org/docs/stable/core_extensions/vss',model2vec:'https://github.com/MinishLab/model2vec',intro:'00_intro.html',d22:'D2_2_modeling.html',d43:'D4_3_governance_privacy.html',lab:colab('D4_4_data_for_ai.ipynb'),capstone:repo+'capstone/README.md',template:repo+'capstone/TEMPLATE_README.md',brief:'capstone.html'};
const H=(...c)=>({c,cls:'h'}),SANS=v=>`<span style="font-family:var(--sans)">${v}</span>`,BAD=v=>`<em class="k bad">${v}</em>`,K=v=>`<em class="k">${v}</em>`;
const AR=v=>`<bdi style="font-family:var(--sans)">${v}</bdi>`;
// Code on a board: leading spaces become non-breaking, so YAML and SQL keep their indentation.
const code=(lines,x,y,w,id)=>label(lines.map(l=>l.replace(/^ +/,s=>'&nbsp;'.repeat(s.length))).join('<br>'),x,y,'code',w,id);

// The question of the deck, and who asks it.
const ASK='كم متوسط قيمة السلة في جدة يوم اليوم الوطني؟',ASK_EN='What was the average basket in Jeddah on National Day?';
const manager=(x=1200,y=20,w=220,detail='a store manager in Jeddah')=>art('operations',x,y,w,'Operations',detail,'f-ops');
const engineer=(x,y,w,detail='')=>art('engineer',x,y,w,'Data team',detail,'f-eng');
const question=(x=0,y=40,w=760)=>rec('ask',x,y,w,AR(ASK),`“${ASK_EN}”`,'','src');
// Riyadh, September 2026, from marts.fct_orders: three honest definitions of revenue.
const REV=[['items, delivered orders','Finance’s definition (D2_2)','1,748,564.50'],['items + delivery fees, delivered','what customers paid','1,842,731.50'],['items, every order','cancelled and refunded too','1,980,138.76']];

add('Day 4 · Session 4','Data for AI','',
svg(path('M180 130 C200 160 210 190 230 215','em')+path('M560 240 C580 210 590 180 600 160','em'))+manager(0,0,180,'')+rec('ask',170,230,400,AR('متوسط السلة في جدة؟'),'National Day · avg_basket_sar','148.59','copy')+engineer(540,0,180),
'Give AI the dashboards’ numbers: metrics defined once, search by meaning, plans instead of SQL.',
'About 25 minutes for the board, then Lab 4.4 (25 minutes), then 10 minutes of capstone kickoff. AI is one more consumer of the gold layer: a model that answers managers’ questions in Arabic or English. It needs what a dashboard needs (one definition per metric, clean gold tables, access rules) plus search by meaning. Every number on the board comes from the course data and Lab 4.4; the lab runs without an LLM, using a rule-based planner, and an OpenAI-compatible endpoint is optional. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 4</p><h1>Data<br><span class="teal">for AI</span></h1><p class="lead">A question in Arabic, one definition of revenue,<br>and a model that never writes SQL.</p>'});

add('00 / You are here','AI is one more consumer of the gold layer','The last two pieces of the map sit in Serve, next to the marts Finance already reads.',
platform(n=>n==='Semantic layer'||n==='Embeddings'?'fresh':'met',{'Semantic layer':'One definition per metric',Embeddings:'Search by meaning'}),
'<strong>The serve stage gets a new reader:</strong> a model answering questions, which needs the same governed numbers as a dashboard.',
'Every other piece of the map is met. Today adds two in Serve: a semantic layer (metrics defined once, compiled to SQL for every consumer) and embeddings (vectors for search by meaning, across Arabic and English). Both read the gold layer that Day 2 built and Day 4 runs, watches and protects. After the lab, the whole map is the capstone’s checklist.');

const QT='Operations asks, in Arabic, at the end of a long day',QS='A store manager wants one number, without writing SQL or waiting for the data team.';
const ask=()=>svg(path('M1190 110 L770 90','em','w-ask'))+question()+manager();
add('01 / The question',QT,QS,ask()+note('An assistant could answer it. With which numbers?',0,170,'teal',760),
'<strong>Ask your data:</strong> the question is easy to write. The answer depends on definitions the model doesn’t have.',
'A build: two boards. The question is Lab 4.4’s: the average basket in Jeddah on Saudi National Day, 23 September. Assistants that “ask your data” are everywhere now: Snowflake Cortex Analyst, Databricks Genie, BI copilots, internal agents. Ask the room: if a model wrote SQL against our raw tables for this question, what would it have to guess?',['intro']);

add('01 / The question',QT,QS,ask()+
 label('<b>Which basket?</b><br>items only, or with the 9 SAR delivery fee?',0,190,'reason',560,'g1')+label('<b>Which orders?</b><br>delivered only, or cancelled too?',0,280,'reason',560,'g2')+label('<b>Which day?</b><br>23 September in Riyadh time, or in UTC?',0,370,'reason',560,'g3')+
 note('Each guess changes the number. None raises an error.',560,300,'red',600),
'A model writing SQL on raw tables answers <strong>confidently</strong>, and <strong>differently</strong> each time it guesses.',
'Three guesses hide in one sentence. Basket: items_amount, or order_total with the fee. Orders: delivered only, or every status. Day: the app stores UTC, but Qafila’s day is Riyadh time (D2_1 converted it). Finance answered all three on Day 2: revenue is delivered items, without delivery fees, by Riyadh date. A human analyst learns that in their first week; a model has to be given it.',['d22']);

add('01 / Three revenues','One month, one city, three honest numbers','Riyadh in September 2026. Each query is correct SQL; each means something different by revenue.',
tbl('rev',0,0,1000,'Riyadh · September 2026<span class="src">marts.fct_orders</span>','1fr 300px 190px',[H('revenue means','used by','SAR'),{c:[SANS(REV[0][0]),SANS(REV[0][1]),REV[0][2]],cls:'hit'},...REV.slice(1).map(([m,u,v])=>[SANS(m),SANS(u),v])],'mono')+
 rec('k-ord',1060,20,380,'order 5043883','Khalid · items 80.50 + fee 9.00','89.50','order')+note('80.50 in Finance’s revenue, 89.50 on his card.',1060,130,'teal',380)+
 label('<b>231,574 SAR</b>between the lowest and the highest: 13%',0,250,'answer',560,'gap'),
'<strong>“Revenue” is a definition, not a column.</strong> Whoever writes the SQL picks one, unless the platform already has.',
'Real totals from Lab 4.4’s warehouse. Delivered items only: 1,748,564.50 (10,890 delivered orders), Finance’s definition since D2_2 and the number on the dashboard mart (D2_3). Add the delivery fees: 1,842,731.50. Count every order, including 893 cancelled, 286 failed deliveries and 280 refunded: 1,980,138.76. Khalid’s order shows the difference in one row: Finance counts his 80.50 of items, his card paid 89.50. A dashboard, a notebook and an assistant can each be right about a different question.',['lab','intro']);

add('01 / Three revenues','The dashboard, a notebook and the assistant disagree. What fixes it for good?','Each one reports a different revenue for Riyadh last month.',
art('analyst',1120,30,250,'Finance','“Which one goes to the board meeting?”','f-fin')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>Paste the dashboard’s SQL into the assistant’s prompt</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>Define revenue once, and have all three ask for that metric</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Fine-tune the model on Finance’s reports</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: which fix still holds when Finance changes the definition?</p>',
'<strong>One definition, compiled for every consumer,</strong> gives the same number by construction.',
'Allow 45 seconds. B is right: a metric defined once, in one file, compiled to SQL for the dashboard, the notebook and the assistant. A fixes today’s prompt and breaks the day the dashboard’s SQL changes; the notebook still disagrees. C makes the model better at guessing, not right: it learns last quarter’s numbers, not the definition. The next chapter builds B.',[],
{quiz:{answer:'B',right:'Yes. One definition in one place: change it once, and every consumer follows.',wrong:'Ask what happens when Finance adds delivery fees next month. Try again.'}});

const YAML=['model: orders','from: marts.fct_orders AS o','joins:','  - marts.dim_stores AS s USING (store_id)','dimensions:','  city:  {expr: s.city}','  month: {expr: "date_trunc(\'month\', o.order_date)::DATE"}','metrics:','  revenue_sar:','    expr: "round(sum(o.items_amount)','           FILTER (WHERE o.status = \'delivered\'), 2)"','    description: Revenue, sales, money made from delivered','      orders in SAR, excluding delivery fees.','  avg_basket_sar:','    expr: "round(avg(o.items_amount) FILTER (…), 2)"'];
const ANN=[[22,'which table, which joins'],[118,'what people group and filter by'],[214,'each metric: its SQL, once'],[262,'and its words, for people and models']];
add('02 / Semantic layer','Metrics defined once, in a file','Lab 4.4’s semantic layer, shortened: 6 metrics and 6 dimensions over the Day 2 star schema.',
code(YAML,0,0,860,'yaml')+svg(ANN.map(([y],i)=>path(`M880 ${y+14} L960 ${y+14}`,'em',`w-a${i}`)).join(''))+ANN.map(([y,t],i)=>label(t,980,y,'hand teal',460,`ann${i}`)).join(''),
'<strong>Semantic layer:</strong> business definitions (metrics, dimensions, joins) stored once and compiled to SQL.',
'The real file from Lab 4.4 task 1, shortened. The from and joins say where the data lives; dimensions are what people group and filter by (order_date, month, city, store_id, payment_method, promo_code); metrics are expressions with a description in English and Arabic (orders, delivered_orders, revenue_sar, avg_basket_sar, sla_rate, refund_rate). revenue_sar is Finance’s D2_2 definition, written down. The same idea at scale: the dbt Semantic Layer with MetricFlow, Cube, LookML, Snowflake semantic views.',['lab','dbtsl','metricflow']);

const SQ=['{"metrics":    ["revenue_sar"],',' "dimensions": ["city"],',' "filters":    [["month", "=", "2026-09-01"]]}'];
const SQL=['SELECT s.city AS city,','  round(sum(o.items_amount) FILTER (WHERE o.status = \'delivered\'), 2) AS revenue_sar','FROM marts.fct_orders AS o','JOIN marts.dim_stores AS s USING (store_id)','WHERE date_trunc(\'month\', o.order_date)::DATE = ?','GROUP BY ALL','ORDER BY city','-- params: [\'2026-09-01\']'];
add('02 / Compile','Ask for metrics by dimensions; the layer writes the SQL','A semantic query names what it wants. compile_query turns it into SQL and a list of parameters.',
code(SQ,0,0,640,'sq')+label('a semantic query: data, not SQL',680,20,'hand teal',460,'p-sq')+svg(path('M40 90 L40 120','em','w-c'))+code(SQL,0,130,1000,'sql')+
 tbl('res',1060,130,380,'result<span class="src">September</span>','1fr 150px',[H('city','revenue_sar'),['Dammam','600,239.76'],['Jeddah','1,062,633.91'],{c:['Riyadh','1,748,564.50'],cls:'hit'}],'mono'),
'<strong>Semantic query:</strong> metrics, dimensions and filters as data. Compiling it is mechanical, and testable.',
'Lab 4.4’s compile_query, run on its example: select the dimension and metric expressions, add the from and the joins, turn each filter into the dimension’s expression and a question mark, group and order by the dimensions. The value 2026-09-01 never enters the SQL text: it travels in params. The result is the lab’s output; Riyadh is the first row of the previous chapter’s table, by construction.',['lab']);

add('02 / Compile','Same number as the dashboard; values stay values','Lab 4.4 checks both: the mart’s revenue, and a filter that tries to inject.',
rec('sem',0,20,640,'semantic layer · revenue_sar','Riyadh · September · from the YAML','1,748,564.50','copy')+rec('mart',0,120,640,'marts.mart_daily_city_sales','Finance’s dashboard (D2_3)','1,748,564.50','copy')+
 label('<b>✓</b> equal for every day and city',0,230,'check',640,'ok1')+
 code(['filters: [["city", "=",','  "Riyadh\'; DROP TABLE x; --"]]','','→ WHERE s.city = ?','  params: ["Riyadh\'; DROP TABLE x; --"]'],760,20,680,'inj')+label('<b>✓</b> the value stays a string',760,230,'check',680,'ok2')+
 note('The assistant and the dashboard now share one definition, and one way in.',0,380,'teal',1440),
'<strong>By construction:</strong> one definition gives one number; parameters keep values out of the SQL.',
'The lab’s two checks. First, the semantic layer’s revenue_sar and sla_rate by order_date and city match marts.mart_daily_city_sales row for row: the dashboard and the layer agree. Second, a filter value carrying an injection attempt is passed as a parameter, so DuckDB compares the city to that odd string and finds nothing; the SQL text never contains DROP. When Finance changes the definition (say, to include fees), it changes in one place.',['lab']);

const VEC='[0.055, −0.168, 0.251, −0.055, −0.132, −0.192, … ]';
add('03 / Embeddings','Text becomes a list of numbers','An embedding maps text to a vector; texts with similar meaning get nearby vectors, in any language.',
rec('p1193',0,30,620,'product 1193 · Al Waha Arabic Coffee',`${AR('قهوة عربية الواحة')} · Dates &amp; Coffee`,'37.97','line')+note('Khalid bought two (the introduction).',0,140,'teal',620)+
 svg(path('M640 75 L740 75','em','w-emb'))+tool('U.embed','one string per product: both names, category, description',760,130,680,'t-emb')+
 label(`<b>${VEC}</b><span>256 numbers</span>`,760,30,'rec copy',680,'vec')+
 label('minishlab/potion-multilingual-128M · a static model2vec model · CPU, no GPU',760,250,'caption',680,'model'),
'<strong>Embedding:</strong> a vector for a text, so that closeness in numbers means closeness in meaning.',
'Lab 4.4 task 2 embeds each of the 347 active products: its English name, Arabic name, category and description in one string. The model is a static multilingual model2vec model: fast on a laptop CPU, 100+ languages, 256 dimensions. These are the first six numbers of Al Waha Arabic Coffee’s real vector. No single number means anything; only distances between vectors do.',['lab','model2vec']);

const SEARCH=[[AR('قهوة عربية'),'Al Waha Arabic Coffee','Dates &amp; Coffee','0.756',''],[AR('حليب للفطور'),'Al Waha Fresh Milk','Dairy','0.550',''],['frozen food for the weekend','Sahra Frozen Vegetables','Frozen','0.513',''],['something to clean the dishes','Barakah Laundry Detergent','Household','0.338','unk']];
add('03 / Search','Search by meaning, across Arabic and English','DuckDB ranks the 347 vectors by cosine similarity to the query’s vector.',
tbl('search',0,0,1440,'search(query)<span class="src">top result</span>','420px 1fr 260px 130px',[H('query','product','category','score'),...SEARCH.map(([q,p,c,s,cls])=>({c:[SANS(q),SANS(p),SANS(c),s],cls}))],'mono')+
 code(['SELECT name_en, array_cosine_similarity(vec, ?::FLOAT[256]) AS score','FROM product_vectors JOIN raw.products USING (product_id)','ORDER BY score DESC LIMIT 5'],0,260,1000,'q-sql')+
 note('Right aisle, wrong product: Dish Soap is in the catalog. A score of 0.338 is a weak match.',1040,250,'red',400),
'<strong>Semantic search</strong> finds the right shelf in either language. Read the scores: a low one is a guess.',
'Lab 4.4’s results, top hit per query. The Arabic queries find Arabic Coffee and Fresh Milk although the query shares no exact words with the English names. The last row is the lesson: the lab’s check only asks for the Household category, and passes, but the top hit is laundry detergent, not one of the four dish soaps. Static embeddings are cheap and good at topics, weaker at fine distinctions; a bigger model or a keyword re-ranker fixes it. Measure search like any feature.',['lab','array']);

add('03 / The pipeline','An embedding pipeline is a data pipeline','Rows in, text built, vectors stored next to the model that made them.',
svg(path('M300 90 L370 90','em','w-e1')+path('M690 90 L760 90','em','w-e2'))+box('raw.products','<p>347 active products</p>',0,30,300,'bronze','e-raw')+box('text','<p>both names · category · description</p>',380,30,310,'silver','e-text')+box('product_vectors','<p>product_id · vec FLOAT[256]<br>model: potion-multilingual-128M</p>',770,30,500,'gold','e-vec')+
 label('<b>A description changes</b> · re-embed that row: change data capture, as in D1_4',0,260,'check',1440,'c1')+label('<b>A new model</b> · re-embed everything; never compare vectors from two models',0,320,'check',1440,'c2')+label('<b>Millions of rows</b> · add an index: DuckDB vss (HNSW), pgvector, LanceDB, Qdrant',0,380,'check',1440,'c3'),
'<strong>Keep vectors in sync</strong> with their source rows, and version the model next to them.',
'The vectors are a derived table like any mart: they go stale when the source changes. Re-embed changed rows on each run (the same incremental logic as D1_4 and D2_3), store the model name beside each vector, and treat a model change as a full rebuild. At 347 products a full scan is instant; at catalog scale an approximate index (HNSW in DuckDB’s vss extension, pgvector in Postgres) keeps search fast.',['vss']);

const RET=[['How much did we sell in Jeddah?','revenue_sar','0.239'],[AR('كم متوسط قيمة السلة في الرياض؟'),'avg_basket_sar','0.500'],['Are we delivering on time?','sla_rate','0.449']];
add('04 / Retrieval','The same trick finds the right metric','Embed each metric’s description once; match every question against them.',
tbl('ret',0,0,1100,'top_metrics(question)<span class="src">Lab 4.4 task 3</span>','1fr 260px 130px',[H('question','metric','score'),...RET.map(([q,m,s])=>[SANS(q),m,s])],'mono')+
 note('The model then sees 3 definitions, not 6, and never the raw tables.',0,220,'teal',1100),
'<strong>Retrieval:</strong> pick the few definitions that matter, and give only those to the model.',
'Lab 4.4’s real scores. This is the retrieval half of “RAG over the semantic layer”: the prompt contains the top metrics and the list of dimensions, nothing else. Fewer names to choose from means fewer wrong choices, a shorter prompt, and no table or column the model could misuse. The Arabic question finds avg_basket_sar because the description includes متوسط قيمة السلة.',['lab']);

add('04 / Retrieval','“How punctual are the couriers?” finds the wrong metric','The question’s words aren’t in sla_rate’s description. The fix is in the semantic layer, not the prompt.',
rec('before',0,20,640,'before','delivered_orders 0.311 · sla_rate 0.264','✗','wrong')+rec('after',0,240,640,'after','sla_rate 0.533 · delivered_orders 0.311','✓','copy')+
 svg(path('M320 120 L320 225','em','w-fix'))+code(['sla_rate.description +=','  "Punctual, on time, late deliveries,','   courier punctuality, delivery speed.','   <bdi>دقة مواعيد التوصيل والتأخير</bdi>"'],760,100,680,'desc'),
'<strong>Descriptions are product surface:</strong> write the words people actually use, in both languages, and review them like code.',
'Lab 4.4’s exercise: find a question that retrieves the wrong metric, then improve the description and re-embed. Before, “punctual” and “couriers” pull toward delivered_orders; after adding synonyms in English and Arabic, sla_rate wins with 0.533. Note what changed: one line in the semantic layer, reviewed in a pull request, not a rule in a prompt.',['lab']);

const STEPS=[['Question','Arabic or English'],['Retrieve','top 3 metrics'],['Plan','JSON, not SQL'],['Validate','known names, safe ops'],['Compile, run','parameterized SQL']];
add('05 / Plan','The model chooses; the platform executes','Five steps from a question to a number. Only one of them is the model.',
svg(STEPS.slice(1).map((_,i)=>path(`M${i*292+262} 90 L${i*292+288} 90`,'em',`w-s${i}`)).join(''))+STEPS.map(([t,d],i)=>box(t,`<p>${d}</p>`,i*292,30,262,i===2?'gold':'silver','s'+i,i===2?'LLM or rules':'')).join('')+
 note('The model picks from names you defined. Everything that touches data is code you wrote and tested.',0,300,'teal',1440),
'<strong>Never let the model write SQL.</strong> It writes a plan; the platform checks it and runs it.',
'Lab 4.4 task 4. The planner turns the question into a semantic query (JSON). With an LLM, the prompt holds only the retrieved metrics and the dimensions; without one, a rule-based planner does the same for simple questions, and that is what runs in class. validate() then checks every name and operator, and compile_query writes parameterized SQL. This is how Snowflake Cortex Analyst, Databricks Genie and the dbt Semantic Layer’s AI integrations work.',['lab']);

add('05 / Plan','The question, answered','The Arabic question from chapter 01, through all five steps.',
question(0,0,760)+svg(path('M380 90 L380 125','em','w-p'))+code(['{"metrics":    ["avg_basket_sar"],',' "dimensions": [],',' "filters": [["city", "in", ["Jeddah"]],','             ["order_date", "=", "2026-09-23"]]}'],0,135,760,'plan')+
 label('<b>148.59 SAR</b>average basket · 377 delivered orders',860,20,'answer',560,'ans')+label('<b>160.11 SAR</b>Jeddah over all of September',860,140,'answer',560,'sep')+manager(1240,280,180,''),
'<strong>Same definition as the dashboard:</strong> delivered items, no fees, by Riyadh date. The model only picked the names.',
'Lab 4.4’s real output. The planner chose avg_basket_sar, filtered on city and order_date, and asked for no dimensions. The 433 orders in Jeddah that day include 56 that weren’t delivered; the metric counts only the 377 delivered. National Day baskets were smaller than September’s average: 224 of the 377 used a promo code, and baskets were smaller with or without one (147.13 and 150.72). Every assumption the model would have guessed in chapter 01 is fixed by the semantic layer.',['lab']);

const BADPLAN=[['dimensions: ["national_id"]','unknown dimension ‘national_id’','D4_3: Secret, never a dimension'],['filters: [["city", "; DROP", "x"]]','operator ‘; DROP’ not allowed','an injection attempt'],['metrics: ["salary"]','unknown metric ‘salary’','not in the layer: no guessing'],['metrics: []','ask for 1 to 3 metrics','an empty or greedy plan']];
add('05 / Guardrail','Validate every plan before it touches data','Lab 4.4’s validate() on four bad plans: whatever wrote them, a model or a person.',
tbl('bad',0,0,1440,'validate(plan)<span class="src">Lab 4.4</span>','420px 420px 1fr',[H('plan','blocked: why','what it was'),...BADPLAN.map(([p,w,d],i)=>({c:[p,BAD(w),SANS(d)],cls:i===0?'unk':''}))],'mono')+
 label('<b>4 of 4</b>blocked before the database',0,250,'answer',360,'n-blk')+note('Known metrics and dimensions, allowed operators, 1–3 metrics, a limit of at most 1,000.',420,270,'teal',1000),
'<strong>Guardrail:</strong> an allow-list check on the plan. Personal data can’t be asked for, because it was never a name.',
'The lab’s four bad plans, all rejected with a ValueError. national_id isn’t a dimension because D4_3 classified it Secret and the gold layer never carried it: the safest column is one the assistant can’t name. The operator check blocks injection through the plan itself; the metric check stops the model inventing salary; the count check stops empty and greedy plans. Role-based access belongs underneath too: the assistant should query as the user, through D4_3’s views.',['lab','d43']);

const EVALS=[['What was the revenue by city in September?','✓'],['Daily orders in Riyadh','✗ retrieved revenue_sar'],[AR('كم متوسط قيمة السلة في جدة يوم اليوم الوطني؟'),'✓'],['Are we delivering on time, by city?','✓'],['How many orders were refunded in Dammam?','✓ but a rate, not a count']];
add('06 / Evals','Measure the planner like any other code','Five questions with the plan you expect. Exact match on metrics, dimensions and filters.',
tbl('evals',0,0,1100,'eval set<span class="src">Lab 4.4 ✱</span>','1fr 330px',[H('question','result'),...EVALS.map(([q,r])=>({c:[SANS(q),r.startsWith('✗')?BAD(r):SANS(r)],cls:r.startsWith('✗')?'unk':''}))],'mono')+
 label('<b>80%</b>planner accuracy',1160,20,'answer',280,'acc')+note('Read the failures, not only the score.',1160,140,'teal',280),
'<strong>Eval:</strong> a fixed question set with expected answers, re-run on every change to prompts, rules or descriptions.',
'The stretch task’s real result: 4 of 5. “Daily orders in Riyadh” is short, so retrieval picks revenue_sar instead of orders. The refund question passes, but the user asked for a count and gets a rate: the eval should catch that too. Fix gaps in the semantic layer (a refunded_orders metric, sharper descriptions) rather than piling rules into the prompt, then re-run the evals.',['lab']);

add('06 / Evals','Before 500 store managers use it','80% on five questions is a start. Ship it like a pipeline, not like a demo.',
[['More evals','real questions from managers, in both languages, with expected plans'],['Log every plan','question, plan, SQL, rows returned: the lineage of an answer'],['Fill the gaps','refunded_orders, daily orders: metrics people ask for'],['Access by role','a Jeddah manager sees Jeddah, through D4_3’s views'],['No personal data out','redact free text; the LLM endpoint stays in an approved region'],['Show the definition','every answer says which metric and filters it used']].map(([t,d],i)=>label(`<b>${t}</b> · ${d}`,i<3?0:740,(i%3)*100,'check',700,`r${i}`)).join(''),
'<strong>The assistant is a data product:</strong> tested, logged, access-controlled and owned.',
'This answers the lab’s fourth checkpoint question. Each item is something the week already built: evals are tests (D2_4), logging is lineage (D4_2), access by role and redaction are D4_3, and a missing metric is a semantic-layer change with a pull request. Showing the plan beside the answer lets a manager spot a wrong filter before acting on the number.',['d43']);

add('07 / The tools','The same ideas, ready-made','Everything on today’s board exists as a product or a built-in.',
'<table class="mapping compact"><thead><tr><th>On the board</th><th>In production</th></tr></thead><tbody><tr><td>metrics defined once</td><td>dbt Semantic Layer (MetricFlow) · Cube · LookML · Snowflake semantic views</td></tr><tr><td>embeddings</td><td>model2vec · sentence-transformers · hosted embedding APIs in an approved region</td></tr><tr><td>vectors and search</td><td>DuckDB <code>array_cosine_similarity</code> and <code>vss</code> · pgvector · LanceDB · Qdrant · OpenSearch</td></tr><tr><td>question → plan → SQL</td><td>Snowflake Cortex Analyst · Databricks Genie · semantic-layer APIs over MCP</td></tr><tr><td>guardrails and evals</td><td>allow-lists in code · eval sets in CI · logged plans</td></tr></tbody></table>',
'<strong>You built each piece by hand in the lab;</strong> in production you configure them.',
'Map each row to its board. The products differ in syntax, not in idea: a metric is an expression with a description, a semantic query names metrics and dimensions, and the engine compiles SQL. MCP (Model Context Protocol) is how many assistants now reach tools such as a semantic layer: the same pattern, the model chooses and the platform executes.',['dbtsl','vss','model2vec']);

const WORDS=[['Semantic layer','definitions stored once, compiled to SQL'],['Metric','an aggregate with one meaning'],['Dimension','what you group and filter by'],['Semantic query','metrics, dimensions, filters as data'],['Parameter','a value passed beside the SQL'],['Embedding','a vector for a text'],['Cosine similarity','how close two vectors point'],['Vector index','fast search over many vectors'],['Retrieval','the few definitions that matter'],['Planner','question in, semantic query out'],['Guardrail','an allow-list check on the plan'],['Eval','questions with expected answers']];
add('08 / The words','Twelve words for serving AI','Every one appeared on a board today. Lab 4.4 uses them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>Define it once, search by meaning, and let the model choose while the platform executes.</strong>',
'A reference board: point at each word and ask the room which board it appeared on, and what it meant for the Jeddah question.');

const RECALL=[
 ['Riyadh’s September revenue is 1.75, 1.84 or 1.98 million. Which is right?','Finance’s: delivered items, no fees (1,748,564.50). Define it once in the semantic layer, and every tool asks for that metric.'],
 ['Why does compile_query put filter values in params, not in the SQL?','So a value is never code: “Riyadh’; DROP TABLE x; --” stays a string to compare.'],
 ['A product’s description changes. What must the pipeline do?','Re-embed that row, with the same model, and keep the model’s name next to the vector.'],
 ['“How punctual are the couriers?” retrieves the wrong metric. Where is the fix?','In sla_rate’s description: add the words people use, in both languages, then re-embed.'],
 ['Name two checks before running a plan a model wrote.','Known metrics and dimensions only (no national_id), allowed operators, 1–3 metrics, a capped limit.']];
add('09 / Recall','Your turn: answer before you look','Five questions on the Jeddah question and the platform that answers it. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer. The next slide fills in all five answers in place.');

add('09 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>One definition, vectors in sync, and plans the platform checks:</strong> the whole session in one line.',
'Go back to the board for any answer the room missed: three revenues (5), compile and parameters (8–9), the embedding pipeline (12), retrieval (13–14), the guardrail (17).');

add('Lab 4.4 · 25 min','Data for AI','A YAML semantic layer, bilingual product search in DuckDB, and questions answered without the model writing SQL.',
`<div class="references"><div><h3>Lab 4.4 · 25 minutes</h3><p>1 · Define metrics once; compile them to SQL · 7′</p><p>2 · Bilingual product search with embeddings · 6′</p><p>3 · Retrieve the right metric for a question · 4′</p><p>4 · Question → validated plan → answer · 6′</p><p class="note">✱ Evaluate the planner on a fixed question set and read the failures. An LLM endpoint in labs/.env is optional.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D4_4_data_for_ai.ipynb</a><a href="${sources.dbtsl}" target="_blank" rel="noopener">Reference · the dbt Semantic Layer</a><a href="${sources.vss}" target="_blank" rel="noopener">Reference · DuckDB vector similarity search</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next: the capstone. Tomorrow you build the whole map yourselves.',
'Hand over to the lab. A quick first check after task 1: September revenue by city is Riyadh 1,748,564.50, Jeddah 1,062,633.91, Dammam 600,239.76. After task 4, the Arabic question must answer 148.59, and all four bad plans must be blocked.',['lab']);

add('10 / Capstone kickoff','Tomorrow, the whole map is yours','Teams of 2–3 build a working platform, and show which part covers every stage and band.',
platform(),
'<strong>The map is the checklist:</strong> for every stage, the part of your platform that covers it, the tool, and why.',
'About 10 minutes after the lab. Every piece of this map was one session this week; the capstone puts them together. Teams of 2–3 build a working platform and present it on Day 5: a 10-minute talk with a live demo, then 5 minutes of technical Q&A. Judges ask about decisions: why this tool, what the grain is, what happens on a re-run, where personal data goes.',['capstone']);

const OPTIONS=[['1 · Qafila Lakehouse','batch and streaming, raw sources to governed marts','everything; pipeline.py is a start'],['2 · Open Data Platform','Saudi open data, tests, a semantic layer, a dashboard','Labs 1.3, 2.3, 2.4, 3.1, 4.2, 4.4'],['3 · Real-time Ops Monitor','courier events, SLA alerts, Iceberg history, replay','Labs 3.3, 3.4, 4.1, 4.2']];
const NEED=[['Two source types','to analytics-ready tables, with one command'],['Layers and grain','raw → staging → marts, each fact’s grain written'],['Incremental','safe to re-run, shown live'],['8+ tests or contract checks','one catches a real problem'],['Orchestrate, govern','a scheduler, and a note on each personal column']];
add('10 / Capstone kickoff','Three options, five requirements','Pick an option tonight. Every option must meet all five.',
OPTIONS.map(([t,d,b],i)=>box(t,`<p>${d}</p>`,i*490,0,460,i===0?'gold':'silver','o'+i,b)).join('')+
 NEED.map(([t,d],i)=>label(`<b>${t}</b> · ${d}`,i<3?0:740,270+(i%3)*80,'check',700,`n${i}`)).join(''),
'<strong>Scope small, finish end to end:</strong> a thin platform that runs beats a wide one that doesn’t.',
'From labs/capstone/README.md. Option 1 starts from capstone/pipeline.py, a working Dagster code location: 9 ingestion assets, the dbt project with every test as an asset check, a gold table in Iceberg, the payments contract and a volume monitor. Its known weaknesses are good first tasks: full refreshes, no streaming path, contract breaches that only warn, unprotected personal data in raw. Rubric: pipeline 30%, architecture and modeling 25%, quality and observability 20%, orchestration and governance 15%, presentation 10%.',['capstone']);

add('Capstone · tonight','Before you leave today','Ten minutes now, then get the baseline running tonight.',
`<div class="references"><div><h3>Tonight</h3><p>1 · Confirm your option, sources and scope with the instructor</p><p>2 · Copy TEMPLATE_README.md into your team repository</p><p>3 · Run capstone/pipeline.py end to end (1–2 minutes)</p><p>4 · Open the Dagster UI and find every asset check</p><p class="note">✱ Personal data stays out of git, out of slides and out of external LLM APIs (Lab 4.3).</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.capstone}" target="_blank" rel="noopener">Capstone · options, requirements, rubric</a><a href="${sources.template}" target="_blank" rel="noopener">Template · your team’s README</a><a href="${sources.brief}">Slides · the capstone day</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Submit the repository link before Day 5, Session 1.',
'Day 5 runs in three build blocks before the presentations: sources landing in raw with one command; models with documented grain, incremental loads, tests and contract checks; then orchestration, the governance note, the README, 6–8 slides, one timed rehearsal and a recorded backup of the demo. The capstone deck has the timer and the break slides for the day.',['capstone','template']);

Story.start({brand:'QAFILA / DATA FOR AI',sources});
})();
