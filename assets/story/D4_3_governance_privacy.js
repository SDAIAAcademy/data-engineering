/* D4_3 Governance, security and privacy, told on one evolving board (engine: story.js), one idea per board.
   The running example is Khalid, customer 107371 (00_intro.js, D1_4): his row holds a name, an email, a mobile and a
   national ID, and copies of it travel through every layer. Compliance's question from the introduction drives the deck:
   who may see Khalid's national ID? The erasure chapter calls back customer 100710, erased in the app on 13 September (D1_4).
   Hard ideas are built step by step: neighbouring boards share a heading, and each click adds one piece.
   Every value is real course data (data/raw/qafila_oltp.sqlite: synthetic people, random IDs with valid check digits) and
   the numbers Lab 4.3 prints; Khalid's token is HMAC-SHA256 with the lab's default key. Engineering view only, not legal advice. */
(() => {
'use strict';
const {art,label,note,path,outline,svg,box,rec,tbl,recall,platform,add}=Story;
const colab=p=>`https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/${p}`;
const sources={pdpl:'https://bigid.com/blog/ndmo-framework-for-pdpl-compliance/',ndmo:'https://uniqus.com/a-comprehensive-overview-of-ndmo-compliance/',iceberg:'https://iceberg.apache.org/docs/latest/maintenance/',presidio:'https://microsoft.github.io/presidio/',intro:'00_intro.html',d14:'D1_4_cdc_scd.html',d22:'D2_2_modeling.html',lab:colab('D4_3_governance_privacy.ipynb'),next:'D4_4_data_for_ai.html'};
const H=(...c)=>({c,cls:'h'}),SANS=v=>`<span style="font-family:var(--sans)">${v}</span>`,BAD=v=>`<em class="k bad">${v}</em>`,K=v=>`<em class="k">${v}</em>`;
const code=(lines,x,y,w,id)=>label(lines.map(l=>l.replace(/^ +/,s=>'&nbsp;'.repeat(s.length))).join('<br>'),x,y,'code',w,id);

// Khalid's row in raw.customers, column by column.
const NAME='<span dir="rtl">خالد التميمي</span>';
const ROW=[['customer_id','107371'],['full_name',NAME],['email','khalid.48@hotmail.com'],['mobile','966530730536'],['national_id','1278681299'],['iban','null'],['city','Riyadh'],['district','Al Malqa']];
// Two of Lab 4.3's 400 generated support notes (notes 1 and 5), and note 1 after the stretch task's redact(); the lab's counts.
const NOTE1='Refund to SA7115564106233328540257 requested; identity checked against ID 1986655445.',NOTE2='Customer called from +966 56 474 3974 about a late order.';
const REDACTED1='Refund to [IBAN] requested; identity checked against ID [NATIONAL_ID].',CANDIDATES=83,VALIDIDS=77,ORPHANS=1;
// The illustrative Qafila policy of Lab 4.3: the PII each column holds, and the level that follows.
const LEVEL={customer_id:'Restricted',full_name:'Restricted',email:'Restricted',mobile:'Restricted',national_id:'Secret',iban:'Secret',city:'Restricted',district:'Restricted'};
const WHY={customer_id:'by association',full_name:'name',email:'email',mobile:'mobile',national_id:'national ID',iban:'IBAN',city:'by association',district:'by association'};
const chip=l=>l==='Secret'?BAD(l):l==='Restricted'?`<em class="o">${l}</em>`:K(l);
const khalid=(x=0,y=0,w=200,detail='customer 107371')=>art('customer',x,y,w,'Khalid',detail,'f-cust');
const compliance=(x=1240,y=0,w=200,detail='')=>art('steward',x,y,w,'Compliance',detail,'f-cmp');

add('Day 4 · Session 3','Governance and privacy','',
svg(path('M215 120 C300 130 380 130 470 120','em'))+khalid(0,20,200,'')+compliance(480,20,200,'')+rec('nid',120,330,420,'national_id','Khalid’s · 10 digits','1278681299','wrong'),
'Find personal data, classify it, protect it per role, and erase it for real when asked.',
'About 25 minutes for the board, then Lab 4.3 (25 minutes). The introduction ended with five teams and their questions; Compliance’s was “who may see Khalid’s national ID?”. Today we answer it with pipeline features: discovery, classification, pseudonymization, masked views and erasure. Khalid is customer 107371; his national ID, mobile and email are synthetic (valid check digits, random numbers), like all the course data. This is the engineering half of compliance: the classification levels and retention rules come from the organization’s data protection officer, not from the data team. Arrow keys navigate, N opens notes, O the overview, F fullscreen.',['intro'],
{cover:'<p class="eyebrow">MODERN DATA ENGINEERING · DAY 4</p><h1>Governance<br><span class="teal">and privacy</span></h1><p class="lead">Khalid’s national ID: who may see it,<br>and how to forget it.</p>'});

add('00 / You are here','Rules that apply to every stage','Personal data enters at the sources and is copied into every layer after them.',
platform(n=>n==='Govern'?'fresh':'met',{Govern:'Who may see what, and for how long'}),
'<strong>Governance is a platform feature:</strong> one model can’t protect data that every layer copies.',
'The Govern band runs under the whole map, like Observe in D4_2. Khalid’s row entered through the app database, travelled in the CDC feed (D1_4), landed in raw (D1_3) and staging (D2_3). Each copy needs the same rules. That is why masking, access and erasure are built into the platform, not into one dbt model.',['d14']);

const QT='Compliance asks: who may see Khalid’s national ID?',QS='His row in raw.customers, exactly as the app stores it.';
const khalidRow=(mark=()=>'',w=780,level=false)=>tbl('k-row',0,0,w,'customer 107371<span class="src">raw.customers</span>',level?'190px 1fr 150px':'190px 1fr',[level?H('column','value','level'):H('column','value'),...ROW.map(([c,v])=>({c:level?[c,v,chip(LEVEL[c])]:[c,v],cls:mark(c)}))],'mono');
add('01 / The question',QT,QS,
khalidRow()+khalid(900,20,200,'signed up 26 September')+compliance(1200,20,200,'“Who may see it?”'),
'<strong>Personal data:</strong> anything that identifies a person. Khalid’s row has five columns of it.',
'Khalid signed up on 26 September (D1_4 met him as the CDC feed’s new customer) and placed order 5043883 two days later. The row has a name, an email, a mobile and a national ID; his IBAN is empty (2,652 of 7,442 customers saved one for refunds). City and district identify nobody on their own, but next to a name they do. The question is about people and purposes: support may need his mobile, Finance needs none of it, and an analyst counting customers needs to tell people apart without knowing who they are.',['intro','d14']);

const COPIES=[['App database','customers',0],['CDC feed','the c event, with after',240],['raw.customers','Lab 1.3',480],['stg_customers','dbt · all of it',720],['SCD2 history','Lab 1.4 · every version',960],['snapshots','Iceberg · old versions',1200]];
add('01 / The question','Khalid’s ID is in six places already','Each layer copied the whole row. Backups and exports would add more.',
svg(COPIES.slice(1).map(([,,x],i)=>path(`M${x-36} 70 L${x-4} 70`,'em',`w-cp${i}`)).join(''))+COPIES.map(([t,d,x],i)=>rec(`cp${i}`,x,30,200,t,d,'','wrong')).join('')+
 box('dim_customers','<p>city, district, signup date<br>no name, mobile or ID</p>',500,180,440,'gold','dim','D2_2 left the identifiers out')+
 note('The gold layer kept them out by design. Every layer before it did not.',0,440,'teal',1440),
'<strong>Copies multiply.</strong> Every place that holds the ID must protect it, and must forget it on request.',
'Trace one value: 1278681299 entered in the app, travelled in the CDC feed’s after image (D1_4 warned about this), landed in raw.customers (Lab 1.3) and stg_customers (dbt keeps it there, for the person key), and in any SCD2 history built from the feed. If the customers table lives in Iceberg, old snapshots hold it too. The dimension in gold (D2_2, D2_3) deliberately has no identifiers: that is data minimization by design. Ask the room which of these copies an analyst can read today.',['d14','d22']);

add('02 / The rules','PDPL and NDMO, from a data engineer’s seat','The Personal Data Protection Law: in force since September 2023, fully enforced since September 2024. SDAIA is the regulator.',
'<table class="mapping compact"><thead><tr><th>The rule says</th><th>The pipeline does</th></tr></thead><tbody><tr><td>know what personal data you hold</td><td>PII discovery, a catalog with a classification per column</td></tr><tr><td>collect and show only what the purpose needs</td><td>pseudonyms for analysts, masked views per role, gold without identifiers</td></tr><tr><td>honour data subjects’ rights: access, correction, erasure</td><td>a deletion that reaches every copy, verified</td></tr><tr><td>keep transfers outside the Kingdom under control</td><td>storage region, backups and LLM endpoints inside approved locations</td></tr><tr><td>be able to show it</td><td>lineage, access logs, retention jobs</td></tr></tbody></table>',
'<strong>Each principle becomes a feature</strong> you can build, test and show to an auditor.',
'PDPL covers lawful purpose, data minimization, data subjects’ rights (access, correction, destruction), breach notification and conditions on transfers outside the Kingdom. The NDMO data management and personal data protection standards add governance roles, a data catalog, data quality and the classification levels on the next board. This board is the engineering view, not legal advice: your DPO and your organization’s policies decide what applies.',['pdpl','ndmo']);

add('02 / Classify','Four levels: Top Secret, Secret, Restricted, Public','The NDMO classification. Lab 4.3’s illustrative Qafila policy puts Khalid’s columns here.',
khalidRow(c=>LEVEL[c]==='Secret'?'unk':'',780,true)+tbl('levels',860,0,580,'Qafila’s policy<span class="src">Lab 4.3</span>','170px 1fr',[H('level','handling'),[BAD('Secret'),SANS('pseudonymize; raw value only in a vault')],[`<em class="o">Restricted</em>`,SANS('mask for everyone except support')],[K('Public'),SANS('no personal data at all')]],'mono')+
 note('city next to a name identifies someone: Restricted “by association”.',860,250,'teal',580),
'<strong>Classification drives handling:</strong> the level decides who may read a column, and in what form.',
'The four NDMO levels are Top Secret, Secret, Restricted and Public. The national ID (and an IBAN, when present) is Secret; the name, email and mobile are Restricted; and every other column of a table that holds personal data is Restricted too, because combined with the rest it identifies someone. Only tables without personal data can be Public. The policy is Lab 4.3’s example: a real one comes from your organization, and is reviewed like code.',['ndmo','lab']);

add('03 / Discovery','Column names lie','Personal data also hides in free text. Two of Lab 4.3’s 400 support notes:',
rec('note1',0,10,1000,'support_notes · note',NOTE1,'','chg')+rec('note2',0,110,1000,'support_notes · note',NOTE2,'','chg')+
 label('A column called <b>note</b> holds IBANs, national IDs, mobiles and emails.',0,250,'reason',1000,'n-lie')+art('quality',1120,0,250,'Scan the values','','f-q'),
'<strong>Discover by content:</strong> scan the values, not the column names.',
'Lab 4.3 generates 400 support notes from real customers’ data, typed the way agents type: a refund to an IBAN, an identity check against a national ID, a callback number. Columns named notes, ref or extra_1 are where catalogs miss personal data. Discovery tools (Presidio, cloud DLP scanners, catalog classifiers) sample each text column and look for patterns.',['lab','presidio']);

add('03 / Discovery','A regex finds too much','Ten digits starting with 1 or 2 look like a national ID. So do the couriers’ tracking numbers.',
rec('cand',0,10,640,'\\b[12]\\d{9}\\b · in the 400 notes',`${CANDIDATES} candidates`,'','')+rec('valid',0,110,640,'… that pass the Luhn check',`${VALIDIDS} national IDs`,'','copy')+rec('fp',0,210,640,'… that don’t',`${CANDIDATES-VALIDIDS} tracking numbers`,'','wrong')+
 note('A check digit tells a real ID from a random 10-digit number.',760,40,'teal',680)+note('IBANs have one too: ISO 13616, mod 97.',760,170,'',680),
'<strong>A pattern finds candidates; a checksum confirms them.</strong> Fewer false alarms, more trust in the inventory.',
'Real counts from Lab 4.3: the regex finds every 10-digit number that starts with 1 or 2 in the notes, including 10-digit courier tracking numbers. Saudi national IDs and Iqama numbers end with a Luhn check digit, so the validator keeps only the real ones. The same idea confirms an IBAN: SA plus 22 characters, rearranged and taken mod 97, must give 1. Some personal data has no pattern at all (names, addresses): declare it in the catalog instead.',['lab']);

// The Luhn check on Khalid's ID: from the right, double every second digit, subtract 9 above 9, add up.
const NID=[1,2,7,8,6,8,1,2,9,9],LX=i=>300+i*104;
const luhn=(stage)=>NID.map((d,i)=>{const fromRight=9-i,dbl=fromRight%2===1,x=dbl?d*2:d,v=x>9?x-9:x;
 return label(String(d),LX(i),0,'cell'+(dbl&&stage>0?' r':''),90,`ld${i}`)+(stage>0?label(dbl?`×2 = ${x}`:'·',LX(i),60,'cell n'+(dbl?' r':''),90,`lx${i}`):'')+(stage>1?label(String(v),LX(i),120,'cell'+(dbl?' r':''),90,`lv${i}`):'');}).join('')+
 label('national_id',0,0,'cell n',260,'l-h0')+(stage>0?label('every 2nd from the right',0,60,'cell n',290,'l-h1'):'')+(stage>1?label('above 9: subtract 9',0,120,'cell n',290,'l-h2'):'');
const LT='The Luhn check, on Khalid’s ID',LS='From the right: double every second digit, subtract 9 from results above 9, add everything up.';
add('03 / Discovery',LT,LS,luhn(1)+note('1 = a citizen, 2 = a resident (Iqama)',300,200,'',640),
'<strong>Luhn:</strong> a check digit that catches any single wrong digit and most swapped pairs.',
'A build: two boards. Khalid’s ID is 1278681299; the first digit 1 marks a citizen (2 would be a resident’s Iqama). From the right, the 2nd, 4th, 6th, 8th and 10th digits are doubled: 9, 1, 6, 7 and 1 become 18, 2, 12, 14 and 2. In the course data 4,612 accounts have a citizen’s ID and 2,830 a resident’s.',['lab']);

add('03 / Discovery',LT,LS,luhn(2)+label('<b>50</b>a multiple of 10 ✓',300,200,'answer',360,'l-sum')+note('Change any one digit and the total stops being a multiple of 10.',700,210,'teal',740),
'Khalid’s ID passes: <strong>9 + 9 + 2 + 2 + 8 + 3 + 8 + 5 + 2 + 2 = 50.</strong> A random 10-digit number passes 1 time in 10.',
'Above 9, subtract 9: 18 becomes 9, 12 becomes 3, 14 becomes 5. The sum of all ten results is 50, divisible by 10, so it is a well-formed ID. A tracking number such as 1234567890 sums to 43 and fails. Lab 4.3 task 1 writes is_saudi_id and is_saudi_iban; the detectors then use them as validators behind each regex.',['lab']);

const CAT=[['customers','national_id','national ID','Secret'],['customers','iban','IBAN','Secret'],['customers','mobile','mobile','Restricted'],['customers','email','email','Restricted'],['customers','full_name','name (declared)','Restricted'],['customers','city','by association','Restricted'],['support_notes','note','ID, IBAN, mobile, email','Secret']];
add('04 / The catalog','Every column gets a level, written down','Lab 4.3’s catalog: what the scanner found, what was declared, and the level that follows.',
tbl('catalog',0,0,1100,'catalog<span class="src">Lab 4.3 task 2</span>','230px 200px 1fr 170px',[H('table','column','personal data','level'),...CAT.map(([t,c,p,l])=>({c:[t,c,SANS(p),chip(l)],cls:t==='support_notes'?'hit':''}))],'mono')+
 note('Free text inherits its most sensitive content.',1150,240,'teal',290),
'<strong>Classification is code:</strong> tags in the catalog that masking and access policies read.',
'The real catalog of Lab 4.3, shortened. The scanner found the four patterned types; full_name was declared (no pattern finds a name); columns without PII of their own are Restricted by association, because the table holds personal data. The note column is Secret: it contains national IDs, so it takes the highest level among its contents. In a real platform these labels are tags in Unity Catalog, Polaris, DataHub or OpenMetadata, and policies follow the tags.',['lab']);

add('05 / Pseudonymize','Analysts need people, not identities','How many people are customers? Count national IDs, without anyone seeing one.',
label('<b>7,442</b>accounts',0,20,'answer',300,'n-acc')+label('<b>7,384</b>people',340,20,'answer',300,'n-ppl')+label('<b>58</b>people with two accounts',680,20,'answer',400,'n-dup')+
 rec('tok-k',0,200,760,'1278681299','Khalid’s national ID','→ ?','wrong')+note('Replace each ID by a token: the same ID always gives the same token.',820,200,'teal',620),
'<strong>Pseudonymization:</strong> replace an identifier by a token that still joins and counts, but doesn’t say who.',
'Real counts: 7,442 accounts belong to 7,384 distinct national IDs, so 58 people registered twice (Day 2 found them with email casing). Marketing wants new-customer counts, Finance wants first orders per person: both need to tell people apart. A token that is stable per ID keeps every join and count working.',['lab']);

add('05 / Pseudonymize','Replace each ID with its SHA-256 hash. Anonymous?','SHA-256 can’t be reversed. Khalid’s token would be ed064fa5e94644b1ddb6.',
compliance(1140,30,250,'“Is it still personal data?”')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>Yes: a hash can’t be turned back into the ID</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>No: anyone with a list of IDs can hash them and match</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Yes, as long as the column is renamed</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Predict first: what does an attacker with a leaked list of IDs do?</p>',
'Identifiers come from <strong>small, guessable spaces</strong>. Hashing them doesn’t hide them.',
'Allow 45 seconds. B is right. A is the common misconception: nobody needs to reverse the hash; they hash every candidate and compare. National IDs have 10 digits with a check digit, so a laptop can hash all possible IDs in a short time, and a leaked list is faster still. C confuses obscurity with protection. The next board runs the attack.',[],
{quiz:{answer:'B',right:'Yes. Hash every ID on a leaked list and look the results up.',wrong:'Think like an attacker who has a list of real IDs. Try again.'}});

add('05 / Pseudonymize','A plain hash re-identifies everyone','Lab 4.3’s attacker holds 500 leaked national IDs and the “anonymous” hashed table.',
rec('sha',0,10,700,'sha256("1278681299")','anyone can compute this','ed064fa5…','wrong')+rec('hmac',0,120,700,'hmac(key, "1278681299")','only whoever holds the key','6fb6a0bd…','copy')+
 label('<b>500 of 500</b>re-identified from plain hashes',780,10,'answer',660,'n-sha')+label('<b>0 of 500</b>without the key',780,120,'answer',660,'n-hmac')+
 label('<b>58</b>people with two accounts: still found, from tokens alone',0,260,'answer',1100,'n-58')+note('The key lives in a secret manager. Rotate it and every token changes.',0,400,'teal',1440),
'<strong>Keyed hash (HMAC):</strong> a stable token per ID that nobody can recompute without the secret key.',
'Real results from Lab 4.3 task 3: with plain SHA-256 the attacker matches all 500 leaked IDs; with HMAC-SHA256 under a secret key, none. The tokens still find the 58 duplicate people. Khalid’s HMAC token with the lab’s default key starts 6fb6a0bd. Pseudonymized data is still personal data under PDPL: whoever holds the key can match it. It lowers the risk; true anonymization (aggregates, k-anonymity, differential privacy) is a much higher bar.',['lab']);

add('06 / Protect','Four ways to protect a value','Different strengths, and only one makes data stop being personal.',
'<table class="mapping compact"><thead><tr><th>Technique</th><th>Khalid’s value becomes</th><th>Reversible?</th><th>Still personal data?</th></tr></thead><tbody><tr><td>masking</td><td><code>+9665•••••36</code> · <code>k•••@hotmail.com</code></td><td>no, for the viewer</td><td>the base table is</td></tr><tr><td>pseudonymization</td><td><code>6fb6a0bdfde478b9d8aa</code></td><td>with the key</td><td>yes</td></tr><tr><td>anonymization</td><td>“Riyadh · Al Malqa: 438 customers”</td><td>no</td><td>no, if done properly (hard)</td></tr><tr><td>encryption</td><td>ciphertext, at rest and in transit</td><td>with the key</td><td>yes</td></tr></tbody></table>',
'<strong>Pick per audience:</strong> masks for people who need a glimpse, tokens for analysis, aggregates for publication.',
'Khalid’s real values: Lab 4.3’s mask keeps the last two digits of the mobile and the first letter and domain of the email. Encryption protects storage and networks but anyone who can query the decrypted table sees everything, so it doesn’t replace access control. Anonymized aggregates are the only form that stops being personal data, and only if small groups can’t be singled out.',['lab']);

const VIEWS=[['support.customers','support',[['full_name',NAME],['email','khalid.48@hotmail.com'],['mobile','+9665•••••36'],['city','Riyadh']]],['analyst.customers','analysts',[['person_token','6fb6a0bdfde478b9d8aa'],['city','Riyadh'],['district','Al Malqa'],['email_domain','hotmail.com']]]];
add('06 / Protect','Each role reads its own view of Khalid','Nobody reads raw.customers directly. Each role gets a view with only what its job needs.',
VIEWS.map(([v,who,rows],i)=>tbl(`view${i}`,i*740,0,700,`${v}<span class="src">${who}</span>`,'190px 1fr',[H('column','value'),...rows],'mono')).join('')+
 rec('blocked',0,260,1440,'analyst: SELECT national_id FROM main.customers','PermissionError: analyst may not read main','blocked','wrong'),
'<strong>Least privilege:</strong> grants on views, never on the table with the raw identifiers.',
'Lab 4.3 task 4. Support sees the name and email to talk to Khalid, but only the last two digits of his mobile. Analysts see a person token, his city and district and an email domain: enough to count people and segment them, nothing to contact or identify him. The lab’s toy access check refuses any query outside the role’s schema, and its PII scanner finds nothing in the analyst view.',['lab']);

add('06 / Protect','A view is a convention. A policy is enforced.','Views work if every tool uses them. Warehouses attach the rule to the column itself.',
code(['CREATE VIEW analyst.customers AS','SELECT pseudo(national_id) AS person_token,','       customer_id, city, district,','       lower(split_part(email, \'@\', 2))','           AS email_domain,','       created_at','FROM main.customers;'],0,0,700,'v-code')+
 label('<b>Column masks and row filters</b><br>Snowflake, BigQuery, Unity Catalog: the rule applies in every tool',780,0,'reason',660,'pol1')+label('<b>Tag-based policies</b><br>classify once (Secret); the mask follows the tag to every table',780,140,'reason',660,'pol2')+label('<b>Roles, not people</b><br>grants go to support and analyst, and people join roles',780,280,'reason',660,'pol3'),
'<strong>Put the rule where the data is:</strong> a policy on the column or tag can’t be bypassed by a new tool.',
'Lab 4.3’s analyst view, as SQL. DuckDB has no column security, so the lab models it with views and a toy grant check. A warehouse or catalog enforces the same thing natively: a masking policy on national_id, or on the Secret tag, applies whether the query comes from a dashboard, a notebook or an AI assistant. RBAC grants go to roles; people are added to roles.',['lab']);

const ET='Compliance asks: erase customer 100710 for real',ES='D1_4’s customer, deleted in the app on 13 September. Our lakehouse copy is an Iceberg table.';
const snaps=(stage)=>rec('s1',0,30,440,'snapshot 1 · the first load','7,442 customers','','copy')+(stage>0?rec('s2',500,30,440,'snapshot 2 · after the DELETE','7,441 customers','','copy'):'')+
 (stage>0?label('scan() today: <b>0 rows</b> ✓',500,140,'check',440,'q-now')+label('scan(snapshot 1): <b class="warn">1 row</b>',0,140,'check',440,'q-old')+label('a data file on disk: <b class="warn">still holds the ID</b>',0,200,'check',600,'q-file'):'');
add('07 / Erasure',ET,ES,snaps(0)+art('steward',1180,20,220,'Compliance','“Is it gone everywhere?”','f-cmp'),
'The PDPL gives data subjects the right to have their data <strong>destroyed</strong>. The table must forget it.',
'A build: two boards. Lab 4.3 does this for one customer of its own table; the steps and the evidence are the same for 100710. D1_4 already deleted 100710 in the replica, and D2_2 kept the orders with an Unknown customer. Now the lakehouse copy: an Iceberg table written once (snapshot 1). Recall D3_1: every change is a new snapshot, and old snapshots stay for time travel.',['d14','lab']);

add('07 / Erasure',ET,ES,snaps(1)+art('steward',1180,20,220,'Compliance','“Is it gone everywhere?”','f-cmp')+note('Time travel is the feature. Here it is the problem.',0,300,'red',1100),
'<strong>DELETE is step one.</strong> The current table forgets; the history and the files on disk don’t.',
'Real results from Lab 4.3 task 5: after tbl.delete, a scan of the current snapshot finds 0 rows for the customer, a scan of snapshot 1 still returns 1, and one Parquet file under the table’s data folder still contains the national ID. Iceberg’s delete wrote a new file without the row; the old file is kept because snapshot 1 points to it.',['lab','iceberg']);

const STEPS=[['1 · Delete','a new snapshot without the row'],['2 · Expire','drop the old snapshots: no time travel back'],['3 · Remove orphans','delete files no snapshot points to'],['4 · Verify','scan every file: the ID is gone']];
add('07 / Erasure','Four steps, then the proof','The same chores as D3_4’s maintenance, run for a reason.',
svg(STEPS.slice(1).map((_,i)=>path(`M${330+i*370} 90 L${370+i*370} 90`,'em',`w-e${i}`)).join(''))+STEPS.map(([t,d],i)=>box(t,`<p>${d}</p>`,i*370,0,330,i===3?'gold':'silver',`e${i}`)).join('')+
 label('<b>1</b>snapshot left',0,260,'answer',300,'n-snap')+label(`<b>${ORPHANS}</b>orphan file removed`,340,260,'answer',300,'n-orph')+label('<b>0</b>files containing the ID',680,260,'answer',300,'n-files')+label('<b>7,441</b>everyone else, intact',1020,260,'answer',400,'n-rest'),
'<strong>Erasure on a lakehouse:</strong> delete, expire snapshots, remove orphan files, then verify on disk.',
'Lab 4.3’s exercise: expire every snapshot except the current one, then delete each data file that no remaining snapshot references (what Spark’s remove_orphan_files procedure does), and prove it by scanning every Parquet file for the ID. The checks: one snapshot left, no file contains the ID, and the other customers are all still there. In production these are Iceberg procedures run by the platform, not by hand.',['lab','iceberg']);

const MORE=[['Derived tables','dbt rebuilds them; incremental models need a delete step'],['History','SCD2 versions (Lab 1.4 deletes them all)'],['Column statistics','min/max in old manifests can hold a value'],['Backups, replicas','expire them on a schedule you can name'],['Exports','files already sent to partners'],['Logs','keep the request, not the data']];
add('07 / Erasure','Beyond the table: the rest of the checklist','Before you tell 100710 the data is gone, every copy from board 4 needs an answer.',
MORE.map(([t,d],i)=>label(`<b>${t}</b> · ${d}`,i<3?0:740,(i%3)*90,'check',700,`m${i}`)).join('')+
 note('At scale: batch erasures weekly, or crypto-shred: encrypt each person’s data with their own key, and delete the key.',0,320,'teal',1440),
'<strong>An erasure is a pipeline run:</strong> it must reach every layer, and leave a record that it did.',
'From the lab’s closing notes. Old manifest files keep per-column lower and upper bounds, which for a single-row file are the value itself. dbt’s table models drop the row on the next build; incremental models keep it unless a delete step runs. Doing all of this per request is expensive, so many teams batch erasures weekly within the legal deadline, or use crypto-shredding.',['lab']);

add('08 / Before AI','Redact free text before it leaves the platform','Lab 4.3’s stretch task. Tomorrow’s assistant (D4_4) must never see a national ID.',
rec('red0',0,10,1440,'before',NOTE1,'','wrong')+rec('red1',0,120,1440,'after redact(note)',REDACTED1,'','copy')+
 note('The same detectors, used the other way round: find each value, replace it with its type.',0,260,'teal',1440),
'<strong>Redaction:</strong> replace detected personal data with typed placeholders before text reaches a model.',
'redact() runs find_pii on each note and replaces every hit with [NATIONAL_ID], [IBAN], [MOBILE] or [EMAIL]; the check is that the scanner finds nothing afterwards. Next session sends questions and metric descriptions to a language model: that boundary is a transfer, and PDPL’s rules on purpose and on transfers outside the Kingdom apply to it.',['lab','next']);

add('09 / The tools','The same ideas, ready-made','Everything on today’s board exists as a product or a built-in.',
'<table class="mapping compact"><thead><tr><th>On the board</th><th>In production</th></tr></thead><tbody><tr><td>PII discovery with validators</td><td>Microsoft Presidio · cloud DLP scanners · catalog classifiers</td></tr><tr><td>a catalog with levels</td><td>Unity Catalog · Polaris · DataHub · OpenMetadata: tags on columns</td></tr><tr><td>tokens and keys</td><td>HMAC with keys in a secret manager (Vault, a cloud KMS) · tokenization services</td></tr><tr><td>masked views per role</td><td>column masking and row access policies · tag-based policies · RBAC</td></tr><tr><td>erasure on Iceberg</td><td><code>expire_snapshots</code> · <code>remove_orphan_files</code> · crypto-shredding</td></tr></tbody></table>',
'<strong>You built each piece by hand in the lab;</strong> in production you configure them.',
'Map each row to its board. Presidio uses the same recipe as the lab: recognizers (patterns plus checksums) and context words. Catalog tags let one classification drive masking in every engine that reads the catalog. Keys belong in a secret manager, never in a notebook or a repository.',['presidio','iceberg']);

const WORDS=[['Personal data','anything that identifies a person'],['PDPL','Saudi Arabia’s data protection law'],['Classification','Top Secret, Secret, Restricted, Public'],['By association','harmless alone, identifying together'],['Discovery','find personal data by its values'],['Check digit','Luhn, mod 97: real IDs, not lookalikes'],['Pseudonymization','a token per ID, still personal data'],['HMAC','a hash only the key holder can repeat'],['Masking','show part of a value to a role'],['Least privilege','each role reads only its view'],['Erasure','delete, expire, remove orphans, verify'],['Redaction','placeholders before text leaves']];
add('10 / The words','Twelve words for protecting people','Every one appeared on a board today. Lab 4.3 uses them all.',
WORDS.map(([w,d],i)=>label(`<b>${w}</b> · ${d}`,i<6?0:740,(i%6)*84,'check',700,`w${i}`)).join(''),
'<strong>Know where the data is, show each role only what it needs, and be able to forget a person.</strong>',
'A reference board: point at each word and ask the room which board it appeared on, and what it did to Khalid’s row.');

const RECALL=[
 ['dim_customers has no national ID. Why is Khalid’s ID still a problem?','It is in the app, the CDC feed, raw, staging, SCD2 history and old snapshots: every copy needs protection.'],
 ['The regex finds tracking numbers too. What tells a real national ID apart?','The Luhn check digit: Khalid’s digits sum to 50, a multiple of 10. A random number passes 1 in 10.'],
 ['Why is a plain SHA-256 of the national ID not anonymous?','Anyone can hash a list of IDs and match: 500 of 500 re-identified. Use HMAC with a secret key.'],
 ['What does an analyst see of Khalid, and why is that enough?','A person token, Riyadh, Al Malqa, hotmail.com: enough to count and segment people, not to contact them.'],
 ['After DELETE, 100710’s ID is still on disk. What else must run?','Expire old snapshots, remove orphan files, verify every file; then derived tables, backups and exports.']];
add('11 / Recall','Your turn: answer before you look','Five questions on Khalid’s row and on erasing 100710. Answer out loud, or with a neighbour.',
recall(RECALL),
'<strong>Say it without the board:</strong> what you can recall now, you can use in the lab.',
'About 3 minutes. Read each question, give the room 20–30 seconds, and take an answer before moving on. Don’t reveal until people have committed to an answer. The next slide fills in all five answers in place.');

add('11 / Recall','Your turn: the answers','Check yours. If one surprised you, go back to its board.',
recall(RECALL,true),
'<strong>Find it, classify it, tokenize or mask it per role, and erase it everywhere:</strong> the whole session in one line.',
'Go back to the board for any answer the room missed: the six copies (4), the Luhn check (9–10), the plain-hash attack (14), the role views (16), erasure on Iceberg (18–20).');

add('Lab 4.3 · 25 min','Governance, security and privacy','Find Saudi PII by content, classify it, pseudonymize it, mask it per role, and erase a customer for real.',
`<div class="references"><div><h3>Lab 4.3 · 25 minutes</h3><p>1 · PII discovery: validators, not just regexes · 6′</p><p>2 · Classify every column (NDMO levels) · 4′</p><p>3 · Pseudonymize with a keyed hash · 5′</p><p>4 · Masked views per role · 5′</p><p>5 · Right to erasure on Iceberg, verified · 5′</p><p class="note">✱ Redact free text before it reaches an LLM. Check Khalid’s ID with your is_saudi_id: 1278681299 must pass.</p></div><div><h3>Open</h3><div class="reference-list"><a href="${sources.lab}" target="_blank" rel="noopener">Lab · D4_3_governance_privacy.ipynb</a><a href="${sources.pdpl}" target="_blank" rel="noopener">Reading · the NDMO framework and PDPL compliance</a><a href="${sources.iceberg}" target="_blank" rel="noopener">Reference · Iceberg table maintenance</a><a href="../index.html">Back to the course page</a></div></div></div>`,
'Next: data for AI. The assistant answers questions, and never sees Khalid’s ID.',
'Hand over to the lab. A quick first check after task 1: is_saudi_id("1278681299") is True and is_saudi_id("1234567890") is False. After task 5, files_containing must return an empty list.',['lab']);

Story.start({brand:'QAFILA / GOVERNANCE AND PRIVACY',sources});
})();
