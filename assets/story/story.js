/* The story engine. One board evolves: shared data-ids glide between slides, and zoom scenes dive into one element.
   A deck script adds its scenes with Story.add and then calls Story.start (see 00_story.js). */
(() => {
'use strict';
const root=document.getElementById('stage');
if(location.search.includes('check'))document.body.classList.add('checking');
const art=(kind,x,y,w,title='',detail='',id=kind)=>`<figure class="figure" data-id="${id}" style="left:${x}px;top:${y}px;width:${w}px;margin:0"><div class="art ${kind}" aria-hidden="true"></div>${title?`<figcaption>${title}</figcaption>`:''}${detail?`<p class="detail">${detail}</p>`:''}</figure>`;
const label=(text,x,y,cls='',w=300,id='')=>`<div class="label ${cls}" ${id?`data-id="${id}"`:''} style="left:${x}px;top:${y}px;width:${w}px">${text}</div>`;
const note=(text,x,y,cls='',w=320)=>`<p class="annotation ${cls}" style="left:${x}px;top:${y}px;width:${w}px">${text}</p>`;
// Wires and enclosures with the same id reshape between slides; without an id they match only when identical.
const path=(d,cls='',id='')=>`<path class="wire ${cls}"${id?` data-id="${id}"`:''} d="${d}"/>`;
const outline=(x,y,w,h,cls='',id='')=>`<path class="enclosure ${cls}"${id?` data-id="${id}"`:''} d="M${x+11} ${y} L${x+w-6} ${y+2} Q${x+w+2} ${y+3} ${x+w} ${y+13} L${x+w-2} ${y+h-9} Q${x+w-1} ${y+h+2} ${x+w-12} ${y+h} L${x+5} ${y+h-1} Q${x-2} ${y+h} ${x} ${y+h-13} L${x+1} ${y+9} Q${x} ${y-1} ${x+11} ${y}Z"/>`;
let svgCount=0;
const svg=content=>{const id=svgCount++;return `<svg class="wires" viewBox="0 0 1440 510" aria-hidden="true"><defs>${[['normal','#61717c'],['em','#348f8d'],['danger','#c46558']].map(([k,c])=>`<marker id="arr-${id}-${k}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M1 1L9 5L1 9" fill="none" stroke="${c}" stroke-width="1.3"/></marker>`).join('')}</defs>${content.replace(/class="wire([^"]*)"/g,(_,cls)=>`class="wire${cls}" marker-end="url(#arr-${id}-${cls.includes('em')?'em':cls.includes('danger')?'danger':'normal'})"`)}</svg>`;};
const box=(title,body,x,y,w,cls='',id=title,tag='')=>`<div class="stage-box ${cls}" data-id="${id}" style="left:${x}px;top:${y}px;width:${w}px"><h3>${title}</h3>${body}${tag?`<span class="tag">${tag}</span>`:''}</div>`;
// Tool names sit beside the work they perform. Dataset boxes always describe stored data.
const tool=(name,action,x,y,w=250,id=name,cls='')=>label(`<strong>${name}</strong><span>${action}</span>`,x,y,`tool-label ${cls}`,w,id);
// A row card: monospace key, a line of detail, an amount on the right. Cards keep their id, so a row glides between slides.
const rec=(id,x,y,w,head,body='',amount='',cls='')=>label(`<b>${head}</b>${body?`<span>${body}</span>`:''}${amount?`<i>${amount}</i>`:''}`,x,y,`rec ${cls}`,w,id);
// A small table: rows are arrays of cells, or {c:[cells],cls} for a highlighted row.
const tbl=(id,x,y,w,title,cols,rows,cls='')=>label(`<div class="t-title">${title}</div>${rows.map(r=>{const [c,rc]=Array.isArray(r)?[r,'']:[r.c,r.cls];return `<div class="t-row ${rc}" style="grid-template-columns:${cols}">${c.map(v=>`<span>${v}</span>`).join('')}</div>`;}).join('')}`,x,y,`tbl ${cls}`,w,id);
// A recall board: numbered questions with an empty answer beside each. The same items with shown=true fill the answers in place.
const recall=(items,shown=false)=>{const gap=Math.floor(505/items.length);return items.map(([q,a],i)=>label(`<b>${i+1}</b>${q}`,0,i*gap,'recall-q',840,`rq${i}`)+label(shown?a:'?',880,i*gap-6,`recall-a${shown?' shown':''}`,560,`ra${i}`)).join('');};
// The course map, redrawn by stage. The same data as de_viz.platform_map (labs/de_viz.py) and the decks' .pmap (assets/deck/deck.js): change them together.
// mark(name) returns '' (plain), 'met' (faded) or 'fresh' (highlighted); a fresh piece with what[name] shows that instead of its session code.
const MAP_TOOLS=[['dlt','2/3',2,'D1_3'],['DuckDB','3/7',2,'D1_1'],['CDC + MERGE','2/3',3,'D1_4'],['Parquet','3/4',3,'D1_2'],['Polars','4/6',3,'D2_1'],['Semantic layer','6/7',3,'D4_4'],['Streaming','2/3',4,'D3_3'],['Iceberg','3/4',4,'D3_1'],['dbt','4/6',4,'D2_2 · D2_3'],['Embeddings','6/7',4,'D4_4'],['Spark','4/5',5,'D3_2'],['Pandera','5/6',5,'D2_4'],['Contracts','5/6',6,'D4_2']];
const MAP_BANDS=[['Orchestrate','Dagster: assets, schedules, checks',7,'D4_1'],['Observe','freshness, volume, schema, distribution, lineage',8,'D4_2'],['Govern','PDPL, masking, access',9,'D4_3']];
const platform=(mark=()=>'',what={})=>{
 const cls=n=>mark(n)?' '+mark(n):'',tail=(n,code)=>mark(n)==='fresh'&&what[n]?`<span class="what">${what[n]}</span>`:`<small>${code}</small>`;
 return `<div class="platform" data-id="map">${['Sources','Ingest','Store','Transform','Test','Serve'].map((h,i)=>`<div class="h" style="grid-column:${i+1};grid-row:1">${h}</div>`).join('')}${['App database','Payments API','CDC feed','Click events','Files, reviews'].map((s,i)=>`<div class="s" style="grid-column:1;grid-row:${i+2}">${s}</div>`).join('')}${MAP_TOOLS.map(([n,col,row,code])=>`<div class="t${cls(n)}" style="grid-column:${col};grid-row:${row}"><b>${n}</b>${tail(n,code)}</div>`).join('')}${MAP_BANDS.map(([n,text,row,code])=>`<div class="b${cls(n)}" style="grid-column:2/-1;grid-row:${row}"><b>${n}</b><span>${text}</span>${tail(n,code)}</div>`).join('')}</div>`;
};
const scenes=[];
// extra.cover: the cover title's HTML. extra.zoom: {in:id} enters an element of the previous slide; {out:id} returns to an element of this slide.
// extra.quiz: {answer,right,wrong} for a slide with .quiz-option buttons.
const add=(chapter,title,subtitle,body,takeaway,notes,refs=[],extra={})=>scenes.push({chapter,title,subtitle,body,takeaway,notes,refs,...extra});

function start({brand,sources={}}){
 scenes.forEach((s,i)=>{
  const el=document.createElement('section');el.className='scene';el.dataset.slide=String(i+1);el.setAttribute('aria-label',`${i+1}. ${s.title}`);
 const heading=s.cover?`<div class="cover-title">${s.cover}</div>`:`<div class="heading"><h2>${s.title}</h2><p class="subtitle">${s.subtitle}</p></div>`;
  if(s.cover)s.body=s.body.replace('viewBox="0 0 1440 510"','viewBox="0 0 730 485"');
  el.innerHTML=`<header><div class="brand"><img src="../assets/img/academy.svg" alt="SDAIA Academy"><span>${brand}</span></div><span class="chapter">${s.chapter}</span></header>${heading}<div class="board ${s.cover?'cover-board':''}"><div class="camera">${s.body}</div></div><p class="takeaway"><span class="signal" aria-hidden="true"></span><span>${s.takeaway}</span></p><footer><span>SDAIA Academy · Modern Data Engineering</span><span>${String(i+1).padStart(2,'0')} / ${scenes.length}</span></footer>`;root.appendChild(el);
 });
 const els=[...root.querySelectorAll('.scene')],notes=document.getElementById('notes-panel');let index=-1,overview=false;
 const progress=document.createElement('div');progress.id='progress';root.appendChild(progress);
 function fit(){if(!overview)root.style.transform=`translate(-50%,-50%) scale(${Math.min(innerWidth/1600,innerHeight/900)})`;}
 
 // Transitions between neighbouring slides. The old slide stays on top while its pieces fade, glide or zoom away.
 const MOVE=750,ZOOM=1000,EASE='cubic-bezier(.45,0,.2,1)',BOARD_W=1440,BOARD_H=510;
 let settle=null;
 function settleTransition(){if(settle){const done=settle;settle=null;done();}}
 const reducedMotion=()=>document.body.classList.contains('checking')||matchMedia('(prefers-reduced-motion: reduce)').matches;
 const geometry=e=>e.style.left&&e.style.top&&e.style.width?{left:e.style.left,top:e.style.top,width:e.style.width}:null;
 function rectOf(camera,id){
  const e=camera.querySelector(`[data-id="${id}"]`);if(!e)return null;
  if(e instanceof SVGGraphicsElement){const b=e.getBBox();return {x:b.x,y:b.y,w:b.width,h:b.height};}
  return {x:e.offsetLeft,y:e.offsetTop,w:e.offsetWidth,h:e.offsetHeight};
 }
 function morph(ca,cb,{run,hide,fadeOut,fadeIn}){
  const sa=ca.querySelector(':scope>svg.wires'),sb=cb.querySelector(':scope>svg.wires');
  const pathKey=p=>p.dataset.id||p.getAttribute('class')+p.getAttribute('d');
  const oldPaths=new Map(sa?[...sa.querySelectorAll('path.wire,path.enclosure')].map(p=>[pathKey(p),p]):[]);
  if(sb)for(const p of sb.querySelectorAll('path.wire,path.enclosure')){
   const o=oldPaths.get(pathKey(p));
   if(!o){fadeIn(p,MOVE*.55);continue;}
   oldPaths.delete(pathKey(p));hide(o);
   const d0=o.getAttribute('d'),d1=p.getAttribute('d');
   if(d0!==d1)run(p,[{d:`path("${d0}")`},{d:`path("${d1}")`}]);
  }
  oldPaths.forEach(fadeOut);
  const pieces=c=>[...c.children].filter(e=>!e.matches('svg.wires'));
  const olds=pieces(ca),byId=new Map(olds.filter(e=>e.dataset.id).map(e=>[e.dataset.id,e])),byHtml=new Map(olds.map(e=>[e.outerHTML,e])),used=new Set();
  for(const n of pieces(cb)){
   const o=(n.dataset.id&&byId.get(n.dataset.id))||byHtml.get(n.outerHTML);
   if(!o||used.has(o)){fadeIn(n,MOVE*.5);continue;}
   used.add(o);
   if(o.outerHTML===n.outerHTML){hide(o);continue;}
   const g0=geometry(o),g1=geometry(n);
   if(o.className===n.className&&o.innerHTML===n.innerHTML){hide(o);if(g0&&g1)run(n,[g0,g1]);continue;}
   // Same id, new content: both copies travel together while one fades into the other.
   if(g0&&g1){run(o,[{...g0,opacity:1},{...g1,opacity:0}]);run(n,[{...g0,opacity:0},{...g1,opacity:1}]);}
   else run(o,[{opacity:1},{opacity:0}],{duration:500,easing:'ease'});
  }
  olds.forEach(o=>{if(!used.has(o))fadeOut(o);});
 }
 function zoom(outer,inner,id,entering,run){
  const r=rectOf(outer,id);if(!r||!r.w||!r.h)return false;
  const s=Math.min(BOARD_W/r.w,BOARD_H/r.h),cx=r.x+r.w/2,cy=r.y+r.h/2;
  const through=`translate(${BOARD_W/2-cx*s}px,${BOARD_H/2-cy*s}px) scale(${s})`,inside=`translate(${cx-BOARD_W/2/s}px,${cy-BOARD_H/2/s}px) scale(${1/s})`,o={duration:ZOOM};
  if(entering){run(outer,[{transform:'none',opacity:1},{opacity:1,offset:.35},{opacity:0,offset:.6},{transform:through,opacity:0}],o);run(inner,[{transform:inside,opacity:0},{opacity:0,offset:.4},{opacity:1,offset:.75},{transform:'none',opacity:1}],o);}
  else{run(inner,[{transform:'none',opacity:1},{opacity:0,offset:.35},{transform:inside,opacity:0}],o);run(outer,[{transform:through,opacity:0},{opacity:0,offset:.25},{opacity:1,offset:.6},{transform:'none',opacity:1}],o);}
  return true;
 }
 function transition(a,b){
  const A=els[a],B=els[b],anims=[],hidden=[];
  const run=(el,frames,o={})=>anims.push(el.animate(frames,{duration:MOVE,easing:EASE,fill:'both',...o}));
  const hide=el=>{el.style.visibility='hidden';hidden.push(el);};
  const fadeOut=el=>run(el,[{opacity:1},{opacity:0}],{duration:300,easing:'ease'});
  const fadeIn=(el,delay)=>run(el,[{opacity:0},{opacity:1}],{duration:450,delay,easing:'ease'});
  [A,B].forEach(s=>s.querySelectorAll('.new').forEach(e=>e.classList.remove('new')));
  A.classList.add('leaving');
  hide(A.querySelector('header'));hide(A.querySelector('footer'));
  // Text never overlaps: the old words leave before the new ones arrive.
  const ch0=A.querySelector('.chapter'),ch1=B.querySelector('.chapter');if(ch0.innerHTML!==ch1.innerHTML)fadeIn(ch1,200);
  for(const sel of ['.heading','.cover-title','.takeaway']){const o=A.querySelector(sel),n=B.querySelector(sel);if(o&&n&&o.innerHTML===n.innerHTML)hide(o);else{if(o)run(o,[{opacity:1},{opacity:0}],{duration:180,easing:'ease-in'});if(n)fadeIn(n,200);}}
  const ca=A.querySelector('.camera'),cb=B.querySelector('.camera'),boards=[ca.parentElement,cb.parentElement];
  // The later slide declares the zoom; its outer board is the one that holds the target element.
  const z=scenes[Math.max(a,b)].zoom,outerIsOld=z&&(z.in?a<b:a>b);
  if(z){boards.forEach(e=>e.classList.add('clip'));if(!zoom(outerIsOld?ca:cb,outerIsOld?cb:ca,z.in||z.out,outerIsOld,run)){boards.forEach(e=>e.classList.remove('clip'));morph(ca,cb,{run,hide,fadeOut,fadeIn});}}
  else morph(ca,cb,{run,hide,fadeOut,fadeIn});
  const done=()=>{anims.forEach(x=>x.cancel());hidden.forEach(e=>{e.style.visibility='';});A.classList.remove('leaving');boards.forEach(e=>e.classList.remove('clip'));};
  settle=done;
  Promise.all(anims.map(x=>x.finished)).then(()=>{if(settle===done)settleTransition();},()=>{});
 }
 function show(n,update=true){
  settleTransition();
  const previous=index,next=Math.max(0,Math.min(els.length-1,n)),oldIds=new Set(index<0?[]:[...els[index].querySelectorAll('[data-id]')].map(e=>e.dataset.id));index=next;
  els.forEach((e,j)=>{e.classList.toggle('active',j===index);e.setAttribute('aria-hidden',String(j!==index));e.inert=j!==index;});
  if(previous>=0&&Math.abs(next-previous)===1&&!overview&&!reducedMotion())transition(previous,next);
  else els[index].querySelectorAll('[data-id]').forEach(e=>e.classList.toggle('new',!oldIds.has(e.dataset.id)));
  if(update)history.replaceState(null,'',`#${index+1}`);progress.style.width=`${(index+1)/els.length*100}%`;
  document.getElementById('previous').disabled=index===0;document.getElementById('next').disabled=index===els.length-1;
  notes.innerHTML=`<h3>${index+1}. ${scenes[index].title}</h3><p>${scenes[index].notes}</p><p style="font-size:16px;margin-top:18px">${scenes[index].refs.filter(k=>sources[k]).map(k=>`<a href="${sources[k]}" target="_blank" rel="noopener">${k}</a>`).join(' · ')}</p>`;
  document.getElementById('live-status').textContent=`Slide ${index+1} of ${els.length}: ${scenes[index].title}`;
 }
 function toggleNotes(){document.getElementById('notes').setAttribute('aria-expanded',String(notes.classList.toggle('open')));}
 function toggleOverview(){overview=!overview;document.body.classList.toggle('overview',overview);if(overview){root.style.transform='';els.forEach(e=>{e.inert=false;e.setAttribute('aria-hidden','false');});}else{show(index);fit();}}
 function fullscreen(){if(document.fullscreenElement)document.exitFullscreen();else document.documentElement.requestFullscreen().catch(()=>{});}
 document.getElementById('previous').onclick=()=>show(index-1);document.getElementById('next').onclick=()=>show(index+1);document.getElementById('notes').onclick=toggleNotes;document.getElementById('overview').onclick=toggleOverview;document.getElementById('fullscreen').onclick=fullscreen;
 els.forEach((e,i)=>e.addEventListener('click',()=>{if(overview){toggleOverview();show(i);}}));
 document.querySelectorAll('.quiz-option').forEach(b=>b.addEventListener('click',()=>{const p=b.closest('.scene'),q=scenes[p.dataset.slide-1].quiz;p.querySelectorAll('.quiz-option').forEach(x=>x.classList.remove('correct','wrong'));const correct=b.dataset.answer===q.answer;b.classList.add(correct?'correct':'wrong');p.querySelector('.feedback').textContent=correct?q.right:q.wrong;}));
 document.addEventListener('keydown',e=>{if(e.ctrlKey||e.metaKey||e.altKey)return;if(e.target.closest('button')&&['Enter',' '].includes(e.key))return;if(['ArrowRight','ArrowDown','PageDown',' '].includes(e.key)){e.preventDefault();show(index+1);}else if(['ArrowLeft','ArrowUp','PageUp'].includes(e.key)){e.preventDefault();show(index-1);}else if(e.key==='Home'){e.preventDefault();show(0);}else if(e.key==='End'){e.preventDefault();show(els.length-1);}else if(e.key.toLowerCase()==='n')toggleNotes();else if(e.key.toLowerCase()==='o')toggleOverview();else if(e.key.toLowerCase()==='f')fullscreen();else if(e.key==='Escape'){if(overview)toggleOverview();notes.classList.remove('open');document.getElementById('notes').setAttribute('aria-expanded','false');}});
 let touchX=null;document.addEventListener('touchstart',e=>{touchX=e.touches[0].clientX;},{passive:true});document.addEventListener('touchend',e=>{if(touchX!==null){const dx=e.changedTouches[0].clientX-touchX;if(Math.abs(dx)>65)show(index+(dx<0?1:-1));touchX=null;}},{passive:true});
 window.addEventListener('resize',fit);window.addEventListener('hashchange',()=>show((parseInt(location.hash.slice(1),10)||1)-1,false));fit();show((parseInt(location.hash.slice(1),10)||1)-1);
 if(location.search.includes('check')){const initial=index,issues=[];for(let i=0;i<els.length;i++){show(i,false);const r=els[i].getBoundingClientRect();for(const e of els[i].querySelectorAll('.figure,.annotation,.label,.stage-box,.heading,.takeaway,.compare-col,.quiz-option,.feedback,.quiz-instruction,.references,.platform,.platform>div,.mapping,.trap')){const b=e.getBoundingClientRect();if(!b.width||!b.height)continue;if(b.left<r.left-1||b.right>r.right+1||b.top<r.top-1||b.bottom>r.bottom+1)issues.push(`Slide ${i+1}: ${e.className} outside canvas`);if(e.scrollWidth>e.clientWidth+3)issues.push(`Slide ${i+1}: ${e.className} overflow`);}}show(initial,false);window.STORY_LAYOUT_CHECK={slides:els.length,issues};const report=document.createElement('output');report.id='check-report';report.style.cssText='position:fixed;left:8px;bottom:8px;font:12px monospace;z-index:99;background:white;padding:5px';report.textContent=`CHECK ${els.length} slides: ${issues.length?issues.join('; '):'OK'}`;document.body.appendChild(report);}
}
window.Story={art,label,note,path,outline,svg,box,tool,rec,tbl,recall,platform,add,start};
})();
