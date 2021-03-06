const currencylayer = 'cbcc2fe08c78d743101fbe51ad8346bc';
const fs = require('fs');
const dateformat = require('dateformat');
const getJson = require('simple-fetch').getJson;
const AsyncFileReader = require('async-file-reader').AsyncFileReader;
const exec = require('shelljs').exec;
const vwap = require('vwap');
const fileExists = require('file-exists');


let rates = {};

async function readInterval({file, first, start, end, rate, dbg}) {
  let ret = [];
  let after = null;
  rate = 1.0/rate;
  if (end < first[2]) {
    console.log("Inteval End is before first.",end, first);
    return {
     done: false, price: 0, after: first, data:[]
    };
  }
  if (first) ret.push([first[0]*rate,first[1]]);
  let within = false;
  let count = 0;
  do {
    line = await file.readLine();
    if (line != null) {
      parts = line.split(',')
      let [time, price, vol] = parts;
      time = time *1;
      price= price *1;
      vol = vol * 1;
      within = (time <= end && time >= start);
      if (within) ret.push([vol,price*rate]); else after=[vol,price,time];
      count++;
      if (!within) console.log("not within, start is",start, new Date(start*1000)," end is",new Date(end*1000), 'time is',new Date(time*1000));
      if (dbg) {
        console.log(new Date(time*1000), new Date(end*1000));

      }
      //if (dbg) console.log("inside interval line", line);
    }
  } while (line != null && within);
  let done = (line == null);
  //if (dbg) console.log(count);
  return { price: vwap(ret), done, after, data: ret };
}

async function getLineCount(filename) {
  let outp = exec('wc -l '+filename);
  return outp.split(' ')[0] * 1;
}

async function gunzip(filename) {
  exec('gunzip '+filename);
}

async function downloadBTCHist({exchange, currency}) {
  let fname = exchange+currency+'.csv.gz';
  exec('wget -O data/hist/'+fname+' http://api.bitcoincharts.com/v1/csv/'+fname);
}

async function requestTimeframeExchange({currency, start, end}) {
  let base = 'https://apilayer.net/api/timeframe';
  let key = currencylayer;
  start = dateformat(start, 'yyyy-mm-dd');
  end = dateformat(end, 'yyyy-mm-dd');
  let url = `${base}?access_key=${key}&start_date=${start}`;
  url += `&end_date=${end}&currencies=${currency}`;
  let result = await getJson(url);
  console.log(result.quotes);
  rates[currency] = result.quotes;
}

async function getTrades({exchange, currency, end}) {
  let fname = 'data/hist/'+exchange+currency+'.csv';
  let exists = await fileExists(fname);
  let updated = new Date('01-01-1970');

  if (exists) updated = fs.statSync(fname).mtime;

  if (!exists || updated < end) {
    console.log('downloading..');	  
    await downloadBTCHist({exchange, currency});
    await gunzip(fname+'.gz');    
  } else {
    console.log('file found');
  }
}

async function skipToStart({file, start, count}) {
  let line = '', skipped = 0;
  let time = 0;
  let start_ = start.getTime()/1000;
 
  while (line != null && time < start_) {
    line = await file.readLine();
    if (line) {
      let parts = line.split(',');
      time = parts[0] * 1;
      skipped++;
      let perc =( skipped / count * 100.0).toFixed(0)+'%';   
      if (skipped % 100000 == 0) console.log('skipped '+skipped+' lines '+perc);
    }
  }

  console.log("skipped "+skipped+" lines, line time is ", new Date(time*1000));
}

async function setStatus(params, msg) {
  console.log(msg);
}

async function getRate({currency, time}) {
  let dt = dateformat(time, 'yyyy-mm-dd');
  //console.log({currency, time, dt, rates, rr:rates[dt]});
  try {
    return rates[currency][dt]['USD'+currency];
  } catch (e) {
    console.log("getrate error, params = ",{currency,time});
    return 1;
  }
}

async function loadAndProcess(params) {
  let {intervalSeconds, start, end, exchA, exchB, exchAcurr, exchBcurr} = params;
  await setStatus(params, "Processing, 0 lines completed");
 
  let filenameA = 'data/hist/'+exchA+exchAcurr+'.csv';
  let filenameB = 'data/hist/'+exchB+exchBcurr+'.csv';
  let fileA = new AsyncFileReader(filenameA);
  let fileB = new AsyncFileReader(filenameB);
	
  let count = await getLineCount(filenameA);
  await skipToStart({file: fileA, start, count});
  
  count = await getLineCount(filenameB);
  await skipToStart({file: fileB, start, count});

  let st_ = dateformat(start, 'mmddyy');
  let en_ = dateformat(end, 'mmddyy');
  let outfname = exchA+exchAcurr+exchB+exchBcurr+st_+'_'+en_+'.csv';
  let outcsv = fs.createWriteStream('data/downloads/'+outfname);

  let intervalEnd = start.getTime() / 1000;
  let done = false; let cc = 0;
  let lastPerc = 0, lastSpread = -1000;
  let total = (end.getTime()/1000) - (start.getTime()/1000);
  let afterA = false; let afterB = false, curr = new Date('01-01-1970');
  let lastPriceA = 0, lastPriceB = 0;
  do {
    let start_ = intervalEnd;
    intervalEnd += intervalSeconds; 
    curr = new Date(intervalEnd*1000);
    let rateA = await getRate({currency:exchAcurr, time: curr});
    let rateB = await getRate({currency:exchBcurr, time: curr});
    console.log("---------------- read AAAA ------------------"); 
    let {price:priceA, done:doneA, after, data} = await readInterval({file: fileA, start:start_, end: intervalEnd, first:afterA, rate:rateA, dbg:true});
    afterA = after;
    let priceB, doneB;
    console.log("--------------- read BBBB -------------------");
    ({price:priceB, done:doneB, after} = await readInterval({file: fileB, start:start_, end: intervalEnd, first:afterB, rate:rateB}));
    afterB = after;
    done = doneA || doneB || curr >= end;

    if (done) {
      console.log({curr,end,doneA,doneB,priceA,priceB});
    }
    priceA = priceA.toFixed(2)*1.0;
    priceB = priceB.toFixed(2)*1.0;
    if (priceA == 0) priceA = lastPriceA;
    if (priceB == 0) priceB = lastPriceB;
 
    let dt = dateformat(new Date(intervalEnd*1000), 'mm-dd-yyyy hh:MM:ss TT Z', true);
    let spread = ((priceA - priceB) / priceA) * 100.0;
    if (!(priceA == 0 || priceB == 0)) {
      outcsv.write([dt, priceA, priceB, spread.toFixed(2)].join(','));
      outcsv.write('\n');
      lastPriceA = priceA;
      lastPriceB = priceB;
    }
  
    let percentDone =(((total-intervalEnd) / total) * 100.0).toFixed(0) ;
    if (percentDone % 5 == 0 && lastPerc != percentDone) {
      console.log(dateformat(curr,'mm-dd hh:MM:ss TT Z'));
      lastPerc = percentDone;
    }
    cc++;
  } while (!done); // && cc < 2);

  outcsv.close(); 
}


async function histCSV({intervalSeconds, start, end, exchA,exchAcurr,exchB,exchBcurr}) {
  await getTrades({exchange:exchA, currency: exchAcurr, end});
  await getTrades({exchange:exchB, currency: exchBcurr, end});
  
  await requestTimeframeExchange({currency: exchAcurr, start, end});
  await requestTimeframeExchange({currency: exchBcurr, start, end});

  await loadAndProcess({exchA, exchB, exchAcurr, exchBcurr, start, end, intervalSeconds});
}

async function test() {
  //await getTrades({exchange:'bitflyer', currency:'JPY', end:new Date('05-01-2018')});
  //await downloadBTCHist({exchange:'bitflyer', currency:'JPY'});
  //console.log(await getLineCount('data/hist/bitflyerJPY.csv'));

  //let start = new Date('2010-03-01'), end = new Date('2010-04-01'), currency='JPY';
  //let data = await requestTimeframeExchange({start, end, currency});
  //console.log(data);
  let start = new Date('03-01-2018'), end = new Date('04-01-2018');
  //await histCSV({intervalSeconds:60,start, end, exchA:'bitflyer',exchB:'bitstamp',exchAcurr:'JPY',
  //	         exchBcurr:'USD'});
  //await histCSV({intervalSeconds:60,start, end, exchA:'kraken',exchB:'bitstamp',exchAcurr:'USD',
  //	         exchBcurr:'USD'});
  //await histCSV({intervalSeconds:60,start, end, exchA:'btcc',exchB:'bitstamp',exchAcurr:'USD',
  //	         exchBcurr:'USD'});
  await histCSV({intervalSeconds:60,start, end, exchA:'hitbtc',exchB:'bitstamp',exchAcurr:'USD',
  	         exchBcurr:'USD'});
}

test().catch(console.error);

