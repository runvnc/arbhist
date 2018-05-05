const currencylayer = 'cbcc2fe08c78d743101fbe51ad8346bc';
const fs = require('fs');
const dateformat = require('dateformat');
const getJson = require('simple-fetch').getJson;
const AsyncFileReader = require('async-file-reader').AsyncFileReader;
const exec = require('shelljs').exec;
const vwap = require('vwap');
const fileExists = require('file-exists');


let rates = {};

async function readInterval({file, first, end, rate}) {
  let ret = [];
  let after = null;
  rate = 1.0/rate;
  if (first) ret.push([first[1]*rate,first[0]]);
  let within = false;
  do {
    line = await file.readLine();
    if (line != null) {
      parts = line.split(',')
      let [time, price, vol] = parts;
      time = time *1;
      price= price *1;
      vol = vol * 1;
      within = (time <= end);
      if (within) ret.push([vol,price*rate]); else after=[vol,price];
      //console.log("inside interval line");
    }
  } while (line != null && within);
  let done = (line == null);
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
    let parts = line.split(',');
    time = parts[0] * 1;
    skipped++;
    let perc =( skipped / count * 100.0).toFixed(0)+'%';   
    if (skipped % 100000 == 0) console.log('skipped '+skipped+' lines '+perc);
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
  let done = false;
  let lastPerc = 0;
  let total = (end.getTime()/1000) - (start.getTime()/1000);
  let afterA = false; let afterB = false, curr = new Date('01-01-1970');
  do {
    intervalEnd += intervalSeconds; 
    curr = new Date(intervalEnd*1000);
    let rateA = await getRate({currency:exchAcurr, time: curr});
    let rateB = await getRate({currency:exchBcurr, time: curr});
    
    let {price:priceA, done:doneA, after, data} = await readInterval({file: fileA, end: intervalEnd, first:afterA, rate:rateA});
    afterA = after;
    if (priceA < 100 ) {
       console.log({priceA, done, after, data}, 'fail');
       process.exit();
    }
    let priceB, doneB;
    ({price:priceB, done:doneB, after} = await readInterval({file: fileB, end: intervalEnd, first:afterB, rate:rateB}));
    afterB = after;
    done = doneA || doneB || curr >= end;
    //console.log({priceA, priceB, doneA, doneB, afterA, afterB});
     


    let dt = dateformat(new Date(intervalEnd*1000), 'mm-dd-yyyy hh:MM:ss TT Z', true);
    let spread = ((priceA - priceB) / priceA) * 100.0;
    outcsv.write([dt, priceA, priceB, spread.toFixed(2)].join(','));
    outcsv.write('\n');
    let percentDone =(((total-intervalEnd) / total) * 100.0).toFixed(0) ;
    if (percentDone % 5 == 0 && lastPerc != percentDone) {
      console.log(dateformat(curr,'mm-dd hh:MM:ss TT Z'));
      lastPerc = percentDone;
    }
  } while (!done);

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
  await histCSV({intervalSeconds:60,start, end, exchA:'bitflyer',exchB:'bitstamp',exchAcurr:'JPY',
	         exchBcurr:'USD'});
}

test().catch(console.error);

