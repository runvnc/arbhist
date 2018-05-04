const currencylayer = 'cbcc2fe08c78d743101fbe51ad8346bc';

const dateformat = require('dateformat');
const getJson = require('simple-fetch').getJson;
const AsyncFileReader = require('async-file-reader').AsyncFileReader;
const exec = require('shelljs').exec;
const vwap = require('vwap');
const fileExists = require('file-exists');


async function readInterval({file, end}) {
  let ret = [];
  let within = false;
  do {
    line = await file.readLine();
    if (line != null) {
      parts = line.split(',')
      ([time, price, vol] = parts);
      within = (time <= end);
      if (within) ret.push([price, vol]);
    }
  } while (line != null && within);
  let done = (line == null);
  return { price: vwap(ret), done };
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
  return result.quotes;
}

async function getTrades({exchange, currency, end}) {
  let fname = 'data/hist/'+exchange+currency+'.csv';
  let exists = await fileExists(fname);
  let updated = new Date('01-01-1970');

  if (exists)
  if (!exists) {
    await downloadBTCHist({exchange, currency});
    
  }
}

async function skipToStart({file, start}) {
  let line = '';
  let time = 0;
  let start_ = start.getTime()/1000;
  while (line != null && time < start_) {
    line = await file.readLine();
    let parts = line.split(',');
    time = parts[0] * 1;
  }
}

async function loadAndProcess(params) {
  let {intervalSeconds, start, end, exchA, exchB, exchAcurr, exchBcurr} = params;
  await setStatus(params, "Processing, 0 lines completed");
 
  let filenameA = 'data/hist/'+exchA+exchAcurr+'.csv';
  let filenameB = 'data/hist/'+exchB+exchBcurr+'.csv';
  let fileA = new AsyncFileReader(filenameA);
  let fileB = new AsyncFileReader(filenameB);
	
  //let lines = await getLineCount(filenameA);

  await skipToStart({file: fileA, start});
  await skipToStart({file: fileB, start});

  let outfname = exchA+exchAcurr+exchB+exchBcurr+start+'_'+end+'.csv';
  let outcsv = fs.createWriteStream('data/downloads/'+outfname);

  let intervalEnd = start.getTime() / 1000;
  let done = false;
  let total = (end.getTime()/1000) - (start.getTime()/1000);

  do {
    intervalEnd += intervalSeconds;      
    let {price:priceA, done:doneA} = await readInterval({file: fileA, end: intervalEnd});
    let {price:priceB, done:doneB} = await readInterval({file: fileB, end: intervalEnd});
    done = doneA || doneB;

    let dt = dateformat(new Date(intervalEnd*1000), 'yyyy-mm-dd hh:mm:ss a', true);
    let spread = ((priceA - priceB) / priceA) * 100.0;
    outcsv.write([dt, priceA, priceB, spread.toFixed(2)].join(','));
    outcsv.write('\n');
    let percentDone =((intervalEnd / total) * 100.0).toFixed(0) + '%';
    if (percentDone % 5 == 0) console.log(percentDone);
  } while (!done);

  outcsv.close();
}


async function histCSV({intervalSeconds, start, end, exchA,exchAcurr,exchB,exchBcurr}) {
  await getTrades({exchange:exchA, currency: exchAcurr, end});
  await getTrades({exchange:exchB, currency: exchBcurr, end});
  
  let ratesA = await requestTimeframeExchange({currency: exchAcurr, start, end});
  let ratesB = await requestTimeframeExchange({currency: exchBcurr, start, end});

  await loadAndProcess({exhA, exchB, exchAcurr, exchBcurr, start, end, intervalSeconds});
}

async function test() {
  await downloadBTCHist({exchange:'bitflyer', currency:'JPY'});
  console.log(await getLineCount('data/hist/bitflyerJPY.csv'));

  let start = new Date('2010-03-01'), end = new Date('2010-04-01'), currency='JPY';
  let data = await requestTimeframeExchange({start, end, currency});
  console.log(data);
}

test().catch(console.error);

