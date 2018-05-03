const currencylayer = 'cbcc2fe08c78d743101fbe51ad8346bc';
const dateformat = require('dateformat');
const getJson = require('simple-fetch').getJson;

const exec = require('shelljs').exec;


async function readSameTime(file) {
        
}

async function getLineCount(filename) {
  let outp = exec('wc -l '+filename);
  return outp.split(' ')[0] * 1;
}

async function gunzip(filename) {
  exec('gunzip '+filename);
}

async function downloadBTCHist({exchange, currency}) {
  exec('wget http://api.bitcoincharts.com/v1/csv/'+exchange+currency+'.csv.gz');
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

async function getHistory({exchange, currency, end}) {

}

async function loadAndProcess(params) {
  let {start, end, exchA, exchB, exchAcurr, exchBcurr} = params;
  await setStatus(params, "Processing, 0 lines completed");
  
  let lines = await getLineCount(fileA);

	// iterate over file `${exchA}${exchAcurr}.csv`
  // read collection of lines
  // with same timestamp
	// if timesstamp is not between start and end
	// ignore
	// do
  //	
  // loop until different timestamp	
  
  // do same with other file exchB exchBcurr
  
  // for both exchanges/timestamp calculate vwap
  // calculate spread
  // write to csv file
  
}


async function histCSV({start, end, exchA,exchAcurr,exchB,exchBcurr}) {
  await getTrades({exchange:exchA, currency: exchAcurr, end});
  await getTrades({exchange:exchB, currency: exchBcurr, end});
  
  let ratesA = await requestTimeframeExchange({currency: exchAcurr, start, end});
  let ratesB = await requestTimeframeExchange({currency: exchBcurr, start, end});

  await loadAndProcess({exhA, exchB, exchAcurr, exchBcurr, start, end});
}

async function test() {
  console.log(await getLineCount('data/hist/bitflyerJPY.csv'));

  let start = new Date('2010-03-01'), end = new Date('2010-04-01'), currency='JPY';
  let data = await requestTimeframeExchange({start, end, currency});
  console.log(data);
}

test().catch(console.error);

