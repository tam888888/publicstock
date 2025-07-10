import fetch from "node-fetch";
import fs from "fs";
import log4js from "log4js";
import json2csv2 from "json2csv"

import http from "node:http";
import https from "node:https";
import path from "path";

const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL) => _parsedURL.protocol == 'http:' ? httpAgent : httpsAgent;

var logger = log4js.getLogger();

const batchSize = process.env.BATCH || 10;
const LIMIT = process.env.LIMIT || 2000;
console.log("BATCH ", batchSize, "LIMIT ", LIMIT)

log4js.configure({
  appenders: {
    everything: { type: "file", filename: "diem.log" },
  },
  categories: {
    default: { appenders: ["everything"], level: "debug" },
  },
});

(async () => {
  let cop = [{ stock_code: 'VNINDEX', post_to: 'HOSE' }, { stock_code: 'VN30', post_to: 'HOSE' }, { stock_code: 'VN30F1M', post_to: 'HNX' }, { stock_code: 'VN30F2M', post_to: 'HNX' }];
  let data = null;
  // data = fs.readFileSync('cop.json');
  // cop = JSON.parse(data);
  // console.log(cop.length);
  let counter = 0;
  let csv = new json2csv2.Parser(
    {
      fields: ['adjRatio', 'buyCount', 'buyForeignQuantity', 'buyForeignValue', 'buyQuantity', 'currentForeignRoom',
        'date', 'dealVolume', 'priceAverage', 'priceBasic', 'priceClose', 'priceHigh', 'priceLow', 'priceOpen',
        'propTradingNetDealValue', 'propTradingNetPTValue', 'propTradingNetValue', 'putthroughValue', 'putthroughVolume',
        'sellCount', 'sellForeignQuantity', 'sellForeignValue', 'sellQuantity', 'symbol', 'totalValue', 'totalVolume']
    });

  let fet = await fetch("https://bgapidatafeed.vps.com.vn/getlistallstock", {
    "headers": {
      "accept": "application/json, text/plain, */*",
      "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
      "if-none-match": "W/\"5ac92-c+NqjXQ6D2JFKgaxgUoTpIzr5z8\"",
      "sec-ch-ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
      "sec-ch-ua-mobile": "?0",
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-site"
    },
    "referrer": "https://banggia.vps.com.vn/",
    "referrerPolicy": "strict-origin-when-cross-origin",
    "body": null,
    "method": "GET",
    "mode": "cors"
  });
  // fet.then(response => response.json())
  // .then(data => console.log(data));

  let xx = await fet.json();
  console.log("total", xx.length)
  xx = xx.filter(item => item.stock_code && item.stock_code.length < 4);
  console.log("filter", xx.length)
  cop = [...cop, ...xx];

  let date = new Date();

  let dir = "./his/";
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir);
  } else {
    let files = fs.readdirSync(dir);
    for (const file of files) {
      fs.unlinkSync(path.join(dir, file));
    }
  }




  // let req = 0;
  // let res = 0;
  let stat = { req: 0, res: 0 };
  for (let x of cop) {
    x['Code'] = x.stock_code;
    x['Exchange'] = x.post_to;
    // if (x.Code.length < 4) {
    let a = x.Exchange.toUpperCase()
    // if (a == "HNX" || a == "UPCOM"||a == "HOSE") {
    if (true) {
      // setTimeout(() => {
      let z = getPrices(x.Code);
      // req++;
      stat.req++;
      z.then((ret) => {
        counter++;
        console.log(counter, ret.Code, stat);
        let data2 = csv.parse(ret.data);
        fs.writeFile("./his/" + ret.Code + "_" + x.Exchange + "_" + date.getTime() + '_trans.txt', data2 + "\n", function (err) {
          if (err) throw err;
        });
        // res++;
        stat.res++;
        if (stat.res % 10 == 0) {
          console.log(stat)
        }
      })
      // }, 500);
    }
    // console.log(req,res)
    while (stat.req - stat.res >= batchSize) {
      await wait(100)
    }
  }
})();


function wait(ms) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve(0);
    }, ms);
  });
}

async function getTrans(symbol) {
  let a = await fetch("https://api-finance-t19.24hmoney.vn/v1/web/stock/transaction-list-ssi?device_id=web&device_name=INVALID&device_model=Windows+10&network_carrier=INVALID&connection_type=INVALID&os=Chrome&os_version=92.0.4515.131&app_version=INVALID&access_token=INVALID&push_token=INVALID&locale=vi&browser_id=web16693664wxvsjkxelc6e8oe325025&symbol=" + symbol + "&page=1&per_page=2000000", {
    "headers": {
      "accept": "application/json, text/plain, */*",
      "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
      "sec-ch-ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
      "sec-ch-ua-mobile": "?0",
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-site"
    },
    "referrer": "https://24hmoney.vn/",
    "referrerPolicy": "strict-origin-when-cross-origin",
    "body": null,
    "method": "GET",
    "mode": "cors"
  });
  let x = await a.json();
  // console.log(x.data.length,x.status,x.execute_time_ms);
  x["Code"] = symbol;
  return x;

}

function getNow() {
  let fd = new Date();
  return fd.getFullYear()
    + "-" + (fd.getMonth() + 1 < 10 ? "0" + (fd.getMonth() + 1) : fd.getMonth() + 1)
    + "-" + (fd.getDate() < 10 ? "0" + fd.getDate() : fd.getDate());
}

async function getPrices(symbol) {
  // 2022-12-06

  let f = async (symbol) => {
    return await fetch("https://restv2.fireant.vn/symbols/" + symbol + "/historical-quotes?startDate=2000-12-06&endDate=" + getNow() + "&offset=0&limit=" + LIMIT, {
      "headers": {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
        "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxOTQ3MjQ3NzkxLCJuYmYiOjE2NDcyNDc3OTEsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsib3BlbmlkIiwicHJvZmlsZSIsInJvbGVzIiwiZW1haWwiLCJhY2NvdW50cy1yZWFkIiwiYWNjb3VudHMtd3JpdGUiLCJvcmRlcnMtcmVhZCIsIm9yZGVycy13cml0ZSIsImNvbXBhbmllcy1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImZpbmFuY2UtcmVhZCIsInBvc3RzLXdyaXRlIiwicG9zdHMtcmVhZCIsInN5bWJvbHMtcmVhZCIsInVzZXItZGF0YS1yZWFkIiwidXNlci1kYXRhLXdyaXRlIiwidXNlcnMtcmVhZCIsInNlYXJjaCIsImFjYWRlbXktcmVhZCIsImFjYWRlbXktd3JpdGUiLCJibG9nLXJlYWQiLCJpbnZlc3RvcGVkaWEtcmVhZCJdLCJzdWIiOiIxZDY5YmE3NC0xNTA1LTRkNTktOTA0Mi00YWNmYjRiODA3YzQiLCJhdXRoX3RpbWUiOjE2NDcyNDc3OTEsImlkcCI6Ikdvb2dsZSIsIm5hbWUiOiJ0cmluaHZhbmh1bmdAZ21haWwuY29tIiwic2VjdXJpdHlfc3RhbXAiOiI5NTMyOGNlZi1jZmY1LTQ3Y2YtYTRkNy1kZGFjYWJmZjRhNzkiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0cmluaHZhbmh1bmdAZ21haWwuY29tIiwidXNlcm5hbWUiOiJ0cmluaHZhbmh1bmdAZ21haWwuY29tIiwiZnVsbF9uYW1lIjoiVHJpbmggVmFuIEh1bmciLCJlbWFpbCI6InRyaW5odmFuaHVuZ0BnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6InRydWUiLCJqdGkiOiJhMTY2MDQwOGNhMGFkYWQxOTcwZDVhNWZhMmFjNjM1NSIsImFtciI6WyJleHRlcm5hbCJdfQ.cpc3almBHrGu-c-sQ72hq6rdwOiWB1dIy1LfZ6cgjyH4YaBWiQkPt4l7M_nTlJnVOdFt9lM2OuSmCcTJMcAKLd4UmdBypeZUpTZp_bUv1Sd3xV8LHF7FSj2Awgw0HIaic08h1LaRg0pPzzf-IRJFT7YA8Leuceid6rD4BCQ3yNvz8r58u2jlCXuPGI-xA8W4Y3151hpNWCtemyizhzi7EKri_4WWpXrXPAeTAnZSdoSq87shTxm9Kyz_QJUBQN6PIEINl9sIQaKL-I_jR9LogYB_aM3hs81Ga6h-n-vbnFK8JR1JEJQmU-rxyX7XvuL-UjQVag3LxQeJwH7Nnajkkg",
        "sec-ch-ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site"
      },
      "referrerPolicy": "no-referrer",
      "body": null,
      "method": "GET",
      "mode": "cors",
      agent
    }, { timeout: 10000 });
  }
  let a = await f(symbol);
  let x = await a.text();
  while (![x.startsWith("[")]) {
    logger.warn(x);
    await wait(500);
    a = f(symbol);
    x = await a.text();
  }
  x = JSON.parse(x);
  return { 'data': x, 'Code': symbol };
}