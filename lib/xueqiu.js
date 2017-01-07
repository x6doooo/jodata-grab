/**
 * Created by dx.yang on 2016/12/10.
 */

const BaseRequest = require('jodata-common/lib/BaseRequest');
const crypto = require('crypto');
const config = require('../config')[[process.env.NODE_ENV]];
const lodash = require('lodash');
const querystring = require('querystring');


const xqUrls = {
    csrf: 'https://xueqiu.com/service/csrf?api=/user/login',
    login: 'https://xueqiu.com/user/login',
    stockList: 'https://xueqiu.com/stock/cata/stocklist.json',
    stockDetail: 'https://xueqiu.com/v4/stock/quote.json',
    stockDataForChart: 'https://xueqiu.com/stock/forchart/stocklist.json'
};

let login_params = {
    telephone: config.xueqiu.telephone,
    remember_me: 'on',
    areacode: '86'
};

let md5 = crypto.createHash('md5');
md5.update(config.xueqiu.password);

login_params.password = md5.digest('hex').toUpperCase();

function init() {
    return new Promise((resolve, reject) => {
        BaseRequest.get(xqUrls.csrf, (e, r) => {
            if (e) {
                console.log(e);
                return
            }
            BaseRequest.post({
                url: xqUrls.login,
                form: login_params
            }, (err, resp) => {
                if (err || resp.statusCode >= 400) {
                    console.log('xueqiu login failed');
                    reject(1);
                    return
                }
                console.log('xueqiu login success');
                resolve(0)
            });
        });
    });
}


function getBaseList(params) {
    params = lodash.assign({
        page: 1,
        size: 100,
        order: 'asc',
        orderby: 'code',
        type: '0,1,2',
        _: new Date() * 1
    }, params);
    let qs = querystring.stringify(params);
    let apiUrl = xqUrls.stockList + '?' + qs;
    return new Promise((resolve, reject) => {
        BaseRequest.get(apiUrl, (err, resp, body) => {
            if (err) {
                reject(err);
                return;
            }
            body = JSON.parse(body);
            resolve(body);
        });
    });
}

let numberKeys = [
    "current", "percentage", "change", "open", "close", "high", "low",
    "high52week", "low52week", "volume", "volumeAverage", "marketCapital",
    "eps", "pe_ttm", "pe_lyr", "totalShares", "turnover_rate", "instOwn",
    "net_assets", "amplitude", "pb", "moving_avg_200_day", "chg_from_200_day_moving_avg",
    "pct_chg_from_200_day_moving_avg", "moving_avg_50_day", "chg_from_50_day_moving_avg",
    "pct_chg_from_50_day_moving_avg", "shares_outstanding", "ebitda", "short_ratio",
    "pe_estimate_next_year", "peg_ratio", "eps_estimate_next_year",
    "eps_estimate_next_quarter", "eps_estimate_current_quarter", "psr",
    "revenue", "profit_margin",
];
function formatDetail(item) {
    lodash.forEach(numberKeys, (k) => {
        item[k] = item[k] * 1;
    });
    item.udpateAt = new Date(item.udpateAt);
    item.time = new Date(item.time);
    return item;
}

async function getDetailsAndSave(mongo, codes) {
    let paramCodes = codes.join();
    let qs = querystring.stringify({
        code: paramCodes,
        _: new Date() * 1
    });
    let apiUrl = xqUrls.stockDetail + '?' + qs;
    let dataList = await new Promise((resolve, reject) => {
        BaseRequest.get(apiUrl, (err, resp, body) => {
            if (err) {
                console.log(err);
                return;
            }
            // console.log(body);
            let details = JSON.parse(body);
            let arr = [];
            lodash.forEach(details, item => {
                item = formatDetail(item);
                arr.push(item);
            });
            resolve(arr);
        });
    });
    for (let i = 0; i < dataList.length; i++) {
        let item = dataList[i];
        let theSameOne = await mongo.collection('summaries').findOne({
            code: item.code,
            time: item.time
        });
        if (!theSameOne) {
            await mongo.collection('summaries').insertOne(item);
        }
    }
}

async function getList(mongo) {

    let total = 1;
    let count = 0;
    let page = 1;
    let size = 100;

    let codes = [];
    while(count < total) {
        let data;
        try {
            data = await getBaseList({page, size});
        } catch(e) {
            console.log(e);
            continue;
        }
        let list = lodash.get(data, 'stocks');
        lodash.forEach(list, (d) => {
            if (d.code) {
                codes.push(d.code);
            }
        });
        total = lodash.get(data, 'count.count');
        count = count + size;
        let less = total - count;
        if (size > less) {
            size = less;
        }
        page += 1;
        console.log('get list from xueqiu: ', page);
    }
    console.log('get list from xueqiu done');

    let idx = 0;
    let codesSize = codes.length;
    while(codes.length) {
        console.log('get detail from xueqiu:', idx++);
        let someCodes = codes.splice(0, 50);
        await getDetailsAndSave(mongo, someCodes);
    }
    console.log('load done');
    return codesSize;

}

module.exports = {
    init,
    getList
};



