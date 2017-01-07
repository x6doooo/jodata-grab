/**
 * Created by dx.yang on 2016/12/1.
 */

const yahoo = require('jodata-common/lib/yahoo');
const lodash = require('lodash');
const util = require('./util');


async function go(list, db, interval, range, startTime, checkExist, prefix) {


    prefix = prefix || 'stock_';

    let fails = [];
    for (let i = 0, len = list.length; i < len; i++) {
        // for (let i = 0, len = list.length; i < 1; i++) {
        let timeFormated = util.time2show(startTime, new Date());

        let info = [
            `total: ${list.length}`,
            `current: ${i}`,
            `fails: ${fails.length}`,
            `time: ${timeFormated}`
        ];
        console.log(info.join(', '));
        let stock = list[i];
        if (stock.isFailInYahoo || stock.volume < 100 * 10000 || stock.current < 10 || stock.current > 60 || stock.instOwn > 50) {
            console.log('jump');
            continue;
        }
        let collectionName = prefix + stock.code;
        if (checkExist) {
            let isExists = await new Promise((resolve, reject) => {
                db.collection(collectionName).count((err, res) => {
                    resolve(res != 0);
                });
            });
            if (isExists) {
                console.log(collectionName, 'has been loaded');
                continue;
            }
        }
        let err;
        await new Promise((resolve, reject) => {
            setTimeout(async() => {
                let data;
                try{
                    data = await yahoo.get(stock.code, interval, range);
                } catch(e) {
                    data = 'fail';
                }
                if (data == 'fail') {
                    err = 1;
                    resolve();
                    return;
                }
                console.log('size:', data.length);
                data = JSON.parse(data);
                let timestamps = lodash.get(data, 'chart.result[0].timestamp');
                let quote = lodash.get(data, 'chart.result[0].indicators.quote[0]');
                let arr = [];

                if (!timestamps || !quote) {
                } else {
                    // let lastTimestamp = await new Promise((resolve, reject) => {
                    //     db.collection(collectionName).findOne({}, {
                    //         fields: {
                    //             ts: 1
                    //         },
                    //         sort: {
                    //             ts: -1
                    //         },
                    //         limit: 1
                    //     }, (err, doc) => {
                    //         if (err) {
                    //             resolve(0);
                    //         } else {
                    //             if (doc) {
                    //                 resolve(doc.ts);
                    //             } else {
                    //                 resolve(0);
                    //             }
                    //         }
                    //     });
                    // });

                    let fields = ['open', 'close', 'high', 'low', 'mean', 'volume'];
                    lodash.forEach(timestamps, (ts, idx) => {

                        let row = {};

                        row.isValid = true;
                        row.ts = ts * 1000;
                        lodash.forEach(fields, f => {
                            switch(f) {
                                case 'mean':
                                    row.mean = (quote.low[idx] + quote.high[idx]) / 2;
                                    break;
                                case 'volume':
                                    row.volume = quote.volume[idx] / 10000.0;
                                    break;
                                default:
                                    row[f] = quote[f][idx];
                                    break;
                            }
                            if (arr.length > 0) {
                                let theLastValue = arr[arr.length - 1][f];
                                if (theLastValue == 0) {
                                    row.isValid = false;
                                }
                                row[f + '_change_rate'] = (row[f] - theLastValue) / theLastValue;
                            } else {
                                row.isValid = 0;
                            }
                        });

                        let date = new Date(row.ts);
                        let year = date.getUTCFullYear();
                        let month = date.getUTCMonth() + 1;
                        let day = date.getUTCDate();
                        if (month < 10) {
                            month = '0' + month;
                        }
                        if (day < 10) {
                            day = '0' + day;
                        }
                        date = `${year}-${month}-${day}`;
                        row.date = date;

                        if (row.close) {
                            arr.push(row);
                        }
                    });

                    arr = arr.filter(item => {
                        return item.isValid;
                    });

                    if (arr.length) {
                        // 写入最新
                        await db.collection(collectionName).createIndex({ts:1,date:1});
                        await db.collection(collectionName).insertMany(arr);
                    }
                }
                resolve();
            }, 1 * 1000);
        });
        if (err) {
            console.log(stock.code, 'fail');
            fails.push(stock);
        } else {
            console.log(stock.code, stock.name, 'done');
        }
    }
    return fails;

}


module.exports = {
    go
};