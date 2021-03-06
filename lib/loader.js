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
        if (stock.isFailInYahoo || stock.volume < 100 * 10000 || stock.current < 7 || stock.current > 60 || stock.instOwn > 50) {
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
                    // await new Promise((resolve, reject) => {
                    //     db.collection('list').findOneAndUpdate({
                    //         code: stock.code
                    //     }, {
                    //         $set: {
                    //             isFailInYahoo: true
                    //         }
                    //     }, (err, r) => {
                    //         console.log(stock.code, 'isFailInYahoo!!!!');
                    //         resolve(r)
                    //     })
                    // });
                } else {
                // if (timestamps && quote) {

                    let lastTimestamp = await new Promise((resolve, reject) => {
                        db.collection(collectionName).findOne({}, {
                            fields: {
                                ts: 1
                            },
                            sort: {
                                ts: -1
                            },
                            limit: 1
                        }, (err, doc) => {
                            if (err) {
                                resolve(0);
                            } else {
                                if (doc) {
                                    resolve(doc.ts);
                                } else {
                                    resolve(0);
                                }
                            }
                        });
                    });

                    // quote.mean = [];
                    lodash.forEach(timestamps, (ts, idx) => {
                        let close = quote.close[idx];
                        let low = quote.low[idx];
                        let high = quote.high[idx];
                        let open = quote.open[idx];
                        let volume = quote.volume[idx] / 10000.0;
                        let mean = (low + high) / 2;
                        // quote.mean.push(mean);
                        ts *= 1000;
                        let date = new Date(ts);
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

                        // let keys = ['close', 'open', 'low', 'high', 'volume'];
                        // let steps = [5, 10, 30, 60, 90];
                        // let limitValue = {};
                        // steps.forEach(s => {
                        //     if (idx >= s) {
                        //         keys.forEach(k => {
                        //             let dataSlice = quote[k].slice(idx - s, idx);
                        //             limitValue[`${k}_max_${step}`] = Math.max.apply(Math, dataSlice);
                        //             limitValue[`${k}_min_${step}`] = Math.min.apply(Math, dataSlice);
                        //             if (k === 'volume') {
                        //                 limitValue[`volume_max_${step}`] /= 10000.0;
                        //                 limitValue[`volume_min_${step}`] /= 10000.0;
                        //             }
                        //         });
                        //     }
                        // });

                        if (close && ts > lastTimestamp) {
                            let item = {
                                date,
                                ts,
                                close,
                                high,
                                low,
                                open,
                                mean,
                                volume,
                                high_over_low: high / low,
                                close_over_open: close / open,
                            };
                            // item = Object.assign(item, limitValue);
                            arr.push(item);
                        }
                    });
                    if (arr.length) {
                        // 写入最新
                        await db.collection(collectionName).createIndex({ts:1,date:1});
                        await db.collection(collectionName).insertMany(arr);
                        // // 删除过去
                        // if (!checkExist) {
                        //     let ids = await db.collection(collectionName).find({}, {_id : 1})
                        //         .limit(arr.length)
                        //         .sort({ts: 1})
                        //         .toArray();
                        //     ids = ids.map(function(doc) { return doc._id; });
                        //     await db.collection(collectionName).remove({_id: {$in: ids}})
                        // }
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