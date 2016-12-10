/**
 * Created by dx.yang on 2016/12/1.
 */

const yahoo = require('jodata-common/lib/yahoo');
const lodash = require('lodash');

async function go(list, db, interval, range, startTime, checkExist) {
    let fails = [];
    for (let i = 0, len = list.length; i < len; i++) {
        // for (let i = 0, len = list.length; i < 1; i++) {
        let time2show = (() => {
            let diff = new Date() - startTime;
            let x = diff / 1000
            let seconds = ~~(x % 60);
            seconds = seconds < 10 ? '0' + seconds : seconds;
            x /= 60;
            let minutes = ~~(x % 60);
            minutes = minutes < 10 ? '0' + minutes : minutes;
            x /= 60;
            let hours = ~~(x % 24);
            hours = hours < 10 ? '0' + hours : hours;
            return `${hours}:${minutes}:${seconds}`
        })();
        let info = [
            `total: ${list.length}`,
            `current: ${i}`,
            `fails: ${fails.length}`,
            `time: ${time2show}`
        ];
        console.log(info.join(', '));
        let stock = list[i];
        let collectionName = 'stock_' + stock.code;
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
                if (timestamps && quote) {

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


                    lodash.forEach(timestamps, (ts, idx) => {
                        let close = quote.close[idx];
                        let low = quote.low[idx];
                        let high = quote.high[idx];
                        let open = quote.open[idx];
                        let volume = quote.volume[idx];
                        ts *= 1;
                        if (close && ts > lastTimestamp) {
                            arr.push({
                                ts,
                                close,
                                high,
                                low,
                                open,
                                volume
                            });
                        }
                    });
                    if (arr.length) {
                        // 写入最新
                        await db.collection(collectionName).insertMany(arr);
                        // 删除过去
                        if (checkExist) {
                            let ids = await db.collection(collectionName).find({}, {_id : 1})
                                .limit(arr.length)
                                .sort({ts: 1})
                                .toArray();
                            ids = ids.map(function(doc) { return doc._id; });
                            await db.collection(collectionName).remove({_id: {$in: ids}})
                        }
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