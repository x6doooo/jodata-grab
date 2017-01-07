/**
 * Created by dx.yang on 2016/12/27.
 */


const mongoDB = require('mongodb');


const lodash = require('lodash');
const cluster = require('cluster');

const k = 20;

var tmpCollectionName = `forecast_tmp_collection`;
if (cluster.isMaster) {} else {
    tmpCollectionName += cluster.worker.id;
}

const predictionLength = 10;
const fields = ['open', 'close', 'high', 'low', 'avg', 'volume', 'closeThanOpen'];
const steps = [5, 10, 30, 60, 90];


let keys = [
    'amplitude',
    'posInYear'
];

lodash.forEach(fields, field => {
    keys.push(`${field}_change`);
    keys.push(`${field}_position`);
    lodash.forEach(steps, step => {
        keys.push(`${field}_ma_${step}_change`);
        keys.push(`${field}_ma_${step}_position`);
    });
});

function compute(historyStockName, historyData, currentData) {
    // console.log(historyData.length, 324);
    let diffList = [];

    for (let i = 0; i < historyData.length - predictionLength; i++) {

        let oneData = historyData[i];
        if (!oneData) {
            continue;
        }
        let diff = 0;

        for(let keyIdx = 0; keyIdx < keys.length; keyIdx++) {
            let key = keys[keyIdx];
            if (oneData[key] === undefined || currentData[key] === undefined) {
                //console.log(historyStockName, oneData, currentData);
                console.log('fxxk!!!!!!!!');
                return [];
                // throw new Error('fxxk!!!!!');
            } else {
                try {
                    let cur = Math.pow(currentData[key] - oneData[key], 2);
                    if (cur == Infinity) {
                        return [];
                        // console.log(1111, currentData[key], oneData[key], key);
                    } else {
                        diff += cur;
                    }
                } catch(e) {
                    return [];
                    // console.log(1, e);
                }
            }
        }

        // lodash.forEach(keys, key => {
        // });

        if (diff !== Infinity && !isNaN(diff)) {
            diffList.push({
                name: historyStockName,
                recordId: oneData._id,
                diff
            });
        }
    }

    return diffList;
}

async function go(db, stockName, recordId, list) {

    try {
        await db.collection(tmpCollectionName).drop();
    } catch(e) {}

    let theRecord = await db.collection(`history_stock_${stockName}`).findOne({_id: recordId});
    let isValible = true;
    lodash.forEach(theRecord, (v, k) => {
        if (v == Infinity) {
            isValible = false;
        }
    });
    if (!isValible) {
        return null;
    }

    if (!list) {
        list = await db.listCollections().toArray();
    }
    for (let i = 0; i < list.length; i++) {
        // console.log(`forecast ${i}/${list.length}`);
        let stock = list[i];
        if (stock.name.indexOf('history_stock') == -1) {
            continue;
        }

        let data = await db.collection(stock.name).find({
            ts: {
                $lt: theRecord.ts
            }
        }).sort({ts: 1}).toArray();

        let tmpData = compute(stock.name, data, theRecord);
        // console.log(''length, tmpData.length);
        if (tmpData.length) {
            await db.collection(tmpCollectionName).insertMany(tmpData)
        }

    }

    let nearlySet = await new Promise((resolve, reject) => {
        db.collection(tmpCollectionName).aggregate([
            {
                $sort: {
                    diff: 1
                }
            },
            {
                $limit: k
            }
        ], {
            allowDiskUse: true
        }, (err, results) => {
            resolve(results);
        });
    });

    if (nearlySet.length == 0) {
        return null;
    }

    let dataSet = [];
    for (let i = 0; i < nearlySet.length; i++) {
        let cur = nearlySet[i];
        let item = await db.collection(cur.name).find({
            _id: {
                $gt: mongoDB.ObjectID(cur.recordId)
            }
        }).limit(predictionLength).toArray();
        dataSet.push(item);
    }


    let prediction10day = [];
    for (let i = 0; i < predictionLength; i++) {
        let row = {
            openThanPreClose: 0,
            closeThanOpen: 0,
        };
        lodash.forEach(dataSet, d => {
            row.openThanPreClose += d[i].openThanPreClose;
            row.closeThanOpen += d[i].closeThanOpen;
            lodash.forEach(fields, field => {
                let key = `${field}_change`;
                if (!row[key]) {
                    row[key] = d[i][key];
                } else {
                    row[key] += d[i][key];
                }
            });
        });
        lodash.forEach(fields, field => {
            let key = `${field}_change`;
            row[key] /= dataSet.length;
        });
        row.openThanPreClose /= dataSet.length;
        row.closeThanOpen /= dataSet.length;
        row.idx = i;
        // console.log(row);
        prediction10day.push(row);
    }

    let baseIdx = 1;
    let upDays = 0;
    lodash.forEach(prediction10day, day => {
        if (day.close_change > 0) {
            upDays += 1;
        }
        baseIdx *= (1 + day.close_change);
    });

    let predictionData = {
        stockName,
        upDays,
        closeChangeInFuture10day: baseIdx,
        day10: prediction10day
    };

    console.log('forecast done');
    return predictionData;

}


module.exports = {
    go
};


