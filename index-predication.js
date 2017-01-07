/**
 * Created by dx.yang on 2016/12/24.
 */



// const googleFinance = require('./lib/googleFinance');
// const mongo = require('jodata-common/lib/mongo');
const lodash = require('lodash');
// const config = require('./config')[process.env.NODE_ENV];
const util = require('./lib/util');

// const mongoDB = require('mongodb');

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


function forecast(historyData, currentData, k, name) {

    let diffList = [];

    for (let i = 0; i < historyData.length - predictionLength; i++) {

        let oneData = historyData[i];
        if (!oneData) {
            continue;
        }
        let diff = 0;

        lodash.forEach(keys, key => {
            try {
                diff += Math.pow(oneData[key] - currentData[key], 2);
            } catch(e) {
                console.log(oneData, currentData);
                console.log(e);
            }
        });

        diffList.push({
            idx: i,
            diff
        });
    }

    diffList.sort((a, b) => {
        return a.diff - b.diff;
    });

    let predictionWeek = [];
    for (let pre = 1; pre <= predictionLength; pre++) {
        let row = {
            openThanPreClose: 0,
            closeThanOpen: 0,
        };
        for (let i = 0; i < k; i++) {
            let idx = diffList[i].idx;
            let willBe = historyData[idx + pre];

            row.openThanPreClose += willBe.openThanPreClose;
            row.closeThanOpen += willBe.closeThanOpen;

            lodash.forEach(fields, field => {
                let key = `${field}_change`;
                if (!row[key]) {
                    row[key] = willBe[key];
                } else {
                    row[key] += willBe[key];
                }
            });
        }
        lodash.forEach(fields, field => {
            let key = `${field}_change`;
            row[key] /= predictionLength;
        });
        row.openThanPreClose /= predictionLength;
        row.closeThanOpen /= predictionLength;
        // row.name = name;
        row.idx = pre;
        predictionWeek.push(row);
    }

    let baseIdx = 1;
    let upDays = 0;
    lodash.forEach(predictionWeek, day => {
        if (day.close_change > 0) {
            upDays += 1;
        }
        baseIdx *= (1 + day.close_change);
    });

    let predictionData = {
        name,
        upDays,
        closeChangeInFutrueWeek: baseIdx,
        week: predictionWeek
    };

    return predictionData;

    // db.collection(name).aggregate([
    //     {
    //         $match: {
    //             _id: {
    //                 '$in': ids[]
    //             }
    //         }
    //     }
    // ], () => {});
    /*
     db.getCollection('stock_AAL').aggregate([
     {
     $match: { "_id": { "$in": [ObjectId("585e2503f0f25412494dad9f"), ObjectId("585e2503f0f25412494dada0")] } }
     },
     {
     $group: {
     _id: null,
     avgClose: { $avg: "$close" }
     }
     }
     ])
     */

}

async function go(db) {

    // let db = await mongo.init(config.mongo.addr, {
    //     server: {
    //         auto_reconnect: true,
    //         poolSize: 10
    //     }
    // });

    // analyzer
    let start = new Date();

    await db.collection('analysis_data').drop();
    let list = await db.listCollections().toArray();
    for (let i = 0; i < list.length; i++) {
        console.log(i, '/', list.length, 'start');
        let stock = list[i];
        if (stock.name.indexOf('history_stock') == -1) {
            continue;
        }

        let data = await db.collection(stock.name).find().sort({ts: 1}).toArray();
        console.log('load history data...');

        let k = 11;
        let predicationData = forecast(data.slice(0, data.length), data[data.length - 1], k, stock.name);
        await db.collection('analysis_data').insertOne(predicationData);
        console.log('end', util.time2show(start, new Date()));
    }
}


async function test(db) {

    // let db = await mongo.init(config.mongo.addr, {
    //     server: {
    //         auto_reconnect: true,
    //         poolSize: 10
    //     }
    // });

    try {
        await db.collection('analysis_test').drop();
    } catch(e) {}


    let testFields = ['closeThanOpen', 'close_change'];
    let list = await db.listCollections().toArray();
    for (let i = 0; i < list.length; i++) {
        console.log(i, '/', list.length, 'start');
        let stock = list[i];
        if (stock.name.indexOf('history_stock') == -1) {
            continue;
        }

        let data = await db.collection(stock.name).find().sort({ts: 1}).toArray();
        console.log('load history data...', stock.name);

        let k = 11;

        let dataSize = data.length;
        let testSize = 10;

        let compare = {};
        lodash.forEach(testFields, f => {
            compare[f] = {
                right: 0,
                wrong: 0
            };
        });
        if (dataSize <= testSize + 10) {
            continue;
        }
        for (let start = dataSize - testSize - 10; start < dataSize - 10; start++) {
            // console.log('prediction', start);
            let predictionData = forecast(data.slice(0, start), data[start], k, stock.name);
            // ['closeThanOpen', 'close_change']
            lodash.forEach(predictionData.week, day => {
                let willBe = data[start + day.idx];
                lodash.forEach(testFields, f => {
                    let willBeValue = willBe[f];
                    let predictionValue = day[f];
                    let multiResult = willBeValue * predictionValue;
                    if (multiResult === 0) {
                        if (willBeValue == 0 && predictionValue == 0) {
                            compare[f].right += 1;
                        } else {
                            compare[f].wrong += 1;
                        }
                    } else if (multiResult > 0) {
                        compare[f].right += 1;
                    } else {
                        compare[f].wrong += 1;
                    }
                });
            });
        }

        let res = {
            name: stock.name
        };
        lodash.forEach(testFields, f => {
            res[`${f}_right`] = compare[f].right;
            res[`${f}_wrong`] = compare[f].wrong;
        });

        await db.collection('analysis_test').insertOne(res);

        //let predicationData = forecast()

        // let predicationData = forecast(data.slice(0, data.length), data[data.length - 1], k, stock.name);
        // await db.collection('analysis_data').insertMany(predicationData);
        //
        //
        // console.log('end', util.time2show(start, new Date()));
    }


}

// test().then(data => {
//
// });
// (async() => {
//
//     let db = await mongo.init(config.mongo.addr, {
//         server: {
//             auto_reconnect: true,
//             poolSize: 10
//         }
//     });
//
//     // analyzer
//     let start = new Date();
//
//     await db.collection('analysis_data').drop();
//     let list = await db.listCollections().toArray();
//     for (let i = 0; i < list.length; i++) {
//         console.log(i, '/', list.length, 'start');
//         let stock = list[i];
//         if (stock.name.indexOf('history_stock') == -1) {
//             continue;
//         }
//
//         let data = await db.collection(stock.name).find().sort({ts: 1}).toArray();
//         console.log('load history data...');
//
//         let k = 11;
//         let predicationData = forecast(data.slice(0, data.length), data[data.length - 1], k, stock.name);
//         await db.collection('analysis_data').insertMany(predicationData);
//         console.log('end', util.time2show(start, new Date()));
//     }
//
//
// })().then(() => {
//     console.log('program done');
// });

module.exports = {
    go,
    test
};

