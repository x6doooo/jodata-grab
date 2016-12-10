/**
 * Created by dx.yang on 2016/11/30.
 */

// xueqiu init


// const googleFinance = require('./lib/googleFinance');
const mongo = require('jodata-common/lib/mongo');
const lodash = require('lodash');
const loader = require('./lib/loader');
const schedule = require('node-schedule');

const config = require('./config')[process.env.NODE_ENV];


// function wash(db) {
//     let collectionName = 'stock_A';
//     db.collection(collectionName).find().sort({ts: 1}).toArray((err, docs) => {
//         let cache = {};
//         let lastKey;
//         lodash.forEach(docs, (d) => {
//             let date = new Date(d.ts * 1000);
//             let year = date.getFullYear();
//             let month = date.getMonth() + 1;
//             month = month < 10 ? '0' + month : month;
//             let day = date.getDate();
//             date = year + '-' + month + '-' + day;
//             if (!cache[date]) {
//                 cache[date] = [];
//                 if (lastKey) {
//                     //cache[lastKey]
//                 }
//             } else {
//                 cache[date].push(d.close);;
//             }
//         });
//     });
// }


(async() => {

    let db = await mongo.init(config.mongo.addr, {
        server: {
            auto_reconnect: true,
            poolSize: 10
        }
    });


    // let list = await new Promise((resolve, reject) => {
    //     db.collection('list').find().toArray((err, docs) => {
    //         resolve(docs);
    //     });
    // });


    // await xueqiu.init(config.xueqiu.telephone, config.xueqiu.password);
    // setInterval(() => {
    //     lodash.forEach(list, (stock, idx) => {
    //         xueqiu.getChartData(stock.code, '1d').then((data) => {
    //             console.log(idx, data.chartlist.length);
    //         }).catch(e => {
    //             console.log(idx, e.message);
    //         });
    //     });
    // }, 1000 * 10);




    // wash(db);

    //------------ init ----------------
    // let list = await new Promise((resolve, reject) => {
    //     db.collection('list').find().toArray((err, docs) => {
    //         resolve(docs);
    //     });
    // });
    // let checkExist = true;
    // let startTime = new Date();
    // while(list.length != 0) {
    //     console.log('------------------ new start ------------', list.length)
    //     list = await loader.go(list, db, '1h', '730d', startTime, checkExist);
    // }
    //
    // console.log('done all');


    //------------ crontab --------------
    schedule.scheduleJob('0 0 6 * * *', function() {
        (async() => {
            let list = await db.collection('list').find().toArray();
            let dontCheckExist = false;
            let startTime = new Date();
            console.log('start-----');
            while(list.length != 0) {
                console.log('------------------ new start ------------', dontCheckExist)
                list = await loader.go(list, db, '1h', '2d', startTime);
            }
            console.log('done all');
        })();
    });



})().then(() => {
    console.log('program done');
});


