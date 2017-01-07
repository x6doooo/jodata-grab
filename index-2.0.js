/**
 * Created by dx.yang on 2017/1/7.
 */


const mongo = require('jodata-common/lib/mongo');
const lodash = require('lodash');
const loader = require('./lib/loader-2.0');
const schedule = require('node-schedule');
const BaseRequest = require('jodata-common/lib/BaseRequest');
const config = require('./config')[process.env.NODE_ENV];

const xueqiu = require('./lib/xueqiu');

const analyzer = require('./lib/analyzer');

const predication = require('./index-predication');

(async() => {

    let db = await mongo.init(config.mongo.addr, {
        server: {
            auto_reconnect: true,
            poolSize: 10
        }
    });


    //------------ crontab --------------
    // schedule.scheduleJob('0 0 6 * * *', function () {
        (async() => {
            // init xueqiu
            // await xueqiu.init(db);
            // await xueqiu.getList(db);

            try {
                let stocks = await db.listCollections().toArray();
                for (let i = 0; i < stocks.length; i++) {
                    let name = stocks[i].name;
                    if (name.indexOf('code_') != -1) {
                        await db.collection(stocks[i].name).drop();
                    }
                }
            } catch (e) {
            }

            let list;
            try {
                let theLastDay = new Date();
                theLastDay.setDate(theLastDay.getDate() - 3);
                theLastDay.setHours(0);
                theLastDay.setMinutes(0);
                theLastDay.setSeconds(0);
                theLastDay.setMilliseconds(0);
                list = await db.collection('summaries').find({
                    time: {
                        $gt: theLastDay
                    }
                }).toArray();
            } catch (e) {
                console.log(e)
            }

            // 去重
            let hash = {};
            lodash.forEach(list, item => {
                if (!hash[item.code] || hash[item.code].time < item.time) {
                    hash[item.code] = item;
                }
            });

            list = [];
            lodash.forEach(hash, item => {
                list.push(item);
            });


            let dontCheckExist = false;
            let startTime = new Date();
            console.log('start-----');
            while (list.length != 0) {
                console.log('------------------ new start ------------', dontCheckExist);
                // list = await loader.go(list, db, '1m', '300d', startTime);
                list = await loader.go(list, db, '1d', '100d', startTime, false, 'code_', '_daily');
            }
            console.log('done all');

            list = [];
            lodash.forEach(hash, item => {
                list.push(item);
            });
            while (list.length != 0) {
                console.log('------------------ new start ------------', dontCheckExist);
                // list = await loader.go(list, db, '1m', '300d', startTime);
                list = await loader.go(list, db, '1m', '1d', startTime, false, 'code_', '_1min');
            }

            // console.log('analyzer go!');
            // // await analyzer.go(db);
            // console.log('analyzer done!');


        })();
    // });
    // console.log('predication go!');
    // await predication.go(db);
    // await predication.test(db);
    // console.log('predication done!');



})().then(() => {
    console.log('index-2.0 program done');
});
