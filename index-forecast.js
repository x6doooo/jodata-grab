/**
 * Created by dx.yang on 2016/12/27.
 */


const mongo = require('jodata-common/lib/mongo');
const lodash = require('lodash');
const config = require('./config')[process.env.NODE_ENV];
const mongoDB = require('mongodb');
const util = require('./lib/util');

const forecast = require('./lib/forecast');


const cluster = require('cluster');
// const http = require('http');
const numCPUs = require('os').cpus().length;

if (cluster.isMaster) {

    (async() => {
        try {
            let db = await mongo.init(config.mongo.addr, {
                server: {
                    auto_reconnect: true,
                    poolsize: 10
                }
            });
            db.collection('analysis_data').drop()
        } catch(e) {}
        for (var i = 0; i < numCPUs; i++) {
            cluster.fork();
        }

        cluster.on('exit', (worker, code, signal) => {
            console.log(`worker ${worker.process.pid} died`);
        });
    })();
    // let
    // messageHandler = (msg) => {
    //
    // };
    // Object.keys(cluster.workers).forEach((id) => {
    //     cluster.workers[id].on('message', messageHandler);
    // });
} else {

    // console.log(cluster.worker.id);

    (async() => {
        let db = await mongo.init(config.mongo.addr, {
            server: {
                auto_reconnect: true,
                poolsize: 10
            }
        });


        let list = await db.listCollections().toArray();

        list = list.filter(item => {
            return item.name.indexOf('history_stock') != -1;
        });
        list.sort((a, b) => {
            return a.name.localeCompare(b.name);
        });

        let step = ~~(list.length / numCPUs);

        let start = new Date();
        let end = ((id) => {
            if (id == numCPUs) {
                return list.length;
            }
            return step * id;
        })(cluster.worker.id);


        console.log('init', step * (cluster.worker.id - 1), cluster.worker.id, end);
        for (let i = step * (cluster.worker.id - 1); i < end; i++) {
            console.log('-----------------------------------------------');
            console.log('id:', cluster.worker.id, i, end, util.time2show(start, new Date()));
            let stock = list[i];
            let data = await db.collection(stock.name).find().sort({ts: -1}).toArray();
            let id = data[0]._id;
            let name = stock.name.replace('history_stock_', '');
            data = await forecast.go(db, name, id, list);
            if (data) {
                console.log('insert db');
                await db.collection('analysis_data').insertOne(data);
            }
        }

        // await forecast.go(db, 'PPP', mongoDB.ObjectId("58619a4ca62360371f7e02b3"));


    })();
}


