/**
 * Created by dx.yang on 2016/12/2.
 */



const mongo = require('jodata-common/lib/mongo');
const config = require('./config')[process.env.NODE_ENV];

(async () => {

    let db = await mongo.init(config.mongo.addr, {
        server: {
            auto_reconnect: true,
            poolSize: 10
        }
    });

    let ids = await db.collection('stock_A').find({}, {_id : 1})
        .limit(100)
        .sort({timestamp:-1})
        .toArray();
    ids = ids.map(function(doc) { return doc._id; });
    let list = await db.collection('stock_A').find({_id: {$in: ids}}).toArray();
    console.log(list)
})();
