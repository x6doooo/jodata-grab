/**
 * Created by dx.yang on 2016/12/16.
 */


// const googleFinance = require('./lib/googleFinance');
const mongo = require('jodata-common/lib/mongo');
const lodash = require('lodash');
const util = require('./util');


const fields = ['open', 'close', 'high', 'low', 'avg', 'volume', 'closeThanOpen'];
const steps = [5, 10, 30, 60, 90];
// const predictionLength = 10;
let baseStartIdx = Math.max.apply(Math, steps);


function getDatePositionInYear(timestamp) {
    let currentDate = new Date(timestamp);
    currentDate.setHours(0);
    currentDate.setMinutes(0);
    currentDate.setSeconds(0);
    currentDate.setMilliseconds(0);
    let yearStart = new Date(currentDate);
    yearStart.setMonth(0);
    yearStart.setDate(1);
    let diff = currentDate - yearStart;
    diff = diff / (24 * 60 * 60 * 1000);
    return diff / 365;
}

function initBaseData(data, baseStartIdx) {
    let baseData = {};
    if (data.length < baseStartIdx) {
        return;
    }
    for (let i = 0; i < baseStartIdx; i++) {
        let d = data[i];
        lodash.forEach(steps, step => {
            if (i >= baseStartIdx - step) {
                lodash.forEach(fields, field => {
                    let key = `last_${step}_day_sum_${field}`;
                    let srcDataKey = key + '_srcData';
                    if (baseData[key] === undefined) {
                        baseData[key] = d[field];
                        baseData[srcDataKey] = [d[field]];
                    } else {
                        baseData[key] += d[field];
                        baseData[srcDataKey].push(d[field]);
                    }
                });
            }
        });
    }
    return baseData;
}

function loadData(db, stockName) {
    return new Promise((resolve, reject) => {
        db.collection(stockName).aggregate([{
            $project: {
                date: 1,
                ts: 1,
                close: 1,
                open: 1,
                low: 1,
                high: 1,
                volume: 1,
                closeThanOpen: {
                    $divide: [{
                        $subtract: ['$close', '$open']
                    }, '$open']
                },
                avg: { $avg: ['$high', '$low'] }
            }
        }, {
            $sort: {
                ts: 1
            }
        }], (err, results) => {
            if (err) {
                reject(err);
                return;
            }
            resolve(results);
        });
    });
}


async function go(db) {

    let startTime = new Date();
    // 历史点入库
    let list = await db.listCollections().toArray();

    let listSize = list.length;
    for (let i = 0; i < listSize; i++) {
        let stock = list[i];
        console.log(i, '/', listSize, stock.name, util.time2show(startTime, new Date()));
        if (stock.name.indexOf('stock_') == -1) {
            continue;
        }

        let data;
        try {
            data = await loadData(db, stock.name);
            console.log(2, data.length);
        } catch(e) {
            console.log(e);
        }

        if (!data) {
            continue;
        }

        // 初始化基础数据
        let baseData = initBaseData(data, baseStartIdx);

        /**
         *  key_ma_step
         *  key_ma_step_position
         *  key_ma_step_change
         *  key_change
         */
        let dataSet = [];
        for (let i = baseStartIdx + 1; i < data.length; i++) {
            let currentPoint = data[i];
            let result = {
                srcRecordId: currentPoint._id,
                ts: currentPoint.ts,
                date: currentPoint.date
            };

            // amplitude
            // 振幅
            result.amplitude = (currentPoint.high - currentPoint.low) / currentPoint.avg;

            // position in year
            // 日期在365天里的位置
            result.posInYear = getDatePositionInYear(currentPoint.ts);

            // open than previous close
            result.openThanPreClose = (currentPoint.open - data[i - 1].close) / data[i - 1].close;
            // result.closeThanOpen = (currentPoint.close - currentPoint.open) / currentPoint.open;
            result.closeThanOpen = currentPoint.closeThanOpen;

            let dataIsValiable = true;
            lodash.forEach(steps, step => {
                let headPoint = data[i - step];
                lodash.forEach(fields, field => {

                    let baseDataFieldName = `last_${step}_day_sum_${field}`;
                    let baseDataSrcFieldName = baseDataFieldName + '_srcData';
                    let baseDataSrcMaFieldName = baseDataSrcFieldName + '_ma';

                    // moving average
                    let maKey = `${field}_ma_${step}`;
                    baseData[baseDataFieldName] = baseData[baseDataFieldName] - headPoint[field] + currentPoint[field];
                    result[maKey] = baseData[baseDataFieldName] / step;

                    // moving average position in history (total size is current step)
                    if (!baseData[baseDataSrcMaFieldName]) {
                        dataIsValiable = false;
                        baseData[baseDataSrcMaFieldName] = [result[maKey]];
                    } else {

                        let maBaseData = baseData[baseDataSrcMaFieldName];

                        if (maBaseData.length === step) {
                            maBaseData.shift();
                        }
                        let maMax = Math.max.apply(Math, maBaseData);
                        let maMin = Math.min.apply(Math, maBaseData);
                        if (maMax == maMin) {
                            dataIsValiable = false;
                        } else {
                            result[`${maKey}_position`] = (result[maKey] - maMin) / (maMax - maMin);
                        }
                        // moving average change
                        let theLastMaValue = maBaseData[maBaseData.length - 1];
                        result[`${maKey}_change`] = (result[maKey] - theLastMaValue) / theLastMaValue;

                        maBaseData.push(result[maKey]);
                    }

                    let srcDataSet = baseData[baseDataSrcFieldName];
                    srcDataSet.shift();
                    let max = Math.max.apply(Math, srcDataSet);
                    let min = Math.min.apply(Math, srcDataSet);


                    let theLastValueInSrcDataSet = srcDataSet[srcDataSet.length - 1];
                    result[`${field}_change`] = (currentPoint[field] - theLastValueInSrcDataSet) / theLastValueInSrcDataSet;
                    baseData[baseDataSrcFieldName].push(currentPoint[field]);

                    if (max == min) {
                        dataIsValiable = false;
                    } else {
                        result[`${field}_position`] = (result[maKey] - min) / (max - min);
                        // FutureData
                        // let futureData = data.slice(i + 1, i + 1 + predictionLength);
                        // let ids = [];
                        // lodash.forEach(futureData, fd => {
                        //     ids.push(fd._id);
                        // });
                        // result.futureDataIds = ids;
                    }

                });
            });
            if (dataIsValiable) {
                dataSet.push(result)
            }
        }

        if (dataSet.length) {

            await db.collection('history_' + stock.name).createIndex({ts:1});
            await db.collection('history_' + stock.name).insertMany(dataSet);
        }


    }

}

module.exports = {
    go
};




