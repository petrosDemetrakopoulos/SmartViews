let contract = null;
const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const contractController = require('./contractController');
const computationsController = require('./computationsController');
const transformations = require('../helpers/transformations');
let config = require('../config_private');

function setContract (contractObject, account) {
    cacheController.setContract(contractObject, account);
}

function reduceGroupByFromCache (cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId, callback) {
    let reductionTimeStart = helper.time();
    computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields, async function (reducedResult, error) {
        let reductionTimeEnd = helper.time();
        if (error) {
            return callback(error);
        }

        let viewMeta = helper.extractViewMeta(view);
        if (view.operation === 'AVERAGE') {
            reducedResult = transformations.transformAverage(reducedResult, view.gbFields, view.aggregationField);
        } else {
            reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
        }
        reducedResult.field = view.aggregationField;
        reducedResult.viewName = view.name;
        reducedResult.operation = view.operation;
        let cacheSaveTimeStart = helper.time();
        cacheController.saveOnCache(reducedResult, view.operation, latestId - 1).on('error', (err) => {
            helper.log('error:', err);
            return callback(err);
        }).on('receipt', (receipt) => {
            helper.log('receipt:' + JSON.stringify(receipt));
            let cacheSaveTimeEnd = helper.time();
            let times2 = { sqlTimeEnd: reductionTimeEnd, sqlTimeStart: reductionTimeStart,
                totalStart: times.totalStart, cacheSaveTimeStart: cacheSaveTimeStart,
                cacheSaveTimeEnd: cacheSaveTimeEnd, cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd };
            clearCacheIfNeeded(sortedByEvictionCost, reducedResult, times2, function (err, results) {
                if (!err) {
                    helper.printTimes(results);
                    return callback(null, results);
                }
                console.log(err);
                return callback(err);
            });
        });
    });
}

function mergeCachedWithDeltasResultsSameFields(view, cachedGroupBy, groupBySqlResult, latestId, sortedByEvictionCost, times, callback) {
    let viewMeta = helper.extractViewMeta(view);
    let rows = helper.extractGBValues(cachedGroupBy, view);
    let rowsDelta = helper.extractGBValues(groupBySqlResult, view);
    let mergeTimeStart = helper.time();
    computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewMeta.viewNameSQL, view, viewMeta.lastCol, viewMeta.prelastCol, function (mergeResult, error) {
        let mergeTimeEnd = helper.time();
        // SAVE ON CACHE BEFORE RETURN
        helper.log('SAVE ON CACHE BEFORE RETURN');
        if (error) {
            return callback(error);
        }
        mergeResult.operation = view.operation;
        mergeResult.field = view.aggregationField;
        mergeResult.gbCreateTable = view.SQLTable;
        mergeResult.viewName = view.name;
        let cacheSaveTimeStart = helper.time();
        cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
            helper.log('error:' + err);
            return callback(err);
        }).on('receipt', (receipt) => {
            let cacheSaveTimeEnd = helper.time();
            delete mergeResult.gbCreateTable;
            let timesReady = {};
            helper.log('receipt:' + JSON.stringify(receipt));
            timesReady.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime + times.getAllGBsTime + times.getLatestFactIdTime;
            timesReady.sqlTime = (mergeTimeEnd - mergeTimeStart) + (times.sqlTimeEnd - times.sqlTimeStart);
            timesReady.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
            timesReady.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
            timesReady.totalTime = timesReady.bcTime + timesReady.sqlTime + timesReady.cacheSaveTime + timesReady.cacheRetrieveTime;
            timesReady.totalStart = times.totalStart;
            clearCacheIfNeeded(sortedByEvictionCost, mergeResult, timesReady, function (err, results) {
                if (!err) {
                    helper.printTimes(mergeResult);
                    return callback(null, results);
                }
                return callback(err);
            });
        });
    });
}

function calculateNewGroupByFromBeginning (view, totalStart, getGroupIdTime, sortedByEvictionCost, callback) {
    let bcTimeStart = helper.time();
    contractController.getLatestId(function (err, latestId) {
        if (err) throw err;
        contractController.getAllFactsHeavy(latestId).then(retval => {
            let bcTimeEnd = helper.time();
            if (retval.length === 0) {
                return callback({ error: 'No facts exist in blockchain' }, null);
            }
            let facts = helper.removeTimestamps(retval);
            helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
            let sqlTimeStart = helper.time();
            computationsController.calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                let sqlTimeEnd = helper.time();
                if (error) {
                    return callback(error, null);
                }
                groupBySqlResult.gbCreateTable = view.SQLTable;
                groupBySqlResult.field = view.aggregationField;
                groupBySqlResult.viewName = view.name;
                if (config.cacheEnabled) {
                    let cacheSaveTimeStart = helper.time();
                    cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                        helper.log('error:', err);
                        return callback(err, null);
                    }).on('receipt', (receipt) => {
                        let cacheSaveTimeEnd = helper.time();
                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                        delete groupBySqlResult.gbCreateTable;
                        helper.log('receipt:' + JSON.stringify(receipt));
                        let times = { sqlTimeEnd: sqlTimeEnd, sqlTimeStart: sqlTimeStart,
                            bcTimeStart: bcTimeStart, bcTimeEnd: bcTimeEnd,
                            getGroupIdTime: getGroupIdTime, totalStart: totalStart };
                        clearCacheIfNeeded(sortedByEvictionCost, groupBySqlResult, times, function (err, results) {
                            if(err){
                                return callback(err);
                            }
                            helper.printTimes(results);
                            return callback(null, results);
                        });
                    });
                } else {
                    let totalEnd = helper.time();
                    groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                    groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                    groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime;
                    groupBySqlResult.allTotal = totalEnd - totalStart;
                    helper.printTimes(groupBySqlResult);
                    return callback(null, groupBySqlResult);
                }
            });
        });
    });
}

function clearCacheIfNeeded (sortedByEvictionCost, groupBySqlResult, times, callback) {
    if (sortedByEvictionCost.length > 0 && sortedByEvictionCost.length >= config.maxCacheSize) {
        contractController.deleteCachedResults(sortedByEvictionCost, function (err) {
            times.totalEnd = helper.time();
            if (!err) {
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                return callback(null, groupBySqlResult);
            } else {
                return callback(err);
            }
        });
    } else {
        if (times) {
            times.totalEnd = helper.time();
            groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
        }
        return callback(null, groupBySqlResult);
    }
}

function calculateFromCache (cachedGroupBy, sortedByEvictionCost, view, gbFields, latestId, times, callback) {
    if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
        // this means we want to calculate a different group by than the stored one
        // but however it can be calculated just from redis cache
        if (cachedGroupBy.field === view.aggregationField &&
            view.operation === cachedGroupBy.operation) {
            reduceGroupByFromCache(cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId, function (error, results) {
                if (error) {
                    return callback(error);
                }
                return callback(null, results);
            });
        } else {
            // some fields contained in a Group by but operation and aggregation fields differ
            // this means we should proceed to new group by calculation from the begining
            calculateNewGroupByFromBeginning(view, times.totalStart, times.getGroupIdTime, sortedByEvictionCost, function (error, result) {
                if (error) {
                    return callback(error);
                }
                return callback(null, result);
            });
        }
    } else {
        if (cachedGroupBy.field === view.aggregationField &&
            view.operation === cachedGroupBy.operation) {
            let totalEnd = helper.time();
            // this means we just have to return the group by stored in cache
            // field, operation are same and no new records written
            cachedGroupBy.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
            cachedGroupBy.totalTime = cachedGroupBy.cacheRetrieveTime;
            cachedGroupBy.allTotal = totalEnd - times.totalStart;
            return callback(null, cachedGroupBy);
        } else {
            // same fields but different operation or different aggregate field
            // this means we should proceed to new group by calculation from the begining
            calculateNewGroupByFromBeginning(view, times.totalStart, times.getGroupIdTime, sortedByEvictionCost, function (error, result) {
                if (error) {
                    return callback(error);
                }
                return callback(null, result);
            });
        }
    }
}

module.exports = {
    setContract: setContract,
    calculateNewGroupByFromBeginning: calculateNewGroupByFromBeginning,
    mergeCachedWithDeltasResultsSameFields: mergeCachedWithDeltasResultsSameFields,
    calculateFromCache: calculateFromCache
};
