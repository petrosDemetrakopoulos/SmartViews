let contract = null;
let mainTransactionObject = {};
const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const contractController = require('./contractController');
const computationsController = require('./computationsController');
const transformations = require('../helpers/transformations');
let config = require('../config_private');

function setContract (contractObject, account) {
    contract = contractObject;
    mainTransactionObject = helper.getMainTransactionObject(account);
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
            reducedResult = transformations.transformReadyAverage(reducedResult, view.gbFields, view.aggregationField);
        } else {
            reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
        }
        reducedResult.field = view.aggregationField;
        reducedResult.viewName = view.name;
        reducedResult.operation = view.operation;

        let cacheSaveTimeStart = helper.time();
        return cacheController.saveOnCache(reducedResult, view.operation, latestId - 1).on('error', (err) => {
            helper.log('error:', err);
            return callback(err);
        }).on('receipt', (receipt) => {
            helper.log('receipt:' + JSON.stringify(receipt));
            let cacheSaveTimeEnd = helper.time();
            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                contractController.deleteCachedResults(sortedByEvictionCost, function (err) {
                    let totalEnd = helper.time();
                    if (!err) {
                        reducedResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                        reducedResult.sqlTime = reductionTimeEnd - reductionTimeStart;
                        reducedResult.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                        reducedResult.totalTime = reducedResult.cacheSaveTime + reducedResult.sqlTime + reducedResult.cacheRetrieveTime;
                        reducedResult.allTotal = totalEnd - times.totalStart;
                        helper.printTimes(reducedResult);
                        callback(null, reducedResult);
                    }
                    return callback(err);
                });
            } else {
                let totalEnd = helper.time();
                reducedResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                reducedResult.sqlTime = reductionTimeEnd - reductionTimeStart;
                reducedResult.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                reducedResult.totalTime = reducedResult.cacheSaveTime + reducedResult.sqlTime + reducedResult.cacheRetrieveTime;
                reducedResult.allTotal = totalEnd - times.totalStart;
                helper.printTimes(reducedResult);
                return callback(null, reducedResult);
            }
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
            mergeResult.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime + times.getAllGBsTime + times.getLatestFactIdTime;
            mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (times.sqlTimeEnd - times.sqlTimeStart);
            mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
            mergeResult.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
            mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                contractController.deleteCachedResults(sortedByEvictionCost, function (err) {
                    let totalEnd = helper.time();
                    if (!err) {
                        mergeResult.allTotal = totalEnd - times.totalStart;
                        helper.printTimes(mergeResult);
                        helper.log('receipt:' + JSON.stringify(receipt));
                        return callback(null, mergeResult);
                    }
                    return callback(err);
                });
            } else {
                let totalEnd = helper.time();
                mergeResult.allTotal = totalEnd - times.totalStart;
                helper.printTimes(mergeResult);
                helper.log('receipt:' + JSON.stringify(receipt));
                return callback(null, mergeResult);
            }
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
                        if (sortedByEvictionCost.length > 0 && sortedByEvictionCost.length >= config.maxCacheSize) {
                            contractController.deleteCachedResults(sortedByEvictionCost, function (err) {
                                let totalEnd = helper.time();
                                if (!err) {
                                    groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                    groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                                    groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                    groupBySqlResult.allTotal = totalEnd - totalStart;
                                    helper.printTimes(groupBySqlResult);
                                    return callback(null, groupBySqlResult);
                                } else {
                                    return callback(err);
                                }
                            });
                        } else {
                            let totalEnd = helper.time();
                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                            groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                            groupBySqlResult.allTotal = totalEnd - totalStart;
                            helper.printTimes(groupBySqlResult);
                            return callback(null, groupBySqlResult);
                        }
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

module.exports = {
    setContract: setContract,
    calculateNewGroupByFromBeginning: calculateNewGroupByFromBeginning,
    mergeCachedWithDeltasResultsSameFields: mergeCachedWithDeltasResultsSameFields,
    reduceGroupByFromCache: reduceGroupByFromCache
};
