const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const contractController = require('./contractController');
const computationsController = require('./computationsController');
const transformations = require('../helpers/transformations');
let config = require('../config_private');

function setContract (contractObject, account) {
    cacheController.setContract(contractObject, account);
}

function calculateForDeltasAndMergeWithCached(mostEfficient, latestId, createTable, view, gbFields,
                                              sortedByEvictionCost, globalAllGroupBysTime,
                                              getLatestFactIdTime, totalStart) {
    return new Promise((resolve, reject) => {
        let bcTimeStart = helper.time();
        contractController.getFactsFromTo(mostEfficient.latestFact, latestId - 1).then(async deltas => {
            let bcTimeEnd = helper.time();
            await computationsController.executeQuery(createTable).then(async results => {
                deltas = helper.removeTimestamps(deltas);
                helper.log('CALCULATING GROUP-BY FOR DELTAS:');
                let sqlTimeStart = helper.time();
                await computationsController.calculateNewGroupBy(deltas, view.operation, view.gbFields, view.aggregationField).then(async groupBySqlResult => {
                    let sqlTimeEnd = helper.time();
                    let allHashes = helper.reconstructSlicedCachedResult(mostEfficient);
                    let cacheRetrieveTimeStart = helper.time();
                    await cacheController.getManyCachedResults(allHashes).then(async allCached => {
                        let cacheRetrieveTimeEnd = helper.time();

                        let cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);

                        if (cachedGroupBy.field === view.aggregationField &&
                            view.operation === cachedGroupBy.operation) {
                            if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                let reductionTimeStart = helper.time();
                                await computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields).then(async reducedResult => {
                                    let reductionTimeEnd = helper.time();

                                    let viewMeta = helper.extractViewMeta(view);
                                    // MERGE reducedResult with groupBySQLResult
                                    reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
                                    reducedResult.field = view.aggregationField;
                                    reducedResult.viewName = view.name;
                                    let rows = helper.extractGBValues(reducedResult, view);
                                    let rowsDelta = helper.extractGBValues(groupBySqlResult, view);

                                    let mergeTimeStart = helper.time();
                                    await computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable,
                                        viewMeta.viewNameSQL, view, viewMeta.lastCol, viewMeta.prelastCol).then(mergeResult => {
                                        let mergeTimeEnd = helper.time();
                                        mergeResult.operation = view.operation;
                                        mergeResult.field = view.aggregationField;
                                        mergeResult.gbCreateTable = view.SQLTable;
                                        mergeResult.viewName = view.name;
                                        // save on cache before return
                                        let cacheSaveTimeStart = helper.time();
                                        cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                            helper.log('error:' + err);
                                            reject(err);
                                        }).on('receipt', async (receipt) => {
                                            let cacheSaveTimeEnd = helper.time();
                                            delete mergeResult.gbCreateTable;
                                            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                await contractController.deleteCachedResults(sortedByEvictionCost).then(receiptDelete => {
                                                    let totalEnd = helper.time();
                                                    let sqlTime = (sqlTimeEnd - sqlTimeStart);
                                                    let reductionTime = (reductionTimeEnd - reductionTimeStart);
                                                    let mergeTime = (mergeTimeEnd - mergeTimeStart);
                                                    let bcTime = (bcTimeEnd - bcTimeStart);
                                                    mergeResult.sqlTime = sqlTime  + reductionTime + mergeTime;
                                                    mergeResult.bcTime = bcTime + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime;
                                                    mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                    mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                    mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                    mergeResult.allTotal = totalEnd - totalStart;
                                                    helper.printTimes(mergeResult);
                                                    helper.log('receipt:' + JSON.stringify(receipt));
                                                    resolve(mergeResult);
                                                }).catch(err => {
                                                    reject(err);
                                                });
                                            } else {
                                                let totalEnd = helper.time();
                                                let sqlTime = (sqlTimeEnd - sqlTimeStart);
                                                let reductionTime = (reductionTimeEnd - reductionTimeStart);
                                                let mergeTime = (mergeTimeEnd - mergeTimeStart);
                                                let bcTime = (bcTimeEnd - bcTimeStart);
                                                mergeResult.sqlTime = sqlTime + reductionTime + mergeTime;
                                                mergeResult.bcTime = bcTime + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime;
                                                mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                mergeResult.allTotal = totalEnd - totalStart;
                                                helper.printTimes(mergeResult);
                                                helper.log('receipt:' + JSON.stringify(receipt));
                                                resolve(mergeResult);
                                            }
                                        });
                                    }).catch(err => {
                                        helper.log(err);
                                        reject(err);
                                    });
                                }).catch(err => {
                                    helper.log(err);
                                    reject(err);
                                });
                            } else {
                                console.log('GROUP-BY FIELDS OF DELTAS AND CACHED ARE THE SAME');
                                // group by fields of deltas and cached are the same so
                                // MERGE cached and groupBySqlResults
                                let times = { bcTimeEnd: bcTimeEnd,
                                    bcTimeStart: bcTimeStart,
                                    getGroupIdTime: globalAllGroupBysTime.getGroupIdTime,
                                    getAllGBsTime: globalAllGroupBysTime.getAllGBsTime,
                                    getLatestFactIdTime: getLatestFactIdTime,
                                    sqlTimeEnd: sqlTimeEnd,
                                    sqlTimeStart: sqlTimeStart,
                                    cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                    cacheRetrieveTimeStart: cacheRetrieveTimeStart,
                                    totalStart: totalStart };

                                mergeCachedWithDeltasResultsSameFields(view, cachedGroupBy,
                                    groupBySqlResult, latestId, sortedByEvictionCost, times).then(result =>  {
                                    resolve(result);
                                }).catch(err => {
                                    helper.log(err);
                                    reject(err);
                                });
                            }
                        }
                    }).catch(err => {
                        helper.log(err);
                        reject(err);
                    });
                }).catch(err => {
                    helper.log(err);
                    reject(err);
                });
            }).catch(err => {
                helper.log(err);
                reject(err);
            });
        });
    });
}

function reduceGroupByFromCache (cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId) {
    return new Promise((resolve, reject) => {
        let reductionTimeStart = helper.time();
        computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields).then(async reducedResult => {
            let reductionTimeEnd = helper.time();

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
                reject(err);
            }).on('receipt', (receipt) => {
                helper.log('receipt:' + JSON.stringify(receipt));
                let cacheSaveTimeEnd = helper.time();
                let times2 = { sqlTimeEnd: reductionTimeEnd, sqlTimeStart: reductionTimeStart,
                    totalStart: times.totalStart, cacheSaveTimeStart: cacheSaveTimeStart,
                    cacheSaveTimeEnd: cacheSaveTimeEnd, cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                    cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd };
                clearCacheIfNeeded(sortedByEvictionCost, reducedResult, times2).then(results => {
                    helper.printTimes(results);
                    resolve(results);
                }).catch(err => {
                    console.log(err);
                    reject(err);
                });
            });
        }).catch(err => {
            console.log(err);
            reject(err);
        });
    });
}

function mergeCachedWithDeltasResultsSameFields(view, cachedGroupBy, groupBySqlResult, latestId, sortedByEvictionCost, times) {
    return new Promise((resolve, reject) => {
        let viewMeta = helper.extractViewMeta(view);
        let rows = helper.extractGBValues(cachedGroupBy, view);
        let rowsDelta = helper.extractGBValues(groupBySqlResult, view);
        let mergeTimeStart = helper.time();
        computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewMeta.viewNameSQL, view, viewMeta.lastCol, viewMeta.prelastCol).then(mergeResult => {
            let mergeTimeEnd = helper.time();
            // SAVE ON CACHE BEFORE RETURN
            helper.log('SAVE ON CACHE BEFORE RETURN');
            mergeResult.operation = view.operation;
            mergeResult.field = view.aggregationField;
            mergeResult.gbCreateTable = view.SQLTable;
            mergeResult.viewName = view.name;
            let cacheSaveTimeStart = helper.time();
            cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                helper.log('error:' + err);
                reject(err);
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
                clearCacheIfNeeded(sortedByEvictionCost, mergeResult, timesReady).then(results =>  {
                    helper.printTimes(mergeResult);
                    resolve(results);
                }).catch(err => {
                    reject(err);
                });
            });
        }).catch(err => {
            reject(err);
        });
    });
}

function calculateNewGroupByFromBeginning (view, totalStart, getGroupIdTime, sortedByEvictionCost) {
    return new Promise((resolve, reject) => {
        let bcTimeStart = helper.time();
        contractController.getLatestId().then(latestId => {
            contractController.getAllFactsHeavy(latestId).then(retval => {
                let bcTimeEnd = helper.time();
                if (retval.length === 0) {
                    reject({ error: 'No facts exist in blockchain' });
                }
                let facts = helper.removeTimestamps(retval);
                helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                let sqlTimeStart = helper.time();
                computationsController.calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField).then(groupBySqlResult  => {
                    let sqlTimeEnd = helper.time();
                    groupBySqlResult.gbCreateTable = view.SQLTable;
                    groupBySqlResult.field = view.aggregationField;
                    groupBySqlResult.viewName = view.name;
                    if (config.cacheEnabled) {
                        let cacheSaveTimeStart = helper.time();
                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                            helper.log('error:', err);
                            reject(err);
                        }).on('receipt', (receipt) => {
                            let cacheSaveTimeEnd = helper.time();
                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                            delete groupBySqlResult.gbCreateTable;
                            helper.log('receipt:' + JSON.stringify(receipt));
                            let times = { sqlTimeEnd: sqlTimeEnd, sqlTimeStart: sqlTimeStart,
                                bcTimeStart: bcTimeStart, bcTimeEnd: bcTimeEnd,
                                getGroupIdTime: getGroupIdTime, totalStart: totalStart };
                            clearCacheIfNeeded(sortedByEvictionCost, groupBySqlResult, times).then(results => {
                                helper.printTimes(results);
                                resolve(results);
                            }).catch(err => {
                                reject(err);
                            })
                        });
                    } else {
                        let totalEnd = helper.time();
                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                        groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime;
                        groupBySqlResult.allTotal = totalEnd - totalStart;
                        helper.printTimes(groupBySqlResult);
                        resolve(groupBySqlResult);
                    }
                }).catch(err => {
                    throw err;
                });
            });
        }).catch(err => {
            throw err;
        });
    });
}

function clearCacheIfNeeded (sortedByEvictionCost, groupBySqlResult, times) {
    return new Promise((resolve, reject) => {
        if (sortedByEvictionCost.length > 0 && sortedByEvictionCost.length >= config.maxCacheSize) {
            contractController.deleteCachedResults(sortedByEvictionCost).then(deleteReceipt => {
                times.totalEnd = helper.time();
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                resolve(groupBySqlResult);
            }).catch(err => {
                reject(err);
            });
        } else {
            if (times) {
                times.totalEnd = helper.time();
                groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
            }
            resolve(groupBySqlResult);
        }
    });
}

function calculateFromCache (cachedGroupBy, sortedByEvictionCost, view, gbFields, latestId, times) {
    return new Promise(async (resolve, reject) => {
        console.log("calculate from cache start");
        if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
            // this means we want to calculate a different group by than the stored one
            // but however it can be calculated just from redis cache
            if (cachedGroupBy.field === view.aggregationField &&
                view.operation === cachedGroupBy.operation) {
                return await reduceGroupByFromCache(cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId).then(results => {
                    return resolve(results);
                }).catch(err => {
                    return reject(err);
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
                return resolve(cachedGroupBy);
            }
        }
        console.log("fucking calculating new gb!!!!");
        calculateNewGroupByFromBeginning(view, times.totalStart, times.getGroupIdTime, sortedByEvictionCost).then(result => {
            return resolve(result);
        }).catch(err => {
            return reject(err);
        });
    });
}

module.exports = {
    setContract: setContract,
    calculateNewGroupByFromBeginning: calculateNewGroupByFromBeginning,
    calculateFromCache: calculateFromCache,
    calculateForDeltasAndMergeWithCached:calculateForDeltasAndMergeWithCached
};
