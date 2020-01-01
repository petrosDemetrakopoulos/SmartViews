const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const contractController = require('./contractController');
const computationsController = require('./computationsController');
const transformations = require('../helpers/transformations');
const stringify = require('fast-stringify');
let config = require('../config_private');
const fs = require('fs');

function setContract (contractObject, account) {
    cacheController.setContract(contractObject, account);
}

function calculateForDeltasAndMergeWithCached (mostEfficient, latestId, createTable,
    view, gbFields, sortedByEvictionCost,
    globalAllGroupBysTime, getLatestFactIdTime,
    totalStart) {
    return new Promise((resolve, reject) => {
        let matSteps = [];
        let bcTimeStart = helper.time();
        contractController.getFactsFromTo(mostEfficient.latestFact, latestId - 1).then(async deltas => {
            let bcTimeEnd = helper.time();
            matSteps.push({ type: 'bcFetchDeltas', numOfFacts: deltas.length });
            await computationsController.executeQuery(createTable).then(async results => {
                deltas = helper.removeTimestamps(deltas);
                helper.log('CALCULATING GROUP-BY FOR DELTAS:');
                let sqlTimeStart = helper.time();
                await computationsController.calculateNewGroupBy(deltas, view.operation, view.gbFields, view.aggregationField).then(async groupBySqlResult => {
                    let sqlTimeEnd = helper.time();
                    let allHashes = helper.reconstructSlicedCachedResult(mostEfficient);
                    matSteps.push({ type: 'sqlCalculationDeltas' });
                    let cacheRetrieveTimeStart = helper.time();
                    await cacheController.getManyCachedResults(allHashes).then(async allCached => {
                        let cacheRetrieveTimeEnd = helper.time();
                        matSteps.push({ type: 'cacheFetch' });
                        let cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);

                        if (cachedGroupBy.field === view.aggregationField &&
                            view.operation === cachedGroupBy.operation) {
                            if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                let reductionTimeStart = helper.time();
                                await computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields).then(async reducedResult => {
                                    let reductionTimeEnd = helper.time();
                                    matSteps.push({ type: 'sqlReduction', from: cachedGroupBy.groupByFields, to: gbFields });
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
                                        matSteps.push({ type: 'sqlMergeReducedCachedWithDeltas' });
                                        mergeResult.operation = view.operation;
                                        mergeResult.field = view.aggregationField;
                                        mergeResult.gbCreateTable = view.SQLTable;
                                        mergeResult.viewName = view.name;
                                        // save on cache before return
                                        let gbSize = stringify(mergeResult).length;
                                        if (gbSize / 1024 <= config.maxCacheSizeInKB) {
                                            let cacheSaveTimeStart = helper.time();
                                            cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                helper.log('error:' + err);
                                                reject(err);
                                            }).on('receipt', async (receipt) => {
                                                let cacheSaveTimeEnd = helper.time();
                                                matSteps.push({ type: 'cacheSave' });
                                                delete mergeResult.gbCreateTable;

                                                let times = { bcTime: (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime,
                                                    sqlTime: (mergeTimeEnd - mergeTimeStart) + (reductionTimeEnd - reductionTimeStart),
                                                    cacheRetrieveTime: cacheRetrieveTimeEnd - cacheRetrieveTimeStart,
                                                    cacheSaveTime: cacheSaveTimeEnd - cacheSaveTimeStart,
                                                    totalStart: totalStart };
                                                times.totalTime = times.bcTime + times.sqlTime + times.cacheRetrieveTime + times.cacheSaveTime;
                                                let sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                                                helper.log('receipt:' + JSON.stringify(receipt));
                                                clearCacheIfNeeded(sortedByEvictionCost, mergeResult, sameOldestResults, times).then(results => {
                                                    helper.printTimes(results);
                                                    results.matSteps = matSteps;
                                                    resolve(results);
                                                }).catch(err => {
                                                    reject(err);
                                                });
                                            });
                                        } else {
                                            let totalEnd = helper.time();
                                            let sqlTime = (sqlTimeEnd - sqlTimeStart);
                                            let reductionTime = (reductionTimeEnd - reductionTimeStart);
                                            let mergeTime = (mergeTimeEnd - mergeTimeStart);
                                            let bcTime = (bcTimeEnd - bcTimeStart);
                                            mergeResult.sqlTime = sqlTime + reductionTime + mergeTime;
                                            mergeResult.bcTime = bcTime + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime;
                                            mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                            mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                            mergeResult.allTotal = totalEnd - totalStart;
                                            mergeResult.matSteps = matSteps;
                                            helper.printTimes(mergeResult);
                                            resolve(mergeResult);
                                        }
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
                                    groupBySqlResult, latestId, sortedByEvictionCost, times).then(result => {
                                    matSteps.push({ type: 'sqlMergeCachedWithDeltas' });
                                    result.matSteps = matSteps;
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
            let gbSize = stringify(reducedResult).length;
            if (gbSize / 1024 <= config.maxCacheSizeInKB) {
                let cacheSaveTimeStart = helper.time();
                cacheController.saveOnCache(reducedResult, view.operation, latestId - 1).on('error', (err) => {
                    helper.log('error:', err);
                    reject(err);
                }).on('receipt', (receipt) => {
                    helper.log('receipt:' + JSON.stringify(receipt));
                    let cacheSaveTimeEnd = helper.time();
                    let times2 = {
                        sqlTimeEnd: reductionTimeEnd,
                        sqlTimeStart: reductionTimeStart,
                        totalStart: times.totalStart,
                        cacheSaveTimeStart: cacheSaveTimeStart,
                        cacheSaveTimeEnd: cacheSaveTimeEnd,
                        cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                        cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd
                    };
                    let sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                    clearCacheIfNeeded(sortedByEvictionCost, reducedResult, sameOldestResults, times2).then(results => {
                        helper.printTimes(results);
                        resolve(results);
                    }).catch(err => {
                        console.log(err);
                        reject(err);
                    });
                });
            } else {
                let times2 = {
                    sqlTimeEnd: reductionTimeEnd,
                    sqlTimeStart: reductionTimeStart,
                    totalStart: times.totalStart,
                    cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                    cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd
                };
                reducedResult = helper.assignTimes(reducedResult, times2);
                helper.printTimes(reducedResult);
                resolve(reducedResult);
            }
        }).catch(err => {
            console.log(err);
            reject(err);
        });
    });
}

function mergeCachedWithDeltasResultsSameFields (view, cachedGroupBy, groupBySqlResult,
    latestId, sortedByEvictionCost, times) {
    return new Promise((resolve, reject) => {
        let viewMeta = helper.extractViewMeta(view);
        let rows = helper.extractGBValues(cachedGroupBy, view);
        let rowsDelta = helper.extractGBValues(groupBySqlResult, view);
        let mergeTimeStart = helper.time();
        computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewMeta.viewNameSQL,
            view, viewMeta.lastCol, viewMeta.prelastCol).then(mergeResult => {
            let mergeTimeEnd = helper.time();
            // SAVE ON CACHE BEFORE RETURN
            helper.log('SAVE ON CACHE BEFORE RETURN');
            mergeResult.operation = view.operation;
            mergeResult.field = view.aggregationField;
            mergeResult.gbCreateTable = view.SQLTable;
            mergeResult.viewName = view.name;
            let gbSize = stringify(mergeResult).length;
            if (gbSize / 1024 <= config.maxCacheSizeInKB) {
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
                    //find from sortedByEvictionCost any cached result that is exactly the same with the one requested
                    //then add it to a separate array and delete it anyway independently to if they are already in sortedByEvictionCost
                    let sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                    clearCacheIfNeeded(sortedByEvictionCost, mergeResult, sameOldestResults, timesReady).then(results => {
                        helper.printTimes(mergeResult);
                        resolve(results);
                    }).catch(err => {
                        reject(err);
                    });
                });
            } else {
                delete mergeResult.gbCreateTable;
                let timesReady = {};
                timesReady.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime + times.getAllGBsTime + times.getLatestFactIdTime;
                timesReady.sqlTime = (mergeTimeEnd - mergeTimeStart) + (times.sqlTimeEnd - times.sqlTimeStart);
                timesReady.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                timesReady.totalTime = timesReady.bcTime + timesReady.sqlTime + timesReady.cacheSaveTime + timesReady.cacheRetrieveTime;
                timesReady.totalStart = times.totalStart;
                timesReady.totalEnd = helper.time();
                mergeResult = helper.assignTimes(mergeResult, timesReady);
                helper.printTimes(mergeResult);
                resolve(mergeResult);
            }
        }).catch(err => {
            reject(err);
        });
    });
}

function calculateNewGroupByFromBeginning (view, totalStart, getGroupIdTime, sortedByEvictionCost) {
    return new Promise((resolve, reject) => {
        let matSteps = [];
        let bcTimeStart = helper.time();
        contractController.getLatestId().then(latestId => {
            contractController.getAllFactsHeavy(latestId).then(retval => {
                let bcTimeEnd = helper.time();
                if (retval.length === 0) {
                    return reject(new Error('No facts exist in blockchain'));
                }
                matSteps.push({ type: 'bcFetch', numOfFacts: retval.length });
                let facts = helper.removeTimestamps(retval);
                helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                let sqlTimeStart = helper.time();
                computationsController.calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField).then(groupBySqlResult => {
                    matSteps.push({ type: 'sqlCalculationInitial' });
                    let sqlTimeEnd = helper.time();
                    groupBySqlResult.gbCreateTable = view.SQLTable;
                    groupBySqlResult.field = view.aggregationField;
                    groupBySqlResult.viewName = view.name;
                    let gbSize = stringify(groupBySqlResult).length;
                    if (config.cacheEnabled && ((gbSize / 1024) <= config.maxCacheSizeInKB)) {
                        let cacheSaveTimeStart = helper.time();
                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                            helper.log('error:', err);
                            reject(err);
                        }).on('receipt', (receipt) => {
                            matSteps.push({ type: 'cacheSave' });
                            let cacheSaveTimeEnd = helper.time();
                            delete groupBySqlResult.gbCreateTable;
                            helper.log('receipt:' + JSON.stringify(receipt));
                            let times = { sqlTime: sqlTimeEnd - sqlTimeStart,
                                bcTime: (bcTimeEnd - bcTimeStart) + getGroupIdTime,
                                cacheSaveTime: cacheSaveTimeEnd - cacheSaveTimeStart,
                                totalStart: totalStart };
                            times.totalTime = times.bcTime + times.sqlTime + times.cacheSaveTime;
                            let sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                            clearCacheIfNeeded(sortedByEvictionCost, groupBySqlResult, sameOldestResults, times).then(results => {
                                helper.printTimes(results);
                                results.matSteps = matSteps;
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
                        groupBySqlResult.matSteps = matSteps;
                        helper.printTimes(groupBySqlResult);
                        resolve(groupBySqlResult);
                    }
                }).catch(err => {
                    console.log(err);
                    throw err;
                });
            });
        }).catch(err => {
            console.log(err);
            throw err;
        });
    });
}

function clearCacheIfNeeded (sortedByEvictionCost, groupBySqlResult, sameOldestResults, times) {
    return new Promise((resolve, reject) => {
        let totalCurrentCacheLoad = 0; // in Bytes
        for (let i = 0; i < sortedByEvictionCost.length; i++) {
            totalCurrentCacheLoad += parseInt(sortedByEvictionCost[i].size);
        }
        console.log('CURRENT CACHE LOAD = ' + totalCurrentCacheLoad + ' Bytes OR ' + (totalCurrentCacheLoad / 1024) + ' KB');
        if (totalCurrentCacheLoad > 0 && (totalCurrentCacheLoad / 1024) >= config.maxCacheSizeInKB) {
            // delete as many cached results as to free up cache size equal to the size of the latest result we computed
            // we can easily multiply it by a factor to see how it performs
            console.log('-->CLEARING CACHE');
            let sortedByEvictionCostFiltered = [];
            let gbSize = stringify(groupBySqlResult).length;
            let totalSize = 0;
            let i = 0;
            for (let k = 0; k < sameOldestResults.length; k++) {
                let indexInSortedByEviction = sortedByEvictionCost.indexOf(sameOldestResults[k]);
                if (indexInSortedByEviction > -1) {
                    sortedByEvictionCost.splice(indexInSortedByEviction, 1);
                }
            }

            let crnSize = parseInt(sortedByEvictionCost[i].size);
            while ((totalSize + crnSize) < (config.maxCacheSizeInKB * 1024 - gbSize)) {
                totalSize += parseInt(sortedByEvictionCost[i].size);
                //sortedByEvictionCostFiltered.push(sortedByEvictionCost[i]);
                i++;
                if (sortedByEvictionCost[i]) {
                    crnSize = parseInt(sortedByEvictionCost[i].size);
                } else {
                    break;
                }
            }

            for (let k = 0; k < sameOldestResults.length; k++) {
                let indexInSortedByEviction = sortedByEvictionCost.indexOf(sameOldestResults[k]);
                if (indexInSortedByEviction > -1) {
                    totalSize += parseInt(sortedByEvictionCost[indexInSortedByEviction].size);
                    sortedByEvictionCost.splice(indexInSortedByEviction, 1);
                }
            }

            for (let k = 0; k < i; k++) {
                sortedByEvictionCostFiltered.push(sortedByEvictionCost[k]);
                console.log('Evicted view with size: ' + sortedByEvictionCost[k])
            }
            console.log('TOTAL SIZE = ' + totalSize);
            console.log('GB SIZE = ' + gbSize);
            let tot = (totalSize + gbSize);
            let res = tot.toString() + '\n';
            console.log('result to txt: ' + res);
            fs.appendFile('cache_sizeWV.txt', res, function (err) {
                if (err) {
                    return console.error(err);
                }
            });
            sortedByEvictionCostFiltered = sortedByEvictionCostFiltered.concat(sameOldestResults);
            contractController.deleteCachedResults(sortedByEvictionCostFiltered).then(deleteReceipt => {
                times.totalEnd = helper.time();
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                resolve(groupBySqlResult);
            }).catch(err => {
                reject(err);
            });
        } else {
            console.log('-->NOT CLEARING CACHE');
            times.totalEnd = helper.time();
            if (times) {
                groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
            }
            resolve(groupBySqlResult);
            /*
            if (sameOldestResults.length > 0) {
                contractController.deleteCachedResults(sameOldestResults).then(deleteReceipt => {
                    times.totalEnd = helper.time();
                    if (times) {
                        groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                    }
                    console.log('DELETED CACHED RESULTS');
                    resolve(groupBySqlResult);
                }).catch(err => {
                    reject(err);
                });
            } else {
                times.totalEnd = helper.time();
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                resolve(groupBySqlResult);
            }
             */
        }
    });
}

function calculateFromCache (cachedGroupBy, sortedByEvictionCost, view, gbFields, latestId, times, matSteps) {
    return new Promise(async (resolve, reject) => {
        if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
            // this means we want to calculate a different group by than the stored one
            // but however it can be calculated just from redis cache
            if (cachedGroupBy.field === view.aggregationField &&
                view.operation === cachedGroupBy.operation) {
                await reduceGroupByFromCache(cachedGroupBy, view, gbFields, sortedByEvictionCost,
                    times, latestId).then(results => {
                    matSteps.push({ type: 'sqlReduction', from: cachedGroupBy.groupByFields, to: gbFields });
                    results.matSteps = matSteps;
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
                cachedGroupBy.matSteps = matSteps;
                return resolve(cachedGroupBy);
            }
        }
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
    calculateForDeltasAndMergeWithCached: calculateForDeltasAndMergeWithCached
};
