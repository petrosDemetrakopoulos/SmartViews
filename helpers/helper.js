const config = require('../config_private');
const microtime = require('microtime');
const fs = require('fs');

function removeTimestamps (records) {
    for (let i = 0; i < records.length; i++) {
        delete records[i].timestamp;
    }
    return records;
}

function configFileValidations () {
    let missingFields = [];
    if (!config.hasOwnProperty('recordsSlice')) {
        missingFields.push('recordsSlice');
    }
    if (!config.hasOwnProperty('cacheEvictionPolicy')) {
        missingFields.push('cacheEvictionPolicy');
    }
    if (!config.hasOwnProperty('maxCacheSize')) {
        missingFields.push('maxCacheSize');
    }
    if (!config.hasOwnProperty('cacheSlice')) {
        missingFields.push('cacheSlice');
    }
    if (!config.hasOwnProperty('redisPort')) {
        missingFields.push('redisPort');
    }
    if (!config.hasOwnProperty('redisIP')) {
        missingFields.push('redisIP');
    }
    if (!config.hasOwnProperty('blockchainIP')) {
        missingFields.push('blockchainIP');
    }
    if (missingFields.length > 0) {
        return { passed: false, missingFields: missingFields };
    }
    let formatErrors = [];
    if (!Number.isInteger(config.recordsSlice)) {
        formatErrors.push({ field: 'recordsSlice', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.cacheSlice)) {
        formatErrors.push({ field: 'cacheSlice', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.maxCacheSize)) {
        formatErrors.push({ field: 'maxCacheSize', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.redisPort)) {
        formatErrors.push({ field: 'redisPort', error: 'Should be integer' });
    }
    if (config.cacheEvictionPolicy !== 'FIFO' &&
        config.cacheEvictionPolicy !== 'costFunction' &&
        config.cacheEvictionPolicy !== 'word2vec') {
        formatErrors.push({ field: 'cacheEvictionPolicy',
            error: 'Should be either \'FIFO\' or \'costFunction\' or \'word2vec\'' });
    }
    if ((typeof config.blockchainIP) !== 'string') {
        formatErrors.push({ field: 'blockchainIP', error: 'Should be string' });
    }
    if ((typeof config.redisIP) !== 'string') {
        formatErrors.push({ field: 'redisIP', error: 'Should be string' });
    }

    if (formatErrors.length > 0) {
        return { passed: false, formatErrors: formatErrors };
    }
    return { passed: true };
}

function printTimes (resultObject) {
    log('sql time = ' + resultObject.sqlTime);
    log('bc time = ' + resultObject.bcTime);
    log('cache save time = ' + resultObject.cacheSaveTime);
    if (resultObject.cacheRetrieveTime) {
        log('cache retrieve time = ' + resultObject.cacheRetrieveTime);
    }
    log('total time = ' + resultObject.totalTime);
    log('all total time = ' + resultObject.allTotal);
}

function containsAllFields (transformedArray, view) {
    for (let i = 0; i < transformedArray.length; i++) {
        let containsAllFields = true;
        let crnView = transformedArray[i];

        let cachedGBFields = JSON.parse(crnView.columns);
        for (let index in cachedGBFields.fields) {
            cachedGBFields.fields[index] = cachedGBFields.fields[index].trim();
        }
        for (let j = 0; j < view.gbFields.length; j++) {
            if (!cachedGBFields.fields.includes(view.gbFields[j])) {
                containsAllFields = false
            }
        }
        transformedArray[i].containsAllFields = containsAllFields;
    }
    return transformedArray;
}

function getRandomInt (min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
function getRandomFloat (min, max) {
    return (Math.random() * (max - min + 1) + min).toFixed(2);
}

function time () {
    return microtime.nowDouble();
}

function log (logString) {
    if (config.logging) {
        console.log(logString);
    }
}

function requireUncached (module) {
    delete require.cache[require.resolve(module)];
    return require(module);
}

function mergeSlicedCachedResult (allCached) {
    let mergedArray = [];
    for (const index in allCached) {
        let crnSub = allCached[index];
        let crnSubArray = JSON.parse(crnSub);
        for (const kv in crnSubArray) {
            if (kv !== 'operation' && kv !== 'groupByFields' && kv !== 'field' && kv !== 'viewName') {
                mergedArray.push(crnSubArray[kv]);
            } else {
                for (const meta in crnSubArray) {
                    mergedArray.push({ [meta]: crnSubArray[meta] });
                }
                break;
            }
        }
    }
    let gbFinal = {};
    for (const i in mergedArray) {
        let crnKey = Object.keys(mergedArray[i])[0];
        gbFinal[crnKey] = Object.values(mergedArray[i])[0];
    }
    return gbFinal;
}

function extractGBValues (reducedResult, view) {
    let rows = [];
    let gbValsReduced = Object.values(reducedResult);
    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4];
    lastCol = lastCol[lastCol.length - 2];

    for (let i = 0, keys = Object.keys(reducedResult); i < keys.length; i++) {
        let key = keys[i];
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
            let crnRow = JSON.parse(key);
            if (view.operation === 'AVERAGE') {
                crnRow[prelastCol] = gbValsReduced[i]['sum'];
                crnRow[lastCol] = gbValsReduced[i]['count'];
            } else {
                crnRow[lastCol] = gbValsReduced[i];
            }
            rows.push(crnRow);
        }
    }
    return rows;
}

function getJSONFiles (items) {
    let suffix = '.json';
    return items.filter(file => {
        return file.indexOf(suffix) !== -1; // filtering out non-json files
    });
}

function transformGBMetadataFromBlockchain (resultGB) {
    let len = Object.keys(resultGB).length;
    for (let j = 0; j < len / 2; j++) {
        delete resultGB[j];
    }
    let transformedArray = [];
    for (let j = 0; j < resultGB.hashes.length; j++) {
        transformedArray[j] = {
            hash: resultGB.hashes[j],
            latestFact: resultGB.latFacts[j],
            columnSize: resultGB.columnSize[j],
            columns: resultGB.columns[j],
            gbTimestamp: resultGB.gbTimestamp[j],
            size: resultGB.size[j],
            id: j
        };
    }
    // then we filter out the empty objects (the ones that are deleted from blockchain, however left with zeroes)
    // it is enough to check if the hash exists
    transformedArray = transformedArray.filter(gb => {
        return gb.hash.length > 0;
    });
    return transformedArray;
}

function updateViewFrequency (factTbl, contract, crnView) {
    return new Promise(function (resolve, reject) {
        factTbl.views[crnView].frequency = factTbl.views[crnView].frequency + 1;
        delete factTbl.views[crnView].id;
        fs.writeFile('./templates/' + contract + '.json', JSON.stringify(factTbl, null, 2), function (err) {
            if (err) {
                log(err);
                reject(err);
            } else {
                log('updated view frequency');
                resolve();
            }
        });
    });
}

function extractFields (view) {
    let fields = [];
    if (Array.isArray(view.fields)) {
        fields = view.fields;
    } else {
        fields.push(view.fields);
    }
    for (let index in fields) {
        fields[index] = fields[index].trim();
    }
    return fields;
}

function extractViewMeta (view) {
    let viewNameSQL = view.SQLTable.split(' ');
    viewNameSQL = viewNameSQL[3];
    viewNameSQL = viewNameSQL.split('(')[0];

    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4]; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
    lastCol = lastCol[lastCol.length - 2];

    let op = '';
    if (view.operation === 'SUM' || view.operation === 'COUNT') {
        op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
    } else {
        op = view.operation;
    }

    return { viewNameSQL: viewNameSQL, lastCol: lastCol, prelastCol: prelastCol, op: op };
}

function checkViewExists (viewsDefined, viewName, factTbl) {
    let view = {};
    for (let crnView in viewsDefined) {
        if (factTbl.views[crnView].name === viewName) {
            view = factTbl.views[crnView];
            view.id = crnView;
            break;
        }
    }
    return view;
}

function sanitizeSQLQuery (gbQuery) {
    let query = gbQuery.query.replace(/"/g, '');
    query = query.replace(/''/g, 'null');
    return query;
}

function filterGBs (resultGB, view) {
    let transformedArray = transformGBMetadataFromBlockchain(resultGB);
    transformedArray = containsAllFields(transformedArray, view); // assigns the containsAllFields value
    let filteredGBs = [];
    for (let i = 0; i < transformedArray.length; i++) { // filter out the group bys that DO NOT CONTAIN all the fields we need -> aka containsAllFields = false
        if (transformedArray[i].containsAllFields) { // BUG THERE: SHOULD CHECK FOR THE OPERATION TOO.
            filteredGBs.push(transformedArray[i]);
        }
    }
    return filteredGBs;
}

async function sortByEvictionCost (resultGB, latestId, view, factTbl) {
    let transformedArray = transformGBMetadataFromBlockchain(resultGB);
    transformedArray = containsAllFields(transformedArray, view); // assigns the containsAllFields value
    let sortedByEvictionCost = JSON.parse(JSON.stringify(transformedArray));
    return costFunctions.dispCost(sortedByEvictionCost, latestId, factTbl).then(async sortedByEvictionCost => {
        await sortedByEvictionCost.sort(function (a, b) {
            if (config.cacheEvictionPolicy === 'FIFO') {
                return parseInt(a.gbTimestamp) - parseInt(b.gbTimestamp);
            } else if (config.cacheEvictionPolicy === 'costFunction') {
                return parseFloat(a.cacheEvictionCost) - parseFloat(b.cacheEvictionCost);
            } else if (config.cacheEvictionPolicy === 'word2vec') {
                return parseFloat(b.word2vecScore) - parseFloat(a.word2vecScore);
            }
        });
        return sortedByEvictionCost;
    });
}

async function sortByCalculationCost (resultGBs, latestId, view) {
    if (config.calculationCostFunction.toLowerCase() === 'costfunction') {
        resultGBs = costFunctions.calculationCostOfficial(resultGBs, latestId); // the cost to materialize the view from each view cached
        await resultGBs.sort(function (a, b) {
            return parseFloat(a.calculationCost) - parseFloat(b.calculationCost)
        }); // order ascending
    }
    return resultGBs;
}

async function sortByWord2Vec (resultGBs, view) {
    if(resultGBs.length > 1) {
        resultGBs = await costFunctions.word2vec(resultGBs, view);
        await resultGBs.sort(function (a, b) {
            return parseFloat(b.word2vecScore) - parseFloat(a.word2vecScore);
        });
    }
    return resultGBs;
}

function reconstructSlicedCachedResult (cachedGB) {
    let hashId = cachedGB.hash.split('_')[1];
    let hashBody = cachedGB.hash.split('_')[0];
    let allHashes = [];
    for (let i = 0; i <= hashId; i++) {
        allHashes.push(hashBody + '_' + i);
    }
    return allHashes;
}

function getMainTransactionObject (account) {
    return {
        from: account,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
}

function assignTimes (result, times) {
    if (times.bcTime && times.sqlTime && times.cacheRetrieveTime && times.cacheSaveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheRetrieveTime = times.cacheRetrieveTime;
        result.cacheSaveTime = times.cacheSaveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    if (times.bcTime && times.sqlTime && times.cacheRetrieveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheRetrieveTime = times.cacheRetrieveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    if (times.bcTime && times.sqlTime && times.cacheSaveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheSaveTime = times.cacheSaveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    result.sqlTime = times.sqlTimeEnd - times.sqlTimeStart;
    result.totalTime = result.sqlTime;
    if (times.bcTimeEnd && times.bcTimeStart && times.getGroupIdTime !== null && times.getGroupIdTime !== undefined) {
        result.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime;
        result.totalTime += result.bcTime;
    }
    if (times.cacheSaveTimeStart && times.cacheSaveTimeEnd) {
        result.cacheSaveTime = times.cacheSaveTimeEnd - times.cacheSaveTimeStart;
        result.totalTime += result.cacheSaveTime;
    }
    if (times.cacheRetrieveTimeStart && times.cacheRetrieveTimeEnd) {
        result.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
        result.totalTime += result.cacheRetrieveTime;
    }
    console.log(times);
    result.allTotal = times.totalEnd - times.totalStart;
    return result;
}

function findSameOldestResults (sortedByEvictionCost, view) {
    let sameOldestResults = [];
    for (let i = 0; i < sortedByEvictionCost.length; i++) {
        let crnRes = sortedByEvictionCost[i];
        let meta = JSON.parse(crnRes.columns);
        if (JSON.stringify(meta.fields) === JSON.stringify(view.gbFields) && meta.aggrFunc === view.operation) {
            sameOldestResults.push(crnRes);
        }
    }
    return sameOldestResults;
}

module.exports = {
    containsAllFields: containsAllFields,
    configFileValidations: configFileValidations,
    removeTimestamps: removeTimestamps,
    printTimes: printTimes,
    getRandomInt: getRandomInt,
    getRandomFloat: getRandomFloat,
    time: time,
    log: log,
    requireUncached: requireUncached,
    mergeSlicedCachedResult: mergeSlicedCachedResult,
    extractGBValues: extractGBValues,
    getJSONFiles: getJSONFiles,
    updateViewFrequency: updateViewFrequency,
    extractFields: extractFields,
    extractViewMeta: extractViewMeta,
    checkViewExists: checkViewExists,
    sanitizeSQLQuery: sanitizeSQLQuery,
    filterGBs: filterGBs,
    sortByEvictionCost: sortByEvictionCost,
    sortByCalculationCost: sortByCalculationCost,
    reconstructSlicedCachedResult: reconstructSlicedCachedResult,
    getMainTransactionObject: getMainTransactionObject,
    assignTimes: assignTimes,
    findSameOldestResults: findSameOldestResults,
    sortByWord2Vec: sortByWord2Vec
};
const costFunctions = require('./costFunctions');
