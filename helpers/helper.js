'use strict';
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
        config.cacheEvictionPolicy !== 'cubeDistance') {
        formatErrors.push({ field: 'cacheEvictionPolicy',
            error: 'Should be either \'FIFO\' or \'costFunction\' or \'cubeDistance\'' });
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
        const crnView = transformedArray[i];

        let cachedGBFields = JSON.parse(crnView.columns);
        for (let index in cachedGBFields.fields) {
            cachedGBFields.fields[index] = cachedGBFields.fields[index].trim();
        }
        for (let j = 0; j < view.fields.length; j++) {
            if (!cachedGBFields.fields.includes(view.fields[j])) {
                containsAllFields = false;
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
    let reservedKeys = ['operation', 'groupByFields', 'field', 'viewName']
    for (const index in allCached) {
        let crnSub = allCached[index];
        let crnSubArray = JSON.parse(crnSub);
        for (const kv in crnSubArray) {
            if (reservedKeys.indexOf(kv) === -1) {
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
    for (const i of mergedArray) {
        const crnKey = Object.keys(i)[0];
        gbFinal[crnKey] = Object.values(i)[0];
    }
    return gbFinal;
}

function extractGBValues (reducedResult, view) {
    let rows = [];
    const gbValsReduced = Object.values(reducedResult);
    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4];
    lastCol = lastCol[lastCol.length - 2];

    for (let i = 0, keys = Object.keys(reducedResult); i < keys.length; i++) {
        const key = keys[i];
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
            let crnRow = JSON.parse(key);
            if (view.operation === 'AVERAGE') {
                crnRow[prelastCol] = gbValsReduced[i].sum;
                crnRow[lastCol] = gbValsReduced[i].count;
            } else {
                crnRow[lastCol] = gbValsReduced[i];
            }
            rows.push(crnRow);
        }
    }
    return rows;
}

function getJSONFiles (items) {
    const suffix = '.json';
    return items.filter(file => {
        return file.indexOf(suffix) !== -1; // filtering out non-json files
    });
}

function transformGBMetadataFromBlockchain (resultGB) {
    const len = Object.keys(resultGB).length;
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

function extractOperation (op) {
    let operation = '';
    if (op === 'SUM' || op === 'COUNT') {
        operation = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
    } else {
        operation = op;
    }
    return operation;
}

function extractViewMeta (view) {
    let viewNameSQL = view.SQLTable.split(' ');
    viewNameSQL = viewNameSQL[3];
    viewNameSQL = viewNameSQL.split('(')[0];

    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4]; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
    lastCol = lastCol[lastCol.length - 2];

    let op = extractOperation(view.operation);

    return { viewNameSQL: viewNameSQL, lastCol: lastCol, prelastCol: prelastCol, op: op };
}

function checkViewExists (viewsDefined, viewName) {
    let view = {};
    if (viewsDefined.has(viewName)) {
        view = viewsDefined.get(viewName);
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
        if (transformedArray[i].containsAllFields && JSON.parse(transformedArray[i].columns).aggrFunc === view.operation) {
            filteredGBs.push(transformedArray[i]);
        }
    }
    return filteredGBs;
}

async function sortByEvictionCost (resultGB, latestId, view, factTbl) {
    let transformedArray = transformGBMetadataFromBlockchain(resultGB);
    transformedArray = containsAllFields(transformedArray, view); // assigns the containsAllFields value
    let sortedByEvictionCost = JSON.parse(JSON.stringify(transformedArray));
    if (config.cacheEvictionPolicy === 'dataCubeDistance') {
        sortedByEvictionCost = costFunctions.dataCubeDistanceBatch(sortedByEvictionCost, view);
    } else if (config.cacheEvictionPolicy === 'word2vec') {
        sortedByEvictionCost = costFunctions.word2vec(resultGB, view);
    } else if (config.cacheEvictionPolicy === 'costFunction') {
        sortedByEvictionCost = await costFunctions.dispCost(sortedByEvictionCost, latestId, factTbl);
    }
    sortedByEvictionCost.sort(function (a, b) {
        switch (config.cacheEvictionPolicy) {
        case 'FIFO':
            return parseInt(a.gbTimestamp) - parseInt(b.gbTimestamp);
        case 'costFunction:':
            return parseInt(a.cacheEvictionCost) - parseInt(b.cacheEvictionCost);
        case 'word2vec':
            return parseInt(a.word2vecScore) - parseInt(b.word2vecScore);
        case 'dataCubeDistance':
            return parseFloat(b.dataCubeDistance) - parseFloat(a.dataCubeDistance);
        }
    });
    return sortedByEvictionCost;
}

function sortByCalculationCost (resultGBs, latestId, view) {
    if (config.calculationCostFunction === 'costFunction') {
        resultGBs = costFunctions.calculationCostOfficial(resultGBs, latestId); // the cost to materialize the view from each view cached
        resultGBs.sort((a, b) => parseFloat(a.calculationCost) - parseFloat(b.calculationCost)); // order ascending
    } else if (config.calculationCostFunction === 'dataCubeDistance') {
        resultGBs = costFunctions.dataCubeDistanceBatch(resultGBs, view);
        resultGBs.sort((a, b) => parseFloat(a.dataCubeDistance) - parseFloat(b.dataCubeDistance)); // order ascending
    }
    return resultGBs;
}

async function sortByWord2Vec (resultGBs, view) {
    if (resultGBs.length > 1) {
        resultGBs = await costFunctions.word2vec(resultGBs, view);
        resultGBs.sort((a, b) => parseFloat(b.word2vecScore) - parseFloat(a.word2vecScore));
    }
    return resultGBs;
}

function reconstructSlicedCachedResult (cachedGB) {
    const hashId = cachedGB.hash.split('_')[1];
    const hashBody = cachedGB.hash.split('_')[0];
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
    result.allTotal = times.totalEnd - times.totalStart;
    return result;
}

function findSameOldestResults (sortedByEvictionCost, view) {
    let sameOldestResults = [];
    for (let i = 0; i < sortedByEvictionCost.length; i++) {
        const crnRes = sortedByEvictionCost[i];
        const meta = JSON.parse(crnRes.columns);
        if (JSON.stringify(meta.fields) === JSON.stringify(view.fields) && meta.aggrFunc === view.operation) {
            sameOldestResults.push(crnRes);
        }
    }
    return sameOldestResults;
}

function welcomeMessage () {
    console.log('     _____                          _ __      __ _                      ');
    console.log('    / ____|                        | |\\ \\    / /(_)                     ');
    console.log('   | (___   _ __ ___    __ _  _ __ | |_\\ \\  / /  _   ___ __      __ ___ ');
    console.log('    \\___ \\ | \'_ ` _ \\  / _` || \'__|| __| \\ / /  | | / _ \\ \\  /\\ / // __|');
    console.log('    ____) || | | | | || (_| || |   | |_  \\  /   | ||  __/ \\ V  V / \\__ \\');
    console.log('   |_____/ |_| |_| |_| \\__,_||_|    \\__|  \\/    |_| \\___|  \\_/\\_/  |___/');
    console.log('*******************************************************************************');
    console.log('                  A blockchain enabled OLAP Data Warehouse                    ');
    console.log('*******************************************************************************');
}

function errorToJson (error) {
    return { status: 'ERROR', message: error.message };
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
    sortByWord2Vec: sortByWord2Vec,
    extractOperation: extractOperation,
    welcomeMessage: welcomeMessage,
    errorToJson: errorToJson
};
const costFunctions = require('./costFunctions');
