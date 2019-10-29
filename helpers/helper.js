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
    if (config.cacheEvictionPolicy !== 'FIFO' && config.cacheEvictionPolicy !== 'COST FUNCTION') {
        formatErrors.push({ field: 'cacheEvictionPolicy', error: 'Should be either \'FIFO\' or \'COST FUNCTION\'' });
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
    if (resultObject.obj1Time) {
        log('->     obj1 server time = ' + resultObject.obj1Time);
    }
    if (resultObject.obj2Time) {
        log('->     obj2 server time = ' + resultObject.obj2Time);
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
        log(cachedGBFields);
        for (let j = 0; j < view.gbFields.length; j++) {
            log(view.gbFields[j]);
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

function time(){
    return microtime.nowDouble();
}

function log(logString){
    if(config.logging){
        console.log(logString);
    }
}

function requireUncached(module){
    delete require.cache[require.resolve(module)];
    return require(module);
}

function mergeSlicedCachedResult (allCached){
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

function getJSONFiles(items){
    let suffix = '.json';
    let jsonFiles = items.filter(file => {
        return file.indexOf(suffix) !== -1; // filtering out non-json files
    });
    return jsonFiles;
}

function transformGBMetadataFromBlockchain(resultGB){
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
    return transformedArray;
}

function updateViewFrequency(factTbl, contract, crnView){
    factTbl.views[crnView].frequency = factTbl.views[crnView].frequency + 1;
    fs.writeFile('./templates/' + contract + '.json', JSON.stringify(factTbl, null, 2), function (err) {
        if (err) return helper.log(err);
        log('updated view frequency');
    });
}

module.exports = {
    containsAllFields: containsAllFields,
    configFileValidations: configFileValidations,
    removeTimestamps: removeTimestamps,
    printTimes: printTimes,
    getRandomInt: getRandomInt,
    getRandomFloat: getRandomFloat,
    time: time,
    log:log,
    requireUncached: requireUncached,
    mergeSlicedCachedResult: mergeSlicedCachedResult,
    extractGBValues: extractGBValues,
    getJSONFiles: getJSONFiles,
    transformGBMetadataFromBlockchain: transformGBMetadataFromBlockchain,
    updateViewFrequency:updateViewFrequency
};
