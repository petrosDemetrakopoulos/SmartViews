const config = require('../config');
function sumObjects (ob1, ob2) {
    let sum = {};
    Object.keys(ob1).forEach(key => {
        if (key !== 'operation' && key !== 'field' && key !== 'groupByFields') {
            if (ob2.hasOwnProperty(key)) {
                sum[key] = ob1[key] + ob2[key]
            }
        }
    });
    sum['operation'] = ob1['operation'];
    sum['field'] = ob1['field'];
    return sum;
}

function maxObjects (ob1, ob2) {
    let max = {};
    Object.keys(ob1).forEach(key => {
        if (key !== 'operation' && key !== 'field' && key !== 'groupByFields') {
            if (ob2.hasOwnProperty(key)) {
                if (ob1[key] >= ob2[key]) {
                    max[key] = ob1[key];
                } else {
                    max[key] = ob2[key];
                }
            }
        }
    });
    max['operation'] = ob1['operation'];
    max['field'] = ob1['field'];
    return max;
}

function minObjects (ob1, ob2) {
    let min = {};
    Object.keys(ob1).forEach(key => {
        if (key !== 'operation' && key !== 'field' && key !== 'groupByFields') {
            if (ob2.hasOwnProperty(key)) {
                if (ob1[key] <= ob2[key]) {
                    min[key] = ob1[key];
                } else {
                    min[key] = ob2[key];
                }
            }
        }
    });
    min['operation'] = ob1['operation'];
    min['field'] = ob1['field'];
    return min;
}

function averageObjects (ob1, ob2) {
    let avg = {};
    Object.keys(ob1).forEach(key => {
        if (key !== 'operation' && key !== 'field' && key !== 'groupByFields') {
            if (ob2.hasOwnProperty(key)) {
                let sumNew = ob1[key]['sum'] + ob2[key]['sum'];
                let countNew = ob1[key]['count'] + ob2[key]['count'];
                let avgNew = sumNew / countNew;
                avg[key] = { 'average': avgNew, 'count': countNew, 'sum': sumNew };
            }
        }
    });
    avg['operation'] = ob1['operation'];
    avg['field'] = ob1['field'];
    return avg;
}

function flatten (items) {
    const flat = [];
    items.forEach(item => {
        flat.push(item);
        if (Array.isArray(item.children) && item.children.length > 0) {
            flat.push(...flatten(item.children));
            delete item.children
        }
        delete item.children
    });
    return flat;
}

function removeDuplicates (arr) {
    return arr.filter(function (elem, index, self) {
        return index === self.indexOf(elem);
    });
}

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
    console.log('sql time = ' + resultObject.sqlTime);
    console.log('bc time = ' + resultObject.bcTime);
    console.log('cache save time = ' + resultObject.cacheSaveTime);
    if (resultObject.cacheRetrieveTime) {
        console.log('cache retrieve time = ' + resultObject.cacheRetrieveTime);
    }
    if (resultObject.obj1Time) {
        console.log('->     obj1 server time = ' + resultObject.obj1Time);
    }
    if (resultObject.obj2Time) {
        console.log('->     obj2 server time = ' + resultObject.obj2Time);
    }
    console.log('total time = ' + resultObject.totalTime);
    console.log('all total time = ' + resultObject.allTotal);
}
function containsAllFields (transformedArray, view) {
    for (let i = 0; i < transformedArray.length; i++) {
        let containsAllFields = true;
        let crnView = transformedArray[i];

        let cachedGBFields = JSON.parse(crnView.columns);
        for (let index in cachedGBFields.fields) {
            cachedGBFields.fields[index] = cachedGBFields.fields[index].trim();
        }
        console.log(cachedGBFields);
        for (let j = 0; j < view.gbFields.length; j++) {
            console.log(view.gbFields[j]);
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

module.exports = {
    sumObjects: sumObjects,
    maxObjects: maxObjects,
    minObjects: minObjects,
    averageObjects: averageObjects,
    containsAllFields: containsAllFields,
    flatten: flatten,
    removeDuplicates: removeDuplicates,
    configFileValidations: configFileValidations,
    removeTimestamps: removeTimestamps,
    printTimes: printTimes,
    getRandomInt: getRandomInt,
    getRandomFloat: getRandomFloat
};
