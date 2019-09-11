function transformGBFromSQL (groupByResult, operation, aggregateField, gbField) {
    console.log('***');
    console.log(groupByResult);
    console.log('***');
    console.log(gbField);
    console.log('***');
    let transformed = {};
    if (operation === 'COUNT') {
        console.log('OPERATION = COUNT');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'COUNT';
    } else if (operation === 'SUM') {
        console.log('OPERATION = SUM');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'SUM';
    } else if (operation === 'MIN') {
        console.log('OPERATION = MIN');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MIN(' + aggregateField + ')'];
            delete groupByResult[i]['MIN(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MIN';
    } else if (operation === 'MAX') {
        console.log('OPERATION = MAX');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MAX(' + aggregateField + ')'];
            delete groupByResult[i]['MAX(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MAX';
    } else { // AVERAGE
        console.log('OPERATION = AVERAGE');

        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            let crnSum = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
        }

        transformed['operation'] = 'AVERAGE';
    }
    transformed['groupByFields'] = gbField;
    transformed['field'] = aggregateField;
    return transformed;
}

function transformReadyAverage (groupByResult, gbField, aggregateField) {
    let transformed = {};
    for (let i = 0; i < groupByResult.length; i++) {
        let crnRes = groupByResult[i];
        let sumOfCountsField = Object.keys(crnRes)[Object.values(crnRes).length - 1];
        let sumOfSumsField = Object.keys(crnRes)[Object.values(crnRes).length - 2];
        let crnCount = groupByResult[i][sumOfCountsField];
        let crnSum = groupByResult[i][sumOfSumsField];
        delete groupByResult[i][sumOfCountsField];
        delete groupByResult[i][sumOfSumsField];
        let filtered = groupByResult[i];
        transformed[JSON.stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
    }
    transformed['operation'] = 'AVERAGE';
    transformed['groupByFields'] = gbField;
    transformed['field'] = aggregateField;
    return transformed;
}

function transformGB (groupByResult, operation, aggregateField) {
    if (operation === 'COUNT') {
        console.log('OPERATION = COUNT');
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            for (let row in crnGoup) {
                cnt++;
            }
            groupByResult[key] = cnt;
        }
        groupByResult['operation'] = 'COUNT'
    } else if (operation === 'SUM') {
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            for (let row in crnGoup) {
                cnt += Number(crnGoup[row][aggregateField]);
            }
            groupByResult[key] = cnt;
        }
        groupByResult['operation'] = 'SUM';
        groupByResult['field'] = aggregateField;
    } else if (operation === 'MIN') {
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let min = Number(crnGoup[0][aggregateField]);
            for (let row in crnGoup) {
                if (Number(crnGoup[row][aggregateField]) < min) {
                    min = Number(crnGoup[row][aggregateField])
                }
            }
            groupByResult[key] = min;
        }
        groupByResult['operation'] = 'MIN';
        groupByResult['field'] = aggregateField;
    } else if (operation === 'MAX') {
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let max = Number(crnGoup[0][aggregateField]);
            for (let row in crnGoup) {
                if (Number(crnGoup[row][aggregateField]) > max) {
                    max = Number(crnGoup[row][aggregateField])
                }
            }
            groupByResult[key] = max;
        }
        groupByResult['operation'] = 'MAX';
        groupByResult['field'] = aggregateField;
    } else { // AVERAGE
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            let sum = 0;
            for (let row in crnGoup) {
                sum += Number(crnGoup[row][aggregateField]);
                cnt += 1;
            }
            groupByResult[key] = { 'average': sum / cnt, 'count': cnt, 'sum': sum };
        }
        groupByResult['operation'] = 'AVERAGE';
        groupByResult['field'] = aggregateField;
    }
    return groupByResult;
}

function calculateReducedGB (operation, aggregateField, cachedGroupBy, gbFields) {
    let transformedArray = [];
    let originalArray = [];
    let i = 0;
    // logic to incrementally calculate the new gb
    Object.keys(cachedGroupBy).forEach(function (key, index) {
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field') {
            let crnUniqueVal = JSON.parse(key);
            console.log('crnuniqueVal BEFORE');
            console.log(crnUniqueVal);
            console.log('***');
            originalArray[i] = cachedGroupBy[key];
            Object.keys(crnUniqueVal).forEach(function (key2, index2) {
                console.log('gbFields = ' + gbFields);
                console.log('key2 = ' + key2);
                if (gbFields.indexOf(key2) <= -1) {
                    delete crnUniqueVal[key2];
                }
                transformedArray[i] = JSON.stringify(crnUniqueVal);
            });
            console.log('crnuniqueVal AFTER');
            console.log(crnUniqueVal);
            i++;
            console.log('***');
        }
        console.log('transformed array = ' + transformedArray);
        console.log('original array = ' + originalArray);
    });
    let uniqueKeys = new Set(transformedArray);
    let uniqueKeysArray = Array.from(uniqueKeys);
    let respObj = {};
    if (operation === 'SUM' || operation === 'COUNT') {
        let sumPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            sumPerKey[j] = 0;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            sumPerKey[indexOfUK] += originalArray[j];
        }
        for (let j = 0; j < sumPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = sumPerKey[j];
        }
    } else if (operation === 'MIN') {
        let minPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            minPerKey[j] = Math.max;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            if (originalArray[j] < minPerKey[indexOfUK]) {
                minPerKey[indexOfUK] = originalArray[j];
            }
        }
        for (let j = 0; j < minPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = minPerKey[j];
        }
    } else if (operation === 'MAX') {
        let maxPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            maxPerKey[j] = Math.min;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            if (originalArray[j] > maxPerKey[indexOfUK]) {
                maxPerKey[indexOfUK] = originalArray[j];
            }
        }
        for (let j = 0; j < maxPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = maxPerKey[j];
        }
    } else { // AVERAGE
        let avgPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            avgPerKey[j] = JSON.stringify({ count: 0, sum: 0, average: 0 });
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            let parsedObj = JSON.parse(avgPerKey[j]);
            let newSum = parsedObj['sum'] + originalArray[j]['sum'];
            let newCount = parsedObj['count'] + originalArray[j]['count'];
            avgPerKey[indexOfUK] = { count: newCount, sum: newSum, average: newSum / newCount };
        }
        for (let j = 0; j < avgPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = avgPerKey[j];
        }
    }
    return respObj;
}

module.exports = {
    transformGBFromSQL: transformGBFromSQL,
    transformGB: transformGB,
    calculateReducedGB: calculateReducedGB,
    transformReadyAverage: transformReadyAverage
};
