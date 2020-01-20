const stringify = require('fast-stringify');
const helper = require('../helpers/helper');
function transformGBFromSQL (groupByResult, operation, aggregateField, gbField) {
    let transformed = {};
    if (operation !== 'AVERAGE') { //AVERAGE has exclusive treatment as it can be incrementally calculated iff we keep both sum and count
        helper.log('OPERATION = ' + operation);
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i][operation + '(' + aggregateField + ')'];
            delete groupByResult[i][operation + '(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = crnCount;
        }
        transformed.operation = operation;
    } else {
        helper.log('OPERATION = AVERAGE');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            let crnSum = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
        }
        transformed.operation = 'AVERAGE';
    }
    transformed.groupByFields = gbField;
    transformed.field = aggregateField;
    return transformed;
}

function transformAverage (groupByResult, gbField, aggregateField) {
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
        transformed[stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
    }
    transformed.operation = 'AVERAGE';
    transformed.groupByFields = gbField;
    transformed.field = aggregateField;
    return transformed;
}

module.exports = {
    transformGBFromSQL: transformGBFromSQL,
    transformAverage: transformAverage
};
