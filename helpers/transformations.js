const stringify = require('fast-stringify');
const helper = require('../helpers/helper');
function transformGBFromSQL (groupByResult, operation, aggregateField, gbField) {
    let transformed = {};
    if (operation === 'COUNT') {
        helper.log('OPERATION = COUNT');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'COUNT';
    } else if (operation === 'SUM') {
        helper.log('OPERATION = SUM');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'SUM';
    } else if (operation === 'MIN') {
        helper.log('OPERATION = MIN');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MIN(' + aggregateField + ')'];
            delete groupByResult[i]['MIN(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MIN';
    } else if (operation === 'MAX') {
        helper.log('OPERATION = MAX');
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MAX(' + aggregateField + ')'];
            delete groupByResult[i]['MAX(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MAX';
    } else { // AVERAGE
        helper.log('OPERATION = AVERAGE');

        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            let crnSum = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
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
        transformed[stringify(filtered)] = { count: crnCount, sum: crnSum, average: crnSum / crnCount };
    }
    transformed['operation'] = 'AVERAGE';
    transformed['groupByFields'] = gbField;
    transformed['field'] = aggregateField;
    return transformed;
}

function transformGB (groupByResult, operation, aggregateField) {
    if (operation === 'COUNT') {
        helper.log('OPERATION = COUNT');
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

module.exports = {
    transformGBFromSQL: transformGBFromSQL,
    transformGB: transformGB,
    transformReadyAverage: transformReadyAverage
};
