let config = require('../config_private');
let mysqlConfig = {};
let connection = null;
const mysql = require('mysql');
const transformations = require('../helpers/transformations');
const jsonSql = require('json-sql')({ separatedValues: false });
let createTable = '';
let tableName = '';
const helper = require('../helpers/helper');

function setCreateTable (newCreateTable) {
    createTable = newCreateTable;
}

function setTableName (newTableName) {
    tableName = newTableName;
}

function connectToSQL(callback) {
    config = helper.requireUncached('../config_private');
    mysqlConfig = config.sql;
    connection = mysql.createConnection(mysqlConfig);
    connection.connect(function (err) {
        if (err) {
            console.error('error connecting to mySQL: ' + err.stack);
            callback(err);
        }
        callback(null);
    });
}

function calculateNewGroupBy (facts, operation, gbFields, aggregationField, callback) {
    connection.query('DROP TABLE IF EXISTS ' + tableName, function (err) {
        if (err) {
            helper.log(err);
            callback(null, err);
        }
        connection.query(createTable, function (error, results) { // creating the SQL table for 'Fact Table'
            if (error) {
                helper.log(error);
                callback(null, error);
            }
            if (facts.length === 0) {
                callback(null, { error: 'No facts' });
            }
            let sql = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: facts
            });

            let editedQuery = sql.query.replace(/"/g, '');
            editedQuery = editedQuery.replace(/''/g, 'null');
            connection.query(editedQuery, function (error, results2) { // insert facts
                if (error) {
                    helper.log(error);
                    callback(null, error);
                }

                let gbQuery = null;
                if (operation === 'AVERAGE') {
                    gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: gbFields,
                        fields: [gbFields,
                            {
                                func: {
                                    name: 'SUM', args: [{ field: aggregationField }]
                                }
                            },
                            {
                                func: {
                                    name: 'COUNT', args: [{ field: aggregationField }]
                                }
                            }]
                    });
                } else {
                    gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: gbFields,
                        fields: [gbFields,
                            {
                                func: {
                                    name: operation,
                                    args: [{ field: aggregationField }]
                                }
                            }]
                    });
                }
                let editedGB = gbQuery.query.replace(/"/g, '');
                connection.query(editedGB, function (error, results3) {
                    if (error) {
                        helper.log(error);
                        callback(null, error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        if (err) {
                            helper.log(err);
                            callback(null, err);
                        }
                        let groupBySqlResult = transformations.transformGBFromSQL(results3, operation, aggregationField, gbFields);
                        callback(groupBySqlResult, null);
                    });
                });
            });
        });
    });
}

function calculateReducedGroupBy (cachedGroupBy, view, gbFields, callback) {
    // this means we want to calculate a different group by than the stored one
    // but however it can be calculated just from redis cache
    // calculating the reduced Group By in SQL
    let tableName = cachedGroupBy.gbCreateTable.split(' ');
    tableName = tableName[3];
    tableName = tableName.split('(')[0];
    helper.log('TABLE NAME = ' + tableName);
    connection.query(cachedGroupBy.gbCreateTable, async function (error) {
        if (error) {
            callback(null, error);
        }
        let rows = [];
        let lastCol = '';
        let prelastCol = ''; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
        lastCol = cachedGroupBy.gbCreateTable.split(' ');
        prelastCol = lastCol[lastCol.length - 4];
        lastCol = lastCol[lastCol.length - 2];
        let gbVals = Object.values(cachedGroupBy);
        for (let i = 0, keys = Object.keys(cachedGroupBy); i < keys.length; i++) {
            let key = keys[i];
            if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
                let crnRow = JSON.parse(key);
                if (view.operation === 'AVERAGE') {
                    crnRow[prelastCol] = gbVals[i]['sum'];
                    crnRow[lastCol] = gbVals[i]['count'];
                } else {
                    crnRow[lastCol] = gbVals[i];
                }
                rows.push(crnRow);
            }
        }

        let sqlInsert = jsonSql.build({
            type: 'insert',
            table: tableName,
            values: rows
        });
        let editedQuery = sqlInsert.query.replace(/"/g, '');
        editedQuery = editedQuery.replace(/''/g, 'null');
        connection.query(editedQuery, function (error, results, fields) {
            if (error) {
                helper.log(error);
                callback(null, error);
            }
            let op = '';
            if (view.operation === 'SUM' || view.operation === 'COUNT') {
                op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
            } else if (view.operation === 'MIN') {
                op = 'MIN'
            } else if (view.operation === 'MAX') {
                op = 'MAX';
            }
            let gbQuery = jsonSql.build({
                type: 'select',
                table: tableName,
                group: gbFields,
                fields: [gbFields,
                    {
                        func: {
                            name: op,
                            args: [{ field: lastCol }]
                        }
                    }]
            });
            if (view.operation === 'AVERAGE') {
                gbQuery = jsonSql.build({
                    type: 'select',
                    table: tableName,
                    group: gbFields,
                    fields: [gbFields,
                        {
                            func: {
                                name: 'SUM',
                                args: [{ field: prelastCol }]
                            }
                        },
                        {
                            func: {
                                name: 'SUM',
                                args: [{ field: lastCol }]
                            }
                        }]
                });
            }
            let editedGBQuery = gbQuery.query.replace(/"/g, '');
            editedGBQuery = editedGBQuery.replace(/''/g, 'null');
            connection.query(editedGBQuery, function (error, results) {
                if (error) {
                    helper.log(error);
                    callback(null, error);
                }
                connection.query('DROP TABLE ' + tableName, function (err) {
                    if (err) {
                        helper.log(err);
                        callback(null, err);
                    }
                    callback(results);
                });
            });
        });
    });
}

function mergeGroupBys (groupByA, groupByB, gbCreateTable, tableName, view, lastCol, prelastCol, callback) {
    connection.query(gbCreateTable, function (error, results, fields) {
        if (error) {
            helper.log(error);
            callback(null, error);
        }

        let sqlInsertA = jsonSql.build({
            type: 'insert',
            table: tableName,
            values: groupByA
        });

        let sqlInsertB = jsonSql.build({
            type: 'insert',
            table: tableName,
            values: groupByB
        });

        let editedQueryA = sqlInsertA.query.replace(/"/g, '');
        editedQueryA = editedQueryA.replace(/''/g, 'null');

        let editedQueryB = sqlInsertB.query.replace(/"/g, '');
        editedQueryB = editedQueryB.replace(/''/g, 'null');

        connection.query(editedQueryA, function (err, results, fields) {
            if (err) {
                helper.log(err);
                callback(null, err);
            }
            connection.query(editedQueryB, function (err, results, fields) {
                if (err) {
                    helper.log(err);
                    callback(null, err);
                }
                let op = '';
                if (view.operation === 'SUM' || view.operation === 'COUNT') {
                    op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
                } else if (view.operation === 'MIN') {
                    op = 'MIN'
                } else if (view.operation === 'MAX') {
                    op = 'MAX';
                }
                let gbQuery = jsonSql.build({
                    type: 'select',
                    table: tableName,
                    group: view.gbFields,
                    fields: [view.gbFields,
                        {
                            func: {
                                name: op,
                                args: [{ field: lastCol }]
                            }
                        }]
                });
                if (view.operation === 'AVERAGE') {
                    gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: view.gbFields,
                        fields: [view.gbFields,
                            {
                                func: {
                                    name: 'SUM',
                                    args: [{ field: prelastCol }]
                                }
                            },
                            {
                                func: {
                                    name: 'SUM',
                                    args: [{ field: lastCol }]
                                }
                            }]
                    });
                }

                let editedGBQuery = gbQuery.query.replace(/"/g, '');
                editedGBQuery = editedGBQuery.replace(/''/g, 'null');
                connection.query(editedGBQuery, async function (error, results, fields) {
                    if (error) {
                        helper.log(error);
                        callback(null, error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        if (err) {
                            helper.log(err);
                            callback(null, err);
                        }

                        let groupBySqlResult = {};
                        if (view.operation === 'AVERAGE') {
                            groupBySqlResult = transformations.transformReadyAverage(results, view.gbFields, view.aggregationField);
                        } else {
                            groupBySqlResult = transformations.transformGBFromSQL(results, op, lastCol, view.gbFields);
                        }
                        callback(groupBySqlResult);
                    });
                });
            });
        });
    });
}

function executeQuery (queryString, callback) {
    connection.query(queryString, async function (error, results, fields) {
        if (error) {
           callback(error);
        }
        callback(null, results, fields);
    });
}

module.exports = {
    setCreateTable: setCreateTable,
    setTableName: setTableName,
    connectToSQL: connectToSQL,
    calculateNewGroupBy: calculateNewGroupBy,
    calculateReducedGroupBy: calculateReducedGroupBy,
    mergeGroupBys: mergeGroupBys,
    executeQuery: executeQuery
};