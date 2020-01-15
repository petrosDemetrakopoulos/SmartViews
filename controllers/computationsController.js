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

function connectToSQL () {
    return new Promise((resolve, reject) => {
        config = helper.requireUncached('../config_private');
        mysqlConfig = config.sql;
        connection = mysql.createConnection(mysqlConfig);
        connection.connect(function (err) {
            if (err) {
                /* istanbul ignore next */
                console.error('error connecting to mySQL: ' + err.stack);
                reject(err);
            }
            resolve(true);
        });
    });
}

// It is a very common pattern to run a query to make a view materialization
// and then drop the temporary table we used to do it.
function queryAndDropTable (query, tableName) {
    return new Promise((resolve, reject) => {
        connection.query(query, function (error, results) {
            if (error) {
                /* istanbul ignore next */
                helper.log(error);
                reject(error);
            }
            connection.query('DROP TABLE ' + tableName, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    helper.log(err);
                    reject(err);
                }
                resolve(results);
            });
        });
    });
}

function calculateNewGroupBy (facts, operation, gbFields, aggregationField) {
    return new Promise((resolve, reject) => {
        connection.query('DROP TABLE IF EXISTS ' + tableName, function (err) {
            if (err) {
                /* istanbul ignore next */
                helper.log(err);
                reject(err);
            }
            connection.query(createTable, function (error) { // creating the SQL table for 'Fact Table'
                if (error) {
                    /* istanbul ignore next */
                    helper.log(error);
                    reject(err);
                }
                if (facts.length === 0) {
                    reject(new Error('No facts'));
                }
                let sql = jsonSql.build({
                    type: 'insert',
                    table: tableName,
                    values: facts
                });

                let editedQuery = helper.sanitizeSQLQuery(sql);
                connection.query(editedQuery, function (error) { // insert facts
                    if (error) {
                        /* istanbul ignore next */
                        helper.log(error);
                        reject(error);
                    }

                    let gbQuery;
                    if (operation === 'AVERAGE') {
                        gbQuery = jsonSql.build({
                            type: 'select',
                            table: tableName,
                            group: gbFields,
                            fields: [gbFields,
                                {
                                    func: { name: 'SUM', args: [{ field: aggregationField }] }
                                },
                                {
                                    func: { name: 'COUNT', args: [{ field: aggregationField }] }
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

                    let editedGB = helper.sanitizeSQLQuery(gbQuery);
                    queryAndDropTable(editedGB, tableName).then(results => {
                        let groupBySqlResult = transformations.transformGBFromSQL(results, operation, aggregationField, gbFields);
                        resolve(groupBySqlResult);
                    }).catch(err => {
                        /* istanbul ignore next */
                        helper.log(err);
                        reject(err);
                    });
                });
            });
        });
    });
}

function calculateReducedGroupBy (cachedGroupBy, view, gbFields) {
    // this means we want to calculate a different group by than the stored one
    // but however it can be calculated just from redis cache
    // calculating the reduced Group By in SQL
    return new Promise((resolve, reject) => {
        let tableName = cachedGroupBy.gbCreateTable.split(' ');
        tableName = tableName[3];
        tableName = tableName.split('(')[0];
        connection.query(cachedGroupBy.gbCreateTable, async function (error) {
            if (error) {
                /* istanbul ignore next */
                reject(error);
            }
            let lastCol = '';
            let prelastCol = '';
            // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
            lastCol = cachedGroupBy.gbCreateTable.split(' ');
            prelastCol = lastCol[lastCol.length - 4];
            lastCol = lastCol[lastCol.length - 2];

            let rows = helper.extractGBValues(cachedGroupBy, view);

            let sqlInsert = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: rows
            });
            let editedQuery = helper.sanitizeSQLQuery(sqlInsert);
            connection.query(editedQuery, function (error) {
                if (error) {
                    /* istanbul ignore next */
                    helper.log(error);
                    reject(error);
                }
                let op = helper.extractOperation(view.operation);

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

                let editedGBQuery = helper.sanitizeSQLQuery(gbQuery);
                queryAndDropTable(editedGBQuery, tableName).then(results => {
                    resolve(results);
                }).catch(err => {
                    /* istanbul ignore next */
                    helper.log(err);
                    /* istanbul ignore next */
                    reject(err);
                });
            });
        });
    });
}

function mergeGroupBys (groupByA, groupByB, view, viewMeta) {
    return new Promise((resolve, reject) => {
        let lastCol = viewMeta.lastCol;
        let prelastCol = viewMeta.prelastCol;
        let tableName = viewMeta.viewNameSQL;
        let gbCreateTable = view.SQLTable;
        connection.query(gbCreateTable, function (error) {
            if (error) {
                /* istanbul ignore next */
                helper.log(error);
                reject(error);
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

            let editedQueryA = helper.sanitizeSQLQuery(sqlInsertA);
            let editedQueryB = helper.sanitizeSQLQuery(sqlInsertB);

            connection.query(editedQueryA, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    helper.log(err);
                    /* istanbul ignore next */
                    reject(err);
                }
                connection.query(editedQueryB, function (err) {
                    if (err) {
                        /* istanbul ignore next */
                        helper.log(err);
                        /* istanbul ignore next */
                        reject(err);
                    }
                    let op = helper.extractOperation(view.operation);
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

                    let editedGBQuery = helper.sanitizeSQLQuery(gbQuery);
                    queryAndDropTable(editedGBQuery, tableName).then(results => {
                        let groupBySqlResult;
                        if (view.operation === 'AVERAGE') {
                            groupBySqlResult = transformations.transformAverage(results, view.gbFields, view.aggregationField);
                        } else {
                            groupBySqlResult = transformations.transformGBFromSQL(results, op, lastCol, view.gbFields);
                        }
                        resolve(groupBySqlResult);
                    }).catch(err => {
                        /* istanbul ignore next */
                        helper.log(err);
                        /* istanbul ignore next */
                        reject(err);
                    });
                });
            });
        });
    });
}

function executeQuery (queryString) {
    return new Promise((resolve, reject) => {
        connection.query(queryString, async function (error, results) {
            if (error) {
                /* istanbul ignore next */
                reject(error)
            } else {
                resolve(results);
            }
        });
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
