const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const stringify = require('fast-stringify');
let config = require('./config_private');
const configLab = require('./config_lab');
const path = require('path');
abiDecoder = require('abi-decoder');
const app = express();
const jsonParser = bodyParser.json();
const helper = require('./helpers/helper');
const contractGenerator = require('./helpers/contractGenerator');
const transformations = require('./helpers/transformations');
const contractDeployer = require('./helpers/contractDeployer');
const contractController = require('./helpers/contractController');
const cacheController = require('./helpers/cacheController');
app.use(jsonParser);
let running = false;
let gbRunning = false;
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static('public'));
const microtime = require('microtime');
let http = require('http').Server(app);
let io = require('socket.io')(http);
const jsonSql = require('json-sql')({ separatedValues: false });
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider(config.blockchainIP));
const mysql = require('mysql');
let createTable = '';
let tableName = '';
let connection = null;
let contractsDeployed = [];

web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let acc = null;
let mainTransactionObject = {};
app.get('/', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            console.error('error reading templates directory: ' + err.stack);
            return;
        }
        res.render('index', { 'templates': items });
    })
});

app.get('/dashboard', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            console.error('error reading templates directory: ' + err.stack);
            return;
        }
        let suffix = ".json";
        let jsonFiles = items.filter(file => {
            return file.indexOf(suffix) !== -1; //filtering out non-json files
        });
        web3.eth.getBlockNumber().then(blockNum => {
            res.render('dashboard', { 'templates': jsonFiles, 'blockNum': blockNum });
        });
    });
});

app.get('/form/:contract', function (req, res) {
    let factTbl = require('./templates/' + req.params.contract);
    let templ = {};
    if ('template' in factTbl) {
        templ = factTbl['template'];
    } else {
        templ = factTbl;
    }
    let address = '0';
    for (let i = 0; i < contractsDeployed.length; i++) {
        if (contractsDeployed[i].contractName === factTbl.name) {
            address = contractsDeployed[i].address;
            break;
        }
    }

    let readyViews = factTbl.views;
    readyViews = readyViews.map(x => x.name);
    res.render('form', { 'template': templ, 'name': factTbl.name, 'address': address, 'readyViews': readyViews });
});

http.listen(3000, () => {
    console.log(`Smart-Views listening on http://localhost:3000/dashboard`);
    let mysqlConfig = {};
    let validations = helper.configFileValidations();
    if (process.env.ENVIRONMENT === 'LAB') {
        config = configLab;
    }
    if (validations.passed) {
        mysqlConfig = config.sql;
        connection = mysql.createConnection(mysqlConfig);
        connection.connect(function (err) {
            if (err) {
                console.error('error connecting to mySQL: ' + err.stack);
                return;
            }
            console.log('mySQL connected');
        });
    } else {
        console.log('Config file validations failed');
        console.log(validations);
        // if config validations fail, stop the server
        process.exit(1);
    }
});

app.get('/deployContract/:fn', function (req, res) {
    web3.eth.getAccounts(function (err, accounts) {
        if (!err) {
            acc = accounts[1];
            mainTransactionObject = {
                from: acc,
                gas: 1500000000000,
                gasPrice: '30000000000000'
            };
            contractDeployer.deployContract(accounts[0], './contracts/' + req.params.fn, contract)
                .then(options => {
                    console.log("******************");
                    console.log('Contract Deployed!');
                    console.log("******************");
                    contractsDeployed.push(options.contractDeployed);
                    contract = options.contractObject;
                    contractController.setContract(contract, acc);
                    cacheController.setContract(contract,acc);
                    res.send({ status: 'OK', options: options.options });
                })
                .catch(err => {
                    console.log('error on deploy ' + err);
                    res.status(400);
                    res.send({ status: 'ERROR', options: 'Deployment failed' });
                })
        }
    });
});

app.get('/load_dataset/:dt', function (req, res) {
    let dt = require('./test_data/' + req.params.dt);
    console.log('ENDPOINT HIT AGAIN');
    if (contract) {
        if (!running) {
            running = true;
            let startTime = microtime.nowDouble();
            contractController.addManyFacts(dt, config.recordsSlice).then(retval => {
                let endTime = microtime.nowDouble();
                let timeDiff = endTime - startTime;
                running = false;
                io.emit('DONE', 'TRUE');
                console.log('Added ' + dt.length + ' records in ' + timeDiff + ' seconds');
                return res.send('DONE');
            }).catch(error => {
                console.log(error);
            });
        }
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.get('/new_contract/:fn', function (req, res) {
    contractGenerator.generateContract(req.params.fn).then(result => {
        createTable = result.createTable;
        tableName = result.tableName;
        return res.send({ msg: 'OK', 'filename': result.filename + '.sol', 'template': result.template });
    }).catch(err => {
        console.log(err);
        return res.send({ msg: 'error' });
    });
});

app.get('/getFactById/:id', function (req, res) {
    if (contract) {
        contract.methods.getFact(parseInt(req.params.id, 10)).call(function (err, result) {
            if (!err) {
                let len = Object.keys(result).length;
                for (let j = 0; j < len / 2; j++) {
                    delete result[j];
                }
                res.send(result);
            } else {
                console.log(err);
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.get('/getFactsFromTo/:from/:to', function (req, res) {
    let timeStart = microtime.nowDouble();
    contractController.getFactsFromTo(parseInt(req.params.from), parseInt(req.params.to)).then(retval => {
        let timeFinish = microtime.nowDouble() - timeStart;
        retval.push({ time: timeFinish });
        res.send(retval);
    }).catch(err => {
        res.send(err);
    });
});

app.get('/allfacts', function (req, res) {
    if (contract) {
        contract.methods.dataId().call(function (err, result) {
            if (!err) {
                // async loop waiting to get all the facts separately
                let timeStart = microtime.nowDouble();
                contractController.getAllFactsHeavy(result).then(retval => {
                    let timeFinish = microtime.nowDouble() - timeStart;
                    console.log('Get all facts time: ' + timeFinish + ' s');
                    retval.push({ time: timeFinish });
                    // retval.timeDone = microtime.nowDouble() - timeStart;
                    res.send(retval);
                }).catch(error => {
                    console.log(error);
                });
            } else {
                console.log(err);
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.get('/groupbyId/:id', function (req, res) {
    if (contract) {
        contract.methods.getGroupBy(parseInt(req.params.id, 10)).call(function (err, result) {
            if (!err) {
                res.send(result)
            } else {
                console.log(err);
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

// this function  assigns a cost to each group by
function calculationCost (groupBys) {
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        let crnCost = (0.5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        crnGroupBy.cost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function cacheEvictionCost (groupBys) {
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        let crnCost = (5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        crnGroupBy.cacheEvictionCost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function myFunc(allGroupBys, latestFact, viewName) {
    let frequency = 10;
    let factTbl = require('./templates/ABCD');
    let viewsDefined = factTbl.views;
    let found = false;
    for (let crnView in viewsDefined) {
        if (factTbl.views[crnView].name === viewName) {
            found = true;
            frequency = factTbl.views[crnView].frequency;
            break;
        }
    }
    let AllGbCalculationCosts = calculationCostOfficial(allGroupBys, latestFact);
    let cachedViewsWithoutCurrentVC = [];
    let crnViewMinus = [];
    for (let i = 0; i < allGroupBys.length; i++){
        for (let j = 0; j < allGroupBys.length; j++){
            if (j !== i) { // we add all cached view EXCEPT the current one
                crnViewMinus.push(allGroupBys[j]);
            }
        }
        cachedViewsWithoutCurrentVC.push(crnViewMinus);
    }
    for (let j = 0; j < allGroupBys.length - 1; j++){
        cachedViewsWithoutCurrentVC[j] = calculationCostOfficial(cachedViewsWithoutCurrentVC[j], latestFact);
    }

    // for(let i = 0; i < allGroupBys.length; i++) {
    //     console.log("costs for materialization of view " + i  + ":");
    //     for(let j = 0; j < allGroupBys.length-1; j++){
    //         let cost = frequency*(AllGbCalculationCosts[i].calculationCost - cachedViewsWithoutCurrentVC[i][j].calculationCost);
    //         console.log("   without view " + j  + " = " + cost);
    //         if(i === j){
    //             allGroupBys[i].cacheEvictionCost = cost;
    //         }
    //     }
    // }
    for (let k = 1; k < allGroupBys.length; k++){
        console.log("cost k = " + allGroupBys[k].calculationCost);
        console.log("cost 0 = " + allGroupBys[0].calculationCost);
        allGroupBys[k].cacheEvictionCost = frequency*(allGroupBys[0].calculationCost - allGroupBys[k].calculationCost);
    }

    allGroupBys[0].cacheEvictionCost = 1000;
    return allGroupBys;
}

function cacheEvictionCostOfficial (groupBys, latestFact, viewName) { // the one written on paper , write one with only time or size
    let allHashes = [];
    let allGroupBys =[];
    let allGroupBys2 = [];
    let allMinus = [];
    for(let i = 0; i < groupBys.length; i++){
        allGroupBys.push(groupBys[i]);
        allGroupBys2.push(groupBys[i]);
    }
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        allHashes.push(crnGroupBy.hash);
    }
    cacheController.getManyCachedResults(allHashes, function (error, allCached) {
        let freq = 0;
        if (allHashes.length > 1) {
            for (let j = 0; j < allCached.length; j++) {
                let crnGb = JSON.parse(allCached[j]);
                let factTbl = require('./templates/ABCD');
                let viewsDefined = factTbl.views;
                let found = false;
                for (let crnView in viewsDefined) {
                    if (factTbl.views[crnView].name === crnGb.viewName) {
                        found = true;
                        console.log("FOUND = TRUE");
                        freq = factTbl.views[crnView].frequency;
                        break;
                    }
                }
            }


            for (let i = 0; i < allGroupBys.length; i++) {
                allGroupBys2 = [];
                for(let j = 0; j < groupBys.length; j++){
                    allGroupBys2.push(groupBys[j]);
                }
                let groupBysCachedExceptCrnOne = allGroupBys2.splice(1,i);
                let calcCostVfromVCache = calculationCostOfficial(allGroupBys, latestFact);
                let calcCostVfromVCacheMinusCrnView = calculationCostOfficial(groupBysCachedExceptCrnOne, latestFact);
                console.log("!!!!!!!!");
                console.log("ALL: ");
                console.log(calcCostVfromVCache);
                console.log("WITHOUT: ");
                console.log(calcCostVfromVCacheMinusCrnView);
                allMinus.push(calcCostVfromVCacheMinusCrnView);
               // console.log(" i = " + i);
              //  console.log(calcCostVfromVCacheMinusCrnView);
                let cost = 0;
                if (i > 0) {
                    let crnGB = calcCostVfromVCache[i];
                    for (let k = 0; k < calcCostVfromVCacheMinusCrnView.length; k++) {
                        let crnMinus = calcCostVfromVCacheMinusCrnView[k];
                        if (crnGB.id === crnMinus.id) {
                            cost = freq * (crnGB.calculationCost - crnMinus.calculationCost);
                            allGroupBys[i].cacheEvictionCost = cost;
                            console.log("cost = " + cost);
                        }
                    }
                } else {
                    allGroupBys[i].cacheEvictionCost = 1500;
                }
              //  allGroupBys[i].cacheEvictionCost = cost;
                console.log("|||||||||");
                console.log(allGroupBys[i]);
            }
            for(let p = 0; p < allMinus.length; p++){
                console.log("**** MINUS");
                let crnMinus = allMinus[p];
                console.log(crnMinus);

            }

            for(let p = 0; p < allGroupBys.length; p++){
                let crnGB = allGroupBys[p];

            }


            // για καθε ένα array που λείπει ένα cached view πρεπει να βρω ένα που έχει όλα τα άλλα views και να αφαιρέσω
        } else {
            allGroupBys[0].cacheEvictionCost = 1000;
        }
    });
    if (allGroupBys.length === 1) {
        allGroupBys[0].cacheEvictionCost = 1000;
    }
    return allGroupBys;
}

function calculationCostOfficial (groupBys, latestFact) { // the function we write on paper
    // where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    // which is the cost to materialize view V from view Vi (where V < Vi)
    let a = 10; // factor of deltas
    let sizeDeltas = 0;
    let sizeCached = 0; // crnGroubBy
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        sizeDeltas = latestFact - Number.parseInt(crnGroupBy.latestFact); // latestFact is the latest fact written in bc
        sizeCached = Number.parseInt(crnGroupBy.size);
        let crnCost = a * sizeDeltas + sizeCached;
        crnGroupBy.calculationCost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function calculateNewGroupBy (facts, operation, gbFields, aggregationField, callback) {
    connection.query('DROP TABLE IF EXISTS ' + tableName, function (err) {
        if (err) {
            console.log(err);
            callback(null, err);
        }
        connection.query(createTable, function (error, results) { // creating the SQL table for 'Fact Table'
            if (error) {
                console.log(error);
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
                    console.log(error);
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
                        console.log(error);
                        callback(null, error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        if (err) {
                            console.log(err);
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
    console.log('TABLE NAME = ' + tableName);
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
                console.log(error);
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
                    console.log(error);
                    callback(null, error);
                }
                connection.query('DROP TABLE ' + tableName, function (err) {
                    if (err) {
                        console.log(err);
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
            console.log(error);
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
                console.log(err);
                callback(null, err);
            }
            connection.query(editedQueryB, function (err, results, fields) {
                if (err) {
                    console.log(err);
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
                        console.log(error);
                        callback(null, error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        if (err) {
                            console.log(err);
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

app.get('/getViewByName/:viewName', function (req, res) {
    let totalStart = microtime.nowDouble();
    let factTbl = require('./templates/ABCD');

    // let factTbl = require('./templates/new_sales_min');
    let viewsDefined = factTbl.views;
    console.log(req.params.viewName);
    let found = false;
    let view = {};
    for (let crnView in viewsDefined) {
        if (factTbl.views[crnView].name === req.params.viewName) {
            found = true;
            view = factTbl.views[crnView];
            factTbl.views[crnView].frequency = factTbl.views[crnView].frequency + 1;
            fs.writeFile('./templates/ABCD.json', JSON.stringify(factTbl,null, 2), function (err) {
                if (err) return console.log(err);
                console.log('updated frequency');
            });
            break;
        }
    }

    if (!found) {
        return res.send({ error: 'view not found' });
    }

    console.log('VIEW BY NAME ENDPOINT HIT AGAIN');
    if (!gbRunning && !running) {
        gbRunning = true;
        let gbFields = [];
        if (view.gbFields.indexOf('|') > -1) {
            // more than 1 group by fields
            gbFields = view.gbFields.split('|');
        } else {
            if (Array.isArray(view.gbFields)) {
                gbFields = view.gbFields;
            } else {
                gbFields.push(view.gbFields);
            }
        }
        view.gbFields = gbFields;
        for (let index in view.gbFields) {
            view.gbFields[index] = view.gbFields[index].trim();
        }
        if (contract) {
            if (config.cacheEnabled) {
                console.log('cache enabled = TRUE');
                let getGroupIdTimeStart = microtime.nowDouble();
                contract.methods.groupId().call(function (err, result) {
                    let getGroupIdTimeEnd = microtime.nowDouble();
                    let getGroupIdTime = getGroupIdTimeEnd - getGroupIdTimeStart;
                    if (!err) {
                        if (result > 0) { // At least one group by already exists
                            let getAllGBsFromBCTimeStart = microtime.nowDouble();
                            contract.methods.getAllGroupBys(result).call(async function (err, resultGB) {
                                let getAllGBsFromBCTimeEnd = microtime.nowDouble();
                                let getAllGBsTime = getAllGBsFromBCTimeEnd - getAllGBsFromBCTimeStart;
                                if (!err) {
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

                                    transformedArray = helper.containsAllFields(transformedArray, view); // assigns the containsAllFields value
                                    let filteredGBs = [];
                                    let sortedByEvictionCost = [];
                                    for (let i = 0; i < transformedArray.length; i++) { // filter out the group bys that DO NOT CONTAIN all the fields we need -> aka containsAllFields = false
                                        if (transformedArray[i].containsAllFields) {
                                            filteredGBs.push(transformedArray[i]);
                                        }
                                        sortedByEvictionCost.push(transformedArray[i]);
                                    }

                                    await contract.methods.dataId().call(function (err, latestId) {
                                        if (err) throw err;
                                        console.log("_________________________________");
                                        sortedByEvictionCost = myFunc(sortedByEvictionCost,latestId, req.params.viewName);
                                        console.log(sortedByEvictionCost);
                                        //  sortedByEvictionCost = cacheEvictionCostOfficial(sortedByEvictionCost, latestId);
                                        console.log('cache eviction costs assigned:');
                                        console.log(sortedByEvictionCost);
                                        filteredGBs = calculationCostOfficial(filteredGBs, latestId); // the cost to materialize the view from each view cached
                                    });

                                    await sortedByEvictionCost.sort(function (a, b) {
                                        if (config.cacheEvictionPolicy === 'FIFO') {
                                            return parseInt(a.gbTimestamp) - parseInt(b.gbTimestamp);
                                        } else if (config.cacheEvictionPolicy === 'COST FUNCTION') {
                                            console.log('SORT WITH COST FUNCTION');
                                            return parseFloat(a.cacheEvictionCost) - parseFloat(b.cacheEvictionCost);
                                        }
                                    });

                                    console.log('SORTED GBs by eviction cost:');
                                    console.log(sortedByEvictionCost); // TS ORDER ascending, the first ones are less 'expensive' than the last ones.
                                    console.log('________________________');
                                    // assign costs
                                    // filteredGBs = calculationCostOfficial(filteredGBs, latestId);
                                    if (filteredGBs.length > 0) {
                                        // pick the one with the less cost
                                        filteredGBs.sort(function (a, b) {
                                            return parseFloat(a.calculationCost) - parseFloat(b.calculationCost)
                                        }); // order ascending
                                        let mostEfficient = filteredGBs[0]; // TODO: check what we do in case we have no groub bys that match those criteria
                                   //     console.log('MOST EFFICIENT IS:');
                                   //     console.log(mostEfficient);
                                        let getLatestFactIdTimeStart = microtime.nowDouble();
                                        contract.methods.dataId().call(function (err, latestId) {
                                            console.log('LATEST ID IS:');
                                            console.log(latestId);
                                            let getLatestFactIdTimeEnd = microtime.nowDouble();
                                            let getLatestFactIdTime = getLatestFactIdTimeEnd - getLatestFactIdTimeStart;
                                            if (err) {
                                                console.log(err);
                                                gbRunning = false;
                                                return res.send(err);
                                            }
                                            if (mostEfficient.gbTimestamp > 0) {
                                                let getLatestFactTimeStart = microtime.nowDouble();
                                                contract.methods.getFact(latestId - 1).call(function (err, latestFact) {
                                                    let getLatestFactTimeEnd = microtime.nowDouble();
                                                    let getLatestFactTime = getLatestFactTimeEnd - getLatestFactTimeStart;
                                                    if (err) {
                                                        console.log(err);
                                                        gbRunning = false;
                                                        return res.send(err);
                                                    }
                                                    console.log(latestFact);
                                                    if (mostEfficient.gbTimestamp > latestFact.timestamp) {
                                                        console.log('NO NEW FACTS');
                                                        // NO NEW FACTS after the latest group by
                                                        // -> incrementally calculate the groupby requested by summing the one in redis cache
                                                        let hashId = mostEfficient.hash.split('_')[1];
                                                        let hashBody = mostEfficient.hash.split('_')[0];
                                                        let allHashes = [];
                                                        for (let i = 0; i <= hashId; i++) {
                                                            allHashes.push(hashBody + '_' + i);
                                                        }
                                                        let cacheRetrieveTimeStart = microtime.nowDouble();
                                                        cacheController.getManyCachedResults(allHashes, function (error, allCached) {
                                                            let cacheRetrieveTimeEnd = microtime.nowDouble();
                                                            if (error) {
                                                                console.log(error);
                                                                gbRunning = false;
                                                                return res.send(error);
                                                            }
                                                            let cachedGroupBy = {};
                                                            if (allCached.length === 1) { // it is <= of slice size, so it is not sliced
                                                                cachedGroupBy = JSON.parse(allCached[0]);
                                                            } else { // it is sliced
                                                                let mergedArray = [];
                                                                for (const index in allCached) {
                                                                    let crnSub = allCached[index];
                                                                    let crnSubArray = JSON.parse(crnSub);
                                                                    for (const kv in crnSubArray) {
                                                                        if (kv !== 'operation' && kv !== 'groupByFields' && kv !== 'field' && kv !== 'viewName') {
                                                                            mergedArray.push(crnSubArray[kv]);
                                                                        } else {
                                                                            for (const meta in crnSubArray) {
                                                                                mergedArray.push({[meta]: crnSubArray[meta]});
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
                                                                cachedGroupBy = gbFinal;
                                                            }

                                                            if (err) {
                                                                console.log(error);
                                                                gbRunning = false;
                                                                return res.send(error);
                                                            }
                                                            if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                                                // this means we want to calculate a different group by than the stored one
                                                                // but however it can be calculated just from redis cache
                                                                if (cachedGroupBy.field === view.aggregationField &&
                                                                    view.operation === cachedGroupBy.operation) {
                                                                    // caclculating the reduced Group By in SQL
                                                                    console.log(cachedGroupBy);
                                                                    let tableName = cachedGroupBy.gbCreateTable.split(' ');
                                                                    tableName = tableName[3];
                                                                    tableName = tableName.split('(')[0];
                                                                    console.log('TABLE NAME = ' + tableName);
                                                                    let sqlTimeStartCT = microtime.nowDouble();
                                                                    connection.query(cachedGroupBy.gbCreateTable, async function (error, results, fields) {
                                                                        let sqlTimeEndCT = microtime.nowDouble();
                                                                        if (error) throw error;
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
                                                                        let sqlTimeStartInsert = microtime.nowDouble();
                                                                        await connection.query(editedQuery, async function (error, results, fields) {
                                                                            let sqlTimeEndInsert = microtime.nowDouble();
                                                                            if (error) {
                                                                                console.log(error);
                                                                                throw error;
                                                                            }
                                                                            let op = '';
                                                                            let gbQuery = {};
                                                                            if (view.operation === 'SUM' || view.operation === 'COUNT') {
                                                                                op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
                                                                            } else if (view.operation === 'MIN') {
                                                                                op = 'MIN'
                                                                            } else if (view.operation === 'MAX') {
                                                                                op = 'MAX';
                                                                            }
                                                                            gbQuery = jsonSql.build({
                                                                                type: 'select',
                                                                                table: tableName,
                                                                                group: gbFields,
                                                                                fields: [gbFields,
                                                                                    {
                                                                                        func: {
                                                                                            name: op,
                                                                                            args: [{field: lastCol}]
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
                                                                                                args: [{field: prelastCol}]
                                                                                            }
                                                                                        },
                                                                                        {
                                                                                            func: {
                                                                                                name: 'SUM',
                                                                                                args: [{field: lastCol}]
                                                                                            }
                                                                                        }]
                                                                                });
                                                                            }
                                                                            let editedGBQuery = gbQuery.query.replace(/"/g, '');
                                                                            editedGBQuery = editedGBQuery.replace(/''/g, 'null');
                                                                            let sqlTimeStartSelect = microtime.nowDouble();
                                                                            await connection.query(editedGBQuery, async function (error, results, fields) {
                                                                                let sqlTimeEndSelect = microtime.nowDouble();
                                                                                if (error) {
                                                                                    console.log(error);
                                                                                    throw error;
                                                                                }
                                                                                let sqlTimeStartDrop = microtime.nowDouble();
                                                                                await connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                                                                                    let sqlTimeEndDrop = microtime.nowDouble();
                                                                                    if (err) {
                                                                                        console.log(err);
                                                                                        throw err;
                                                                                    }

                                                                                    let groupBySqlResult = {};
                                                                                    if (view.operation === 'AVERAGE') {
                                                                                        groupBySqlResult = transformations.transformReadyAverage(results, view.gbFields, view.aggregationField);
                                                                                    } else {
                                                                                        groupBySqlResult = transformations.transformGBFromSQL(results, op, lastCol, gbFields);
                                                                                    }
                                                                                    groupBySqlResult.operation = view.operation;
                                                                                    groupBySqlResult.field = view.aggregationField;
                                                                                    groupBySqlResult.viewName = req.params.viewName;
                                                                                    let cacheSaveTimeStart = microtime.nowDouble();
                                                                                    return cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                        console.log('error:', err);
                                                                                        gbRunning = false;
                                                                                        return res.send(err);
                                                                                    }).on('transactionHash', (err) => {
                                                                                        console.log('transactionHash:', err);
                                                                                    }).on('receipt', (receipt) => {
                                                                                        console.log('receipt:', receipt);
                                                                                        let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                            cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                    let totalEnd = microtime.nowDouble();
                                                                                                    if (!err) {
                                                                                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                        groupBySqlResult.sqlTime = (sqlTimeEndCT - sqlTimeStartCT) + (sqlTimeEndInsert - sqlTimeStartInsert) + (sqlTimeEndSelect - sqlTimeStartSelect) + (sqlTimeEndDrop - sqlTimeStartDrop);
                                                                                                        groupBySqlResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                        groupBySqlResult.totalTime = groupBySqlResult.cacheSaveTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheRetrieveTime;
                                                                                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                                        helper.printTimes(groupBySqlResult);
                                                                                                        io.emit('view_results', stringify(groupBySqlResult).replace('\\', ''));
                                                                                                        gbRunning = false;
                                                                                                        return res.send(stringify(groupBySqlResult).replace('\\', ''));
                                                                                                    }
                                                                                                    gbRunning = false;
                                                                                                    return res.send(err);
                                                                                                });
                                                                                            });
                                                                                        } else {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                            groupBySqlResult.sqlTime = (sqlTimeEndCT - sqlTimeStartCT) + (sqlTimeEndInsert - sqlTimeStartInsert) + (sqlTimeEndSelect - sqlTimeStartSelect) + (sqlTimeEndDrop - sqlTimeStartDrop);
                                                                                            groupBySqlResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                            groupBySqlResult.totalTime = groupBySqlResult.cacheSaveTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheRetrieveTime;
                                                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                            helper.printTimes(groupBySqlResult);
                                                                                            io.emit('view_results', stringify(groupBySqlResult).replace('\\', ''));
                                                                                            gbRunning = false;
                                                                                            return res.send(stringify(groupBySqlResult).replace('\\', ''));
                                                                                        }
                                                                                    });
                                                                                });
                                                                            });
                                                                        });
                                                                    });
                                                                } else {
                                                                    // some fields contained in a Group by but operation and aggregation fields differ
                                                                    // this means we should proceed to new group by calculation from the begining
                                                                    let bcTimeStart = microtime.nowDouble();
                                                                    contractController.getAllFactsHeavy(latestId).then(retval => {
                                                                        let bcTimeEnd = microtime.nowDouble();
                                                                        for (let i = 0; i < retval.length; i++) {
                                                                            delete retval[i].timestamp;
                                                                        }
                                                                        console.log('CALCULATING NEW GB FROM BEGINING');
                                                                        let sqlTimeStart = microtime.nowDouble();
                                                                        calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                            let sqlTimeEnd = microtime.nowDouble();
                                                                            if (error) {
                                                                                gbRunning = false;
                                                                                return res.send(error);
                                                                            }
                                                                            groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                            groupBySqlResult.field = view.aggregationField;
                                                                            groupBySqlResult.viewName = req.params.viewName;
                                                                            let cacheSaveTimeStart = microtime.nowDouble();
                                                                            cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                console.log('error:', err);
                                                                                gbRunning = false;
                                                                                return res.send(err);
                                                                            }).on('transactionHash', (err) => {
                                                                                console.log('transactionHash:', err);
                                                                            }).on('receipt', (receipt) => {
                                                                                let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                delete groupBySqlResult.gbCreateTable;
                                                                                if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                    cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                        contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            if (!err) {
                                                                                                groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                                groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                                groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                                                groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                                helper.printTimes(groupBySqlResult);
                                                                                                console.log('receipt:', receipt);
                                                                                                io.emit('view_results', stringify(groupBySqlResult));
                                                                                                gbRunning = false;
                                                                                                return res.send(stringify(groupBySqlResult));
                                                                                            }
                                                                                            gbRunning = false;
                                                                                            return res.send(err);
                                                                                        });
                                                                                    });
                                                                                } else {
                                                                                    let totalEnd = microtime.nowDouble();
                                                                                    groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                    groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                    groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                    groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                                    groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                    helper.printTimes(groupBySqlResult);
                                                                                    console.log('receipt:', receipt);
                                                                                    io.emit('view_results', stringify(groupBySqlResult));
                                                                                    gbRunning = false;
                                                                                    return res.send(stringify(groupBySqlResult));
                                                                                }
                                                                            });
                                                                        });
                                                                    });
                                                                }
                                                            } else {
                                                                if (cachedGroupBy.field === view.aggregationField &&
                                                                    view.operation === cachedGroupBy.operation) {
                                                                    let totalEnd = microtime.nowDouble();
                                                                    // this means we just have to return the group by stored in cache
                                                                    // field, operation are same and no new records written
                                                                    cachedGroupBy.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                    cachedGroupBy.totalTime = cachedGroupBy.cacheRetrieveTime;
                                                                    cachedGroupBy.allTotal = totalEnd - totalStart;
                                                                    io.emit('view_results', stringify(cachedGroupBy));
                                                                    gbRunning = false;
                                                                    return res.send(stringify(cachedGroupBy));
                                                                } else {
                                                                    // same fields but different operation or different aggregate field
                                                                    // this means we should proceed to new group by calculation from the begining
                                                                    let bcTimeStart = microtime.nowDouble();
                                                                    contractController.getAllFactsHeavy(latestId).then(retval => {
                                                                        let bcTimeEnd = microtime.nowDouble();
                                                                        for (let i = 0; i < retval.length; i++) {
                                                                            delete retval[i].timestamp;
                                                                        }
                                                                        console.log('CALCULATING NEW GB FROM BEGGINING');
                                                                        let sqlTimeStart = microtime.nowDouble();
                                                                        calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                            let sqlTimeEnd = microtime.nowDouble();
                                                                            if (error) {
                                                                                gbRunning = false;
                                                                                return res.send(error);
                                                                            }
                                                                            groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                            groupBySqlResult.field = view.aggregationField;
                                                                            groupBySqlResult.viewName = req.params.viewName;
                                                                            let cacheSaveTimeStart = microtime.nowDouble();
                                                                            cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                console.log('error:', err);
                                                                                gbRunning = false;
                                                                                return res.send(err);
                                                                            }).on('transactionHash', (err) => {
                                                                                console.log('transactionHash:', err);
                                                                            }).on('receipt', (receipt) => {
                                                                                let cachSaveTimeEnd = microtime.nowDouble();
                                                                                delete groupBySqlResult.gbCreateTable;
                                                                                if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                    cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                        contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            if (!err) {
                                                                                                groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                                groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                                groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                                                groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                                                groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                                helper.printTimes(groupBySqlResult);
                                                                                                console.log('receipt:', receipt);
                                                                                                io.emit('view_results', stringify(groupBySqlResult));
                                                                                                gbRunning = false;
                                                                                                return res.send(stringify(groupBySqlResult));
                                                                                            }
                                                                                            gbRunning = false;
                                                                                            return res.send(err);
                                                                                        });
                                                                                    });
                                                                                } else {
                                                                                    let totalEnd = microtime.nowDouble();
                                                                                    groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                    groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                    groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                                    groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                                    groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                    helper.printTimes(groupBySqlResult);
                                                                                    console.log('receipt:', receipt);
                                                                                    io.emit('view_results', stringify(groupBySqlResult));
                                                                                    gbRunning = false;
                                                                                    return res.send(stringify(groupBySqlResult));
                                                                                }
                                                                            });
                                                                        });
                                                                    });
                                                                }
                                                            }
                                                        });
                                                    } else {
                                                        console.log('DELTAS DETECTED');
                                                        // we have deltas -> we fetch them
                                                        // CALCULATING THE VIEW JUST FOR THE DELTAS
                                                        // THEN MERGE IT WITH THE ONES IN CACHE
                                                        // THEN SAVE BACK IN CACHE
                                                        let bcTimeStart = microtime.nowDouble();
                                                        contractController.getFactsFromTo(mostEfficient.latestFact, latestId - 1).then(deltas => {
                                                            let bcTimeEnd = microtime.nowDouble();
                                                            connection.query(createTable, function (error, results, fields) {
                                                                if (error) throw error;
                                                                deltas = helper.removeTimestamps(deltas);
                                                                console.log('CALCULATING GB FOR DELTAS:');
                                                                let sqlTimeStart = microtime.nowDouble();
                                                                calculateNewGroupBy(deltas, view.operation, view.gbFields, view.aggregationField, async function (groupBySqlResult, error) {
                                                                    let sqlTimeEnd = microtime.nowDouble();
                                                                    if (error) {
                                                                        gbRunning = false;
                                                                        return res.send(error);
                                                                    }
                                                                    let hashId = mostEfficient.hash.split('_')[1];
                                                                    let hashBody = mostEfficient.hash.split('_')[0];
                                                                    let allHashes = [];
                                                                    for (let i = 0; i <= hashId; i++) {
                                                                        allHashes.push(hashBody + '_' + i);
                                                                    }

                                                                    let cacheRetrieveTimeStart = microtime.nowDouble();
                                                                    cacheController.getManyCachedResults(allHashes, async function (error, allCached) {
                                                                        let cacheRetrieveTimeEnd = microtime.nowDouble();
                                                                        if (error) {
                                                                            console.log(error);
                                                                            gbRunning = false;
                                                                            return res.send(error);
                                                                        }

                                                                        let cachedGroupBy = {};
                                                                        if (allCached.length === 1) { // it is <= of slice size, so it is not sliced
                                                                            console.log('IT IS NOT SLICED');
                                                                            cachedGroupBy = JSON.parse(allCached[0]);
                                                                        } else { // it is sliced
                                                                            console.log('IT IS SLICED');
                                                                            let mergedArray = [];
                                                                            for (const index in allCached) {
                                                                                let crnSub = allCached[index];
                                                                                let crnSubArray = JSON.parse(crnSub);
                                                                                for (const kv in crnSubArray) {
                                                                                    if (kv !== 'operation' && kv !== 'groupByFields' && kv !== 'field' && kv !== 'viewName') {
                                                                                        mergedArray.push(crnSubArray[kv]);
                                                                                    } else {
                                                                                        for (const meta in crnSubArray) {
                                                                                            mergedArray.push({[meta]: crnSubArray[meta]});
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
                                                                            cachedGroupBy = gbFinal;
                                                                        }

                                                                        if (cachedGroupBy.field === view.aggregationField &&
                                                                            view.operation === cachedGroupBy.operation) {
                                                                            if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                                                                let reductionTimeStart = microtime.nowDouble();
                                                                                calculateReducedGroupBy(cachedGroupBy, view, gbFields, async function (reducedResult, error) {
                                                                                    let reductionTimeEnd = microtime.nowDouble();
                                                                                    if (error) {
                                                                                        gbRunning = false;
                                                                                        return res.send(error);
                                                                                    }

                                                                                    let viewNameSQL = view.SQLTable.split(' ');
                                                                                    viewNameSQL = viewNameSQL[3];
                                                                                    viewNameSQL = viewNameSQL.split('(')[0];

                                                                                    let rows = [];
                                                                                    let rowsDelta = [];
                                                                                    let lastCol = '';
                                                                                    let prelastCol = null; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                                    lastCol = view.SQLTable.split(' ');
                                                                                    prelastCol = lastCol[lastCol.length - 4];
                                                                                    lastCol = lastCol[lastCol.length - 2];

                                                                                    // MERGE reducedResult with groupBySQLResult
                                                                                    let op = '';
                                                                                    if (view.operation === 'SUM' || view.operation === 'COUNT') {
                                                                                        op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
                                                                                    } else if (view.operation === 'MIN') {
                                                                                        op = 'MIN'
                                                                                    } else if (view.operation === 'MAX') {
                                                                                        op = 'MAX';
                                                                                    }

                                                                                    reducedResult = transformations.transformGBFromSQL(reducedResult, op, lastCol, gbFields);
                                                                                    reducedResult.field = view.aggregationField;
                                                                                    reducedResult.viewName = req.params.viewName;
                                                                                    let gbValsReduced = Object.values(reducedResult);

                                                                                    for (let i = 0, keys = Object.keys(reducedResult); i < keys.length; i++) {
                                                                                        let key = keys[i];
                                                                                        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
                                                                                            let crnRow = JSON.parse(key);
                                                                                            lastCol = view.SQLTable.split(' ');
                                                                                            prelastCol = lastCol[lastCol.length - 4];
                                                                                            lastCol = lastCol[lastCol.length - 2];
                                                                                            if (view.operation === 'AVERAGE') {
                                                                                                crnRow[prelastCol] = gbValsReduced[i]['sum'];
                                                                                                crnRow[lastCol] = gbValsReduced[i]['count'];
                                                                                            } else {
                                                                                                crnRow[lastCol] = gbValsReduced[i];
                                                                                            }
                                                                                            rows.push(crnRow);
                                                                                        }
                                                                                    }

                                                                                    let gbValsSqlRes = Object.values(groupBySqlResult);
                                                                                    for (let i = 0, keys = Object.keys(groupBySqlResult); i < keys.length; i++) {
                                                                                        let key = keys[i];
                                                                                        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
                                                                                            let crnRow = JSON.parse(key);
                                                                                            lastCol = view.SQLTable.split(' ');
                                                                                            prelastCol = lastCol[lastCol.length - 4];
                                                                                            lastCol = lastCol[lastCol.length - 2];
                                                                                            if (view.operation === 'AVERAGE') {
                                                                                                crnRow[prelastCol] = gbValsSqlRes[i]['sum'];
                                                                                                crnRow[lastCol] = gbValsSqlRes[i]['count'];
                                                                                            } else {
                                                                                                crnRow[lastCol] = gbValsSqlRes[i];
                                                                                            }
                                                                                            rowsDelta.push(crnRow);
                                                                                        }
                                                                                    }

                                                                                    let mergeTimeStart = microtime.nowDouble();
                                                                                    mergeGroupBys(rows, rowsDelta, view.SQLTable, viewNameSQL, view, lastCol, prelastCol, function (mergeResult, error) {
                                                                                        let mergeTimeEnd = microtime.nowDouble();
                                                                                        if (error) {
                                                                                            gbRunning = false;
                                                                                            return res.send(error);
                                                                                        }
                                                                                        mergeResult.operation = view.operation;
                                                                                        mergeResult.field = view.aggregationField;
                                                                                        mergeResult.gbCreateTable = view.SQLTable;
                                                                                        mergeResult.viewName = req.params.viewName;
                                                                                        // save on cache before return
                                                                                        let cacheSaveTimeStart = microtime.nowDouble();
                                                                                        cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                            console.log('error:', err);
                                                                                            gbRunning = false;
                                                                                            return res.send(err);
                                                                                        }).on('transactionHash', (err) => {
                                                                                            console.log('transactionHash:', err);
                                                                                        }).on('receipt', (receipt) => {
                                                                                            let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                            delete mergeResult.gbCreateTable;
                                                                                            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                                cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                                    contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                        let totalEnd = microtime.nowDouble();
                                                                                                        if (!err) {
                                                                                                            mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                                            mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                                            mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                            mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                            mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                            mergeResult.allTotal = totalEnd - totalStart;
                                                                                                            helper.printTimes(mergeResult);
                                                                                                            console.log('receipt:', receipt);
                                                                                                            io.emit('view_results', mergeResult);
                                                                                                            gbRunning = false;
                                                                                                            return res.send(mergeResult);
                                                                                                        }
                                                                                                        gbRunning = false;
                                                                                                        return res.send(err);
                                                                                                    });
                                                                                                });
                                                                                            } else {
                                                                                                let totalEnd = microtime.nowDouble();
                                                                                                mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                                mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getLatestFactIdTime + getLatestFactTime + getAllGBsTime;
                                                                                                mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                mergeResult.allTotal = totalEnd - totalStart;
                                                                                                helper.printTimes(mergeResult);
                                                                                                console.log('receipt:', receipt);
                                                                                                io.emit('view_results', mergeResult);
                                                                                                gbRunning = false;
                                                                                                return res.send(mergeResult);
                                                                                            }
                                                                                        });
                                                                                    });
                                                                                });
                                                                            } else {
                                                                                console.log('GB FIELDS OF DELTAS AND CACHED ARE THE SAME');
                                                                                // group by fields of deltas and cached are the same so
                                                                                // MERGE cached and groupBySqlResults
                                                                                let viewNameSQL = view.SQLTable.split(' ');
                                                                                viewNameSQL = viewNameSQL[3];
                                                                                viewNameSQL = viewNameSQL.split('(')[0];

                                                                                let rows = [];
                                                                                let rowsDelta = [];
                                                                                let lastCol = '';
                                                                                let prelastCol = null; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                                let gbValsCached = Object.values(cachedGroupBy);
                                                                                lastCol = view.SQLTable.split(' ');
                                                                                prelastCol = lastCol[lastCol.length - 4];
                                                                                lastCol = lastCol[lastCol.length - 2];

                                                                                for (let i = 0, keys = Object.keys(cachedGroupBy); i < keys.length; i++) {
                                                                                    let key = keys[i];
                                                                                    if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
                                                                                        let crnRow = JSON.parse(key);
                                                                                        if (view.operation === 'AVERAGE') {
                                                                                            crnRow[prelastCol] = gbValsCached[i]['sum'];
                                                                                            crnRow[lastCol] = gbValsCached[i]['count'];
                                                                                        } else {
                                                                                            crnRow[lastCol] = gbValsCached[i];
                                                                                        }
                                                                                        rows.push(crnRow);
                                                                                    }
                                                                                }

                                                                                let gbSqlVals = Object.values(groupBySqlResult);
                                                                                for (let i = 0, keys = Object.keys(groupBySqlResult); i < keys.length; i++) {
                                                                                    let key = keys[i];
                                                                                    if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
                                                                                        let crnRow = JSON.parse(key);
                                                                                        if (view.operation === 'AVERAGE') {
                                                                                            crnRow[prelastCol] = gbSqlVals[i]['sum'];
                                                                                            crnRow[lastCol] = gbSqlVals[i]['count'];
                                                                                        } else {
                                                                                            crnRow[lastCol] = gbSqlVals[i];
                                                                                        }
                                                                                        rowsDelta.push(crnRow);
                                                                                    }
                                                                                }
                                                                                let mergeTimeStart = microtime.nowDouble();
                                                                                mergeGroupBys(rows, rowsDelta, view.SQLTable, viewNameSQL, view, lastCol, prelastCol, function (mergeResult, error) {
                                                                                    let mergeTimeEnd = microtime.nowDouble();
                                                                                    // SAVE ON CACHE BEFORE RETURN
                                                                                    console.log('SAVE ON CACHE BEFORE RETURN');
                                                                                    if (error) {
                                                                                        gbRunning = false;
                                                                                        return res.send(error);
                                                                                    }
                                                                                    mergeResult.operation = view.operation;
                                                                                    mergeResult.field = view.aggregationField;
                                                                                    mergeResult.gbCreateTable = view.SQLTable;
                                                                                    mergeResult.viewName = req.params.viewName;
                                                                                    let cacheSaveTimeStart = microtime.nowDouble();
                                                                                    cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                        console.log('error:', err);
                                                                                        gbRunning = false;
                                                                                        return res.send(err);
                                                                                    }).on('transactionHash', (err) => {
                                                                                        console.log('transactionHash:', err);
                                                                                    }).on('receipt', (receipt) => {
                                                                                        let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                        delete mergeResult.gbCreateTable;
                                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                            cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                    let totalEnd = microtime.nowDouble();
                                                                                                    if (!err) {
                                                                                                        mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getAllGBsTime + getLatestFactIdTime + getLatestFactTime;
                                                                                                        mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                                        mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                        mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                        mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                        mergeResult.allTotal = totalEnd - totalStart;
                                                                                                        helper.printTimes(mergeResult);
                                                                                                        console.log('receipt:', receipt);
                                                                                                        io.emit('view_results', mergeResult);
                                                                                                        gbRunning = false;
                                                                                                        return res.send(mergeResult);
                                                                                                    }
                                                                                                    gbRunning = false;
                                                                                                    return res.send(err);
                                                                                                });
                                                                                            });
                                                                                        } else {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getAllGBsTime + getLatestFactIdTime + getLatestFactTime;
                                                                                            mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                            mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                            mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                            mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                            mergeResult.allTotal = totalEnd - totalStart;
                                                                                            helper.printTimes(mergeResult);
                                                                                            console.log('receipt:', receipt);
                                                                                            io.emit('view_results', mergeResult);
                                                                                            gbRunning = false;
                                                                                            return res.send(mergeResult);
                                                                                        }
                                                                                    });
                                                                                });
                                                                            }
                                                                        }
                                                                    });
                                                                });
                                                            });
                                                        });
                                                    }
                                                });
                                                // return res.send({allGbs: filteredGBs, mostEfficient: mostEfficient});
                                            }
                                        });
                                    } else {
                                        // NO FILTERED GBS FOUND, RUNNING GB FROM BEGGINING
                                        let getLatestFactIdTimeStart = microtime.nowDouble();
                                        contract.methods.dataId().call(function (err, latestId) {
                                            console.log('LATEST ID IS:');
                                            console.log(latestId);
                                            let getLatestFactIdTimeEnd = microtime.nowDouble();
                                            let getLatestFactIdTime = getLatestFactIdTimeEnd - getLatestFactIdTimeStart;
                                            if (err) {
                                                console.log(err);
                                                gbRunning = false;
                                                return res.send(err);
                                            }
                                            let bcTimeStart = microtime.nowDouble();
                                            contractController.getAllFactsHeavy(latestId).then(retval => {
                                                let bcTimeEnd = microtime.nowDouble();
                                                for (let i = 0; i < retval.length; i++) {
                                                    delete retval[i].timestamp;
                                                }
                                                console.log('CALCULATING NEW GB FROM BEGGINING');
                                                let sqlTimeStart = microtime.nowDouble();
                                                calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                    let sqlTimeEnd = microtime.nowDouble();
                                                    if (error) {
                                                        gbRunning = false;
                                                        return res.send(error);
                                                    }
                                                    groupBySqlResult.gbCreateTable = view.SQLTable;
                                                    groupBySqlResult.field = view.aggregationField;
                                                    groupBySqlResult.viewName = req.params.viewName;
                                                    let cacheSaveTimeStart = microtime.nowDouble();
                                                    cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                        console.log('error:', err);
                                                        gbRunning = false;
                                                        return res.send(err);
                                                    }).on('transactionHash', (err) => {
                                                        console.log('transactionHash:', err);
                                                    }).on('receipt', (receipt) => {
                                                        let cacheSaveTimeEnd = microtime.nowDouble();
                                                        delete groupBySqlResult.gbCreateTable;
                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                            cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                    let totalEnd = microtime.nowDouble();
                                                                    if (!err) {
                                                                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                        groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                        helper.printTimes(groupBySqlResult);
                                                                        console.log('receipt:', receipt);
                                                                        io.emit('view_results', stringify(groupBySqlResult));
                                                                        gbRunning = false;
                                                                        return res.send(stringify(groupBySqlResult));
                                                                    }
                                                                    gbRunning = false;
                                                                    return res.send(err);
                                                                });
                                                            });
                                                        } else {
                                                            let totalEnd = microtime.nowDouble();
                                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                            groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                            helper.printTimes(groupBySqlResult);
                                                            console.log('receipt:', receipt);
                                                            io.emit('view_results', stringify(groupBySqlResult));
                                                            gbRunning = false;
                                                            return res.send(stringify(groupBySqlResult));
                                                        }
                                                    });
                                                });
                                            });
                                        });
                                    }
                                } else {
                                    console.log(err);
                                    gbRunning = false;
                                    return res.send(err);
                                }
                            });
                        } else {
                            // No group bys exist in cache, we are in the initial state
                            // this means we should proceed to new group by calculation from the begining
                            let bcTimeStart = microtime.nowDouble();
                            contract.methods.dataId().call(function (err, latestId) {
                                if (err) throw err;
                                contractController.getAllFactsHeavy(latestId).then(retval => {
                                    let bcTimeEnd = microtime.nowDouble();
                                    if (retval.length === 0) {
                                        gbRunning = false;
                                        return res.send(stringify({ error: 'No facts exist in blockchain' }));
                                    }
                                    let facts = helper.removeTimestamps(retval);
                                    console.log('CALCULATING NEW GB FROM BEGINING');
                                    let sqlTimeStart = microtime.nowDouble();
                                    calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                        let sqlTimeEnd = microtime.nowDouble();
                                        if (error) {
                                            gbRunning = false;
                                            return res.send(stringify(error))
                                        }
                                        groupBySqlResult.gbCreateTable = view.SQLTable;
                                        groupBySqlResult.field = view.aggregationField;
                                        groupBySqlResult.viewName = req.params.viewName;
                                        let cacheSaveTimeStart = microtime.nowDouble();
                                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                            console.log('error:', err);
                                            gbRunning = false;
                                            return res.send(err);
                                        }).on('transactionHash', (err) => {
                                            console.log('transactionHash:', err);
                                        }).on('receipt', (receipt) => {
                                            let cacheSaveTimeEnd = microtime.nowDouble();
                                            delete groupBySqlResult.gbCreateTable;
                                            let totalEnd = microtime.nowDouble();
                                            console.log('receipt:', receipt);
                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                            groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                            helper.printTimes(groupBySqlResult);
                                            io.emit('view_results', stringify(groupBySqlResult));
                                            gbRunning = false;
                                            return res.send(stringify(groupBySqlResult));
                                        });
                                    });
                                });
                            });
                        }
                    } else {
                        console.log(err);
                        gbRunning = false;
                        return res.send(err);
                    }
                });
            } else {
                console.log('cache enabled = FALSE');
                // cache not enabled, so just fetch everything everytime from blockchain and then make calculation in sql
                // just like the case that the cache is originally empty
                let bcTimeStart = microtime.nowDouble();
                contract.methods.dataId().call(function (err, latestId) {
                    if (err) throw err;
                    contractController.getAllFacts(latestId).then(retval => {
                        let bcTimeEnd = microtime.nowDouble();
                        if (retval.length === 0) {
                            gbRunning = false;
                            return res.send(stringify({ error: 'No facts exist in blockchain' }));
                        }
                        let facts = helper.removeTimestamps(retval);
                        console.log('CALCULATING NEW GB FROM BEGINING');
                        let sqlTimeStart = microtime.nowDouble();
                        calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                            let sqlTimeEnd = microtime.nowDouble();
                            let totalEnd = microtime.nowDouble();
                            if (error) {
                                gbRunning = false;
                                return res.send(stringify(error))
                            }
                            groupBySqlResult.gbCreateTable = view.SQLTable;
                            groupBySqlResult.field = view.aggregationField;
                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                            groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime;
                            groupBySqlResult.allTotal = totalEnd - totalStart;
                            helper.printTimes(groupBySqlResult);
                            io.emit('view_results', stringify(groupBySqlResult));
                            gbRunning = false;
                            return res.send(groupBySqlResult);
                        });
                    });
                });
            }
        } else {
            res.status(400);
            gbRunning = false;
            return res.send({ status: 'ERROR', options: 'Contract not deployed' });
        }
    }
});

app.get('/getcount', function (req, res) {
    if (contract) {
        contractController.getFactsCount().then(result => {
            if(result === -1){
                res.send({ status: 'ERROR', options: 'Error getting count' });
            } else {
                res.send(result);
            }
        });
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.post('/addFact', function (req, res) {
    if (contract) {
        contractController.addFact(req.body).then(receipt => {
            res.send(receipt);
        }).catch(error => {
            console.log(error);
            res.send(error);
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});
