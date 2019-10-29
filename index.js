const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const stringify = require('fast-stringify');
let config = require('./config_private');
const configLab = require('./config_lab');
const path = require('path');
const app = express();
const jsonParser = bodyParser.json();
const helper = require('./helpers/helper');
app.use(jsonParser);
let running = false;
let gbRunning = false;
let mysqlConnected = false;
let blockchainReady = false;
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static('public'));
let http = require('http').Server(app);
let io = require('socket.io')(http);
const contractGenerator = require('./helpers/contractGenerator');
const transformations = require('./helpers/transformations');
const contractDeployer = require('./helpers/contractDeployer');
const contractController = require('./controllers/contractController');
const cacheController = require('./controllers/cacheController');
const costFunctions = require('./helpers/costFunctions');
const computationsController = require('./controllers/computationsController');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider(config.blockchainIP));
let createTable = '';
let tableName = '';
let contractsDeployed = [];

web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let account = null;
app.get('/', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            console.error('error reading templates directory: ' + err.stack);
            return;
        }
        web3.eth.getBlockNumber().then(blockNum => {
            if (blockNum >= 0) {
                blockchainReady = true;
            }
            return res.render('index', { 'templates': items, 'redisStatus': cacheController.getRedisStatus(), 'sqlStatus': mysqlConnected, 'blockchainStatus': blockchainReady });
        });
    });
});

app.get('/dashboard', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            console.error('error reading templates directory: ' + err.stack);
            return;
        }
        let jsonFiles = helper.getJSONFiles(items);
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
    console.log(`Visit http://localhost:3000/ to view Blockchain, mySQL and Redis cache status`);
 //   config = new SelfReloadJSON('./config_private.json');
    let validations = helper.configFileValidations();
    if (process.env.ENVIRONMENT === 'LAB') {
        config = configLab;
    }
    if (validations.passed) {
        computationsController.connectToSQL(function (err) {
            if (err) {
                console.error('error connecting to mySQL: ' + err.stack);
                return;
            }
            mysqlConnected = true;
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
            account = accounts[1];
            contractDeployer.deployContract(accounts[0], './contracts/' + req.params.fn, contract)
                .then(options => {
                    helper.log('******************');
                    helper.log('Contract Deployed!');
                    helper.log('******************');
                    contractsDeployed.push(options.contractDeployed);
                    contract = options.contractObject;
                    contractController.setContract(contract, account);
                    cacheController.setContract(contract, account);
                    res.send({ status: 'OK', options: options.options });
                })
                .catch(err => {
                    helper.log('error on deploy ' + err);
                    res.status(400);
                    res.send({ status: 'ERROR', options: 'Deployment failed' });
                })
        }
    });
});

app.get('/load_dataset/:dt', contractController.contractChecker, function (req, res) {
    let dt = require('./test_data/' + req.params.dt);
    if (!running) { // a guard to check that this asynchronous process will not start again if called while loading data
        running = true;
        let startTime = helper.time();
        contractController.addManyFacts(dt, config.recordsSlice, io).then(retval => {
            let endTime = helper.time();
            let timeDiff = endTime - startTime;
            running = false;
            io.emit('DONE', 'TRUE');
            helper.log('Added ' + dt.length + ' records in ' + timeDiff + ' seconds');
            res.send({ msg: 'OK' });
        }).catch(error => {
            helper.log(error);
            res.send(error);
        });
    }
});

app.get('/new_contract/:fn', function (req, res) {
    contractGenerator.generateContract(req.params.fn).then(result => {
        computationsController.setCreateTable(result.createTable);
        computationsController.setTableName(result.tableName);
        createTable = result.createTable;
        tableName = result.tableName;
        return res.send({ msg: 'OK', 'filename': result.filename + '.sol', 'template': result.template });
    }).catch(err => {
        helper.log(err);
        return res.send({ msg: 'error' });
    });
});

app.get('/getFactById/:id', contractController.contractChecker, function (req, res) {
    contractController.getFactById(req.params.id).then(result => {
        res.send(result);
    }).catch(error => {
        helper.log(error);
        res.send(error);
    });
});

app.get('/getFactsFromTo/:from/:to', function (req, res) {
    let timeStart = helper.time();
    contractController.getFactsFromTo(parseInt(req.params.from), parseInt(req.params.to)).then(retval => {
        let timeFinish = helper.time() - timeStart;
        retval.push({ time: timeFinish });
        res.send(retval);
    }).catch(err => {
        res.send(err);
    });
});

app.get('/allfacts', contractController.contractChecker, function (req, res) {
    contract.methods.dataId().call(function (err, result) {
        if (!err) {
            // async loop waiting to get all the facts separately
            let timeStart = helper.time();
            contractController.getAllFactsHeavy(result).then(retval => {
                let timeFinish = helper.time() - timeStart;
                helper.log('Get all facts time: ' + timeFinish + ' s');
                retval.push({ time: timeFinish });
                res.send(retval);
            }).catch(error => {
                helper.log(error);
            });
        } else {
            helper.log(err);
            res.send(err);
        }
    });
});

app.get('/groupbyId/:id', contractController.contractChecker, function (req, res) {
    contractController.getGroupByWithId(req.params.id).then(result => {
        return res.send(stringify(result).replace('\\', ''));
    }).catch(error => {
        helper.log(error);
        return res.send(error);
    });
});

app.get('/getViewByName/:viewName/:contract',contractController.contractChecker, function (req, res) {
    config = helper.requireUncached('../config_private');
    let totalStart = helper.time();
    let factTbl = require('./templates/' + req.params.contract);
    let viewsDefined = factTbl.views;
    let found = false;
    let view = {};
    for (let crnView in viewsDefined) {
        if (factTbl.views[crnView].name === req.params.viewName) {
            found = true;
            view = factTbl.views[crnView];
            factTbl.views[crnView].frequency = factTbl.views[crnView].frequency + 1;
            fs.writeFile('./templates/' + req.params.contract + '.json', JSON.stringify(factTbl, null, 2), function (err) {
                if (err) return helper.log(err);
                helper.log('updated view frequency');
            });
            break;
        }
    }

    if (!found) {
        res.status(200);
        return res.send({ error: 'view not found' });
    }

    helper.log('View by name endpoint hit again');
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

        if (config.cacheEnabled) {
            helper.log('cache enabled = TRUE');
            let getGroupIdTimeStart = helper.time();
            contract.methods.groupId().call(function (err, result) {
                let getGroupIdTime = helper.time() - getGroupIdTimeStart;
                if (!err) {
                    if (result > 0) { // At least one group by already exists
                        let getAllGBsFromBCTimeStart = helper.time();
                        contract.methods.getAllGroupBys(result).call(async function (err, resultGB) {
                            let getAllGBsTime = helper.time() - getAllGBsFromBCTimeStart;
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
                                    helper.log('_________________________________');
                                    sortedByEvictionCost = costFunctions.cacheEvictionCostOfficial(sortedByEvictionCost, latestId, req.params.viewName, factTbl);
                                    helper.log(sortedByEvictionCost);
                                    helper.log('cache eviction costs assigned:');
                                    helper.log(sortedByEvictionCost);
                                    filteredGBs = costFunctions.calculationCostOfficial(filteredGBs, latestId); // the cost to materialize the view from each view cached
                                });

                                await sortedByEvictionCost.sort(function (a, b) {
                                    if (config.cacheEvictionPolicy === 'FIFO') {
                                        return parseInt(a.gbTimestamp) - parseInt(b.gbTimestamp);
                                    } else if (config.cacheEvictionPolicy === 'COST FUNCTION') {
                                        helper.log('SORT WITH COST FUNCTION');
                                        return parseFloat(a.cacheEvictionCost) - parseFloat(b.cacheEvictionCost);
                                    }
                                });

                                helper.log('SORTED Group Bys by eviction cost:');
                                helper.log(sortedByEvictionCost); // TS ORDER ascending, the first ones are less 'expensive' than the last ones.
                                helper.log('________________________');
                                // assign costs
                                // filteredGBs = calculationCostOfficial(filteredGBs, latestId);
                                if (filteredGBs.length > 0) {
                                    // pick the one with the less cost
                                    filteredGBs.sort(function (a, b) {
                                        return parseFloat(a.calculationCost) - parseFloat(b.calculationCost)
                                    }); // order ascending
                                    let mostEfficient = filteredGBs[0]; // TODO: check what we do in case we have no groub bys that match those criteria
                                    let getLatestFactIdTimeStart = helper.time();
                                    contract.methods.dataId().call(function (err, latestId) {
                                        helper.log('LATEST ID IS:');
                                        helper.log(latestId);
                                        let getLatestFactIdTime = helper.time() - getLatestFactIdTimeStart;
                                        if (err) {
                                            helper.log(err);
                                            gbRunning = false;
                                            return res.send(err);
                                        }
                                        if (mostEfficient.gbTimestamp > 0) {
                                            let getLatestFactTimeStart = helper.time();
                                            contract.methods.getFact(latestId - 1).call(function (err, latestFact) {
                                                let getLatestFactTime = helper.time() - getLatestFactTimeStart;
                                                if (err) {
                                                    helper.log(err);
                                                    gbRunning = false;
                                                    return res.send(err);
                                                }
                                                if (mostEfficient.latestFact >= (latestId - 1)) {
                                                    helper.log('NO NEW FACTS');
                                                    // NO NEW FACTS after the latest group by
                                                    // -> incrementally calculate the groupby requested by summing the one in redis cache
                                                    let hashId = mostEfficient.hash.split('_')[1];
                                                    let hashBody = mostEfficient.hash.split('_')[0];
                                                    let allHashes = [];
                                                    for (let i = 0; i <= hashId; i++) {
                                                        allHashes.push(hashBody + '_' + i);
                                                    }
                                                    let cacheRetrieveTimeStart = helper.time();
                                                    cacheController.getManyCachedResults(allHashes, function (error, allCached) {
                                                        let cacheRetrieveTimeEnd = helper.time();
                                                        if (error) {
                                                            helper.log(error);
                                                            gbRunning = false;
                                                            return res.send(error);
                                                        }
                                                        let cachedGroupBy = {};
                                                        if (allCached.length === 1) { // it is <= of slice size, so it is not sliced
                                                            cachedGroupBy = JSON.parse(allCached[0]);
                                                        } else { // it is sliced
                                                            cachedGroupBy = helper.mergeSlicedCachedResult(allCached);
                                                        }

                                                        if (err) {
                                                            helper.log(error);
                                                            gbRunning = false;
                                                            return res.send(error);
                                                        }
                                                        if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                                            // this means we want to calculate a different group by than the stored one
                                                            // but however it can be calculated just from redis cache
                                                            if (cachedGroupBy.field === view.aggregationField &&
                                                                view.operation === cachedGroupBy.operation) {

                                                                let reductionTimeStart = helper.time();
                                                                computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields, async function (reducedResult, error) {
                                                                    let reductionTimeEnd = helper.time();
                                                                    if (error) {
                                                                        gbRunning = false;
                                                                        return res.send(error);
                                                                    }

                                                                    let lastCol = view.SQLTable.split(' ');
                                                                    let prelastCol = lastCol[lastCol.length - 4];  // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                    lastCol = lastCol[lastCol.length - 2];

                                                                    let op = '';
                                                                    if (view.operation === 'SUM' || view.operation === 'COUNT') {
                                                                        op = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
                                                                    } else if (view.operation === 'MIN') {
                                                                        op = 'MIN'
                                                                    } else if (view.operation === 'MAX') {
                                                                        op = 'MAX';
                                                                    }

                                                                    if (view.operation === 'AVERAGE') {
                                                                        reducedResult = transformations.transformReadyAverage(reducedResult, view.gbFields, view.aggregationField);
                                                                    } else {
                                                                        reducedResult = transformations.transformGBFromSQL(reducedResult, op, lastCol, gbFields);
                                                                    }
                                                                    reducedResult.field = view.aggregationField;
                                                                    reducedResult.viewName = req.params.viewName;
                                                                    reducedResult.operation = view.operation;

                                                                    let cacheSaveTimeStart = helper.time();
                                                                    return cacheController.saveOnCache(reducedResult, view.operation, latestId - 1).on('error', (err) => {
                                                                        helper.log('error:', err);
                                                                        gbRunning = false;
                                                                        return res.send(err);
                                                                    }).on('transactionHash', (txHash) => {
                                                                        helper.log('transactionHash:', txHash);
                                                                    }).on('receipt', (receipt) => {
                                                                        helper.log('receipt:' + JSON.stringify(receipt));
                                                                        let cacheSaveTimeEnd = helper.time();
                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                            cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                    let totalEnd = helper.time();
                                                                                    if (!err) {
                                                                                        reducedResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                        reducedResult.sqlTime = reductionTimeEnd - reductionTimeStart;
                                                                                        reducedResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                        reducedResult.totalTime = reducedResult.cacheSaveTime + reducedResult.sqlTime + reducedResult.cacheRetrieveTime;
                                                                                        reducedResult.allTotal = totalEnd - totalStart;
                                                                                        helper.printTimes(reducedResult);
                                                                                        io.emit('view_results', stringify(reducedResult).replace('\\', ''));
                                                                                        gbRunning = false;
                                                                                        return res.send(stringify(reducedResult).replace('\\', ''));
                                                                                    }
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                });
                                                                            });
                                                                        } else {
                                                                            let totalEnd = helper.time();
                                                                            reducedResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                            reducedResult.sqlTime = reductionTimeEnd - reductionTimeStart;
                                                                            reducedResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                            reducedResult.totalTime = reducedResult.cacheSaveTime + reducedResult.sqlTime + reducedResult.cacheRetrieveTime;
                                                                            reducedResult.allTotal = totalEnd - totalStart;
                                                                            helper.printTimes(reducedResult);
                                                                            io.emit('view_results', stringify(reducedResult).replace('\\', ''));
                                                                            gbRunning = false;
                                                                            res.status(200);
                                                                            return res.send(stringify(reducedResult).replace('\\', ''));
                                                                        }
                                                                    });
                                                                });
                                                            } else {
                                                                // some fields contained in a Group by but operation and aggregation fields differ
                                                                // this means we should proceed to new group by calculation from the begining
                                                                let bcTimeStart = helper.time();
                                                                contractController.getAllFactsHeavy(latestId).then(retval => {
                                                                    let bcTimeEnd = helper.time();
                                                                    for (let i = 0; i < retval.length; i++) {
                                                                        delete retval[i].timestamp;
                                                                    }
                                                                    helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                                                                    let sqlTimeStart = helper.time();
                                                                    computationsController.calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                        let sqlTimeEnd = helper.time();
                                                                        if (error) {
                                                                            gbRunning = false;
                                                                            return res.send(error);
                                                                        }
                                                                        groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                        groupBySqlResult.field = view.aggregationField;
                                                                        groupBySqlResult.viewName = req.params.viewName;
                                                                        let cacheSaveTimeStart = helper.time();
                                                                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                            helper.log('error:', err);
                                                                            gbRunning = false;
                                                                            return res.send(err);
                                                                        }).on('transactionHash', (txHash) => {
                                                                            helper.log('transactionHash:' + txHash);
                                                                        }).on('receipt', (receipt) => {
                                                                            let cacheSaveTimeEnd = helper.time();
                                                                            delete groupBySqlResult.gbCreateTable;
                                                                            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                    contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                        let totalEnd = helper.time();
                                                                                        if (!err) {
                                                                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                            groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                            helper.printTimes(groupBySqlResult);
                                                                                            helper.log('receipt:'+ JSON.stringify(receipt));
                                                                                            io.emit('view_results', stringify(groupBySqlResult));
                                                                                            gbRunning = false;
                                                                                            res.status(200);
                                                                                            return res.send(stringify(groupBySqlResult));
                                                                                        }
                                                                                        gbRunning = false;
                                                                                        return res.send(err);
                                                                                    });
                                                                                });
                                                                            } else {
                                                                                let totalEnd = helper.time();
                                                                                groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                                groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                helper.printTimes(groupBySqlResult);
                                                                                helper.log('receipt:'+ JSON.stringify(receipt));
                                                                                io.emit('view_results', stringify(groupBySqlResult));
                                                                                gbRunning = false;
                                                                                res.status(200);
                                                                                return res.send(stringify(groupBySqlResult));
                                                                            }
                                                                        });
                                                                    });
                                                                });
                                                            }
                                                        } else {
                                                            if (cachedGroupBy.field === view.aggregationField &&
                                                                view.operation === cachedGroupBy.operation) {
                                                                let totalEnd = helper.time();
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
                                                                let bcTimeStart = helper.time();
                                                                contractController.getAllFactsHeavy(latestId).then(retval => {
                                                                    let bcTimeEnd = helper.time();
                                                                    for (let i = 0; i < retval.length; i++) {
                                                                        delete retval[i].timestamp;
                                                                    }
                                                                    helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                                                                    let sqlTimeStart = helper.time();
                                                                    computationsController.calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                        let sqlTimeEnd = helper.time();
                                                                        if (error) {
                                                                            gbRunning = false;
                                                                            return res.send(error);
                                                                        }
                                                                        groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                        groupBySqlResult.field = view.aggregationField;
                                                                        groupBySqlResult.viewName = req.params.viewName;
                                                                        let cacheSaveTimeStart = helper.time();
                                                                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                            helper.log('error:' + err);
                                                                            gbRunning = false;
                                                                            return res.send(err);
                                                                        }).on('transactionHash', (txHash) => {
                                                                            helper.log('transactionHash:' + txHash);
                                                                        }).on('receipt', (receipt) => {
                                                                            let cachSaveTimeEnd = helper.time();
                                                                            delete groupBySqlResult.gbCreateTable;
                                                                            if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                    contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                        let totalEnd = helper.time();
                                                                                        if (!err) {
                                                                                            groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                            groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                                            groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                            helper.printTimes(groupBySqlResult);
                                                                                            helper.log('receipt:'+ JSON.stringify(receipt));
                                                                                            io.emit('view_results', stringify(groupBySqlResult));
                                                                                            gbRunning = false;
                                                                                            res.status(200);
                                                                                            return res.send(stringify(groupBySqlResult));
                                                                                        }
                                                                                        gbRunning = false;
                                                                                        return res.send(err);
                                                                                    });
                                                                                });
                                                                            } else {
                                                                                let totalEnd = helper.time();
                                                                                groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                                groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                                groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                helper.printTimes(groupBySqlResult);
                                                                                helper.log('receipt:'+ JSON.stringify(receipt));
                                                                                io.emit('view_results', stringify(groupBySqlResult));
                                                                                gbRunning = false;
                                                                                res.status(200);
                                                                                return res.send(stringify(groupBySqlResult));
                                                                            }
                                                                        });
                                                                    });
                                                                });
                                                            }
                                                        }
                                                    });
                                                } else {
                                                    helper.log('DELTAS DETECTED');
                                                    // we have deltas -> we fetch them
                                                    // CALCULATING THE VIEW JUST FOR THE DELTAS
                                                    // THEN MERGE IT WITH THE ONES IN CACHE
                                                    // THEN SAVE BACK IN CACHE
                                                    let bcTimeStart = helper.time();
                                                    contractController.getFactsFromTo(mostEfficient.latestFact, latestId - 1).then(deltas => {
                                                        let bcTimeEnd = helper.time();
                                                        computationsController.executeQuery(createTable, function (error, results, fields) {
                                                            if (error) throw error;
                                                            deltas = helper.removeTimestamps(deltas);
                                                            helper.log('CALCULATING GROUP-BY FOR DELTAS:');
                                                            let sqlTimeStart = helper.time();
                                                            computationsController.calculateNewGroupBy(deltas, view.operation, view.gbFields, view.aggregationField, async function (groupBySqlResult, error) {
                                                                let sqlTimeEnd = helper.time();
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

                                                                let cacheRetrieveTimeStart = helper.time();
                                                                cacheController.getManyCachedResults(allHashes, async function (error, allCached) {
                                                                    let cacheRetrieveTimeEnd = helper.time();
                                                                    if (error) {
                                                                        helper.log(error);
                                                                        gbRunning = false;
                                                                        return res.send(error);
                                                                    }

                                                                    let cachedGroupBy = {};
                                                                    if (allCached.length === 1) { // it is <= of slice size, so it is not sliced
                                                                        helper.log('IT IS NOT SLICED');
                                                                        cachedGroupBy = JSON.parse(allCached[0]);
                                                                    } else { // it is sliced
                                                                        helper.log('IT IS SLICED');
                                                                        cachedGroupBy = helper.mergeSlicedCachedResult(allCached);
                                                                    }

                                                                    if (cachedGroupBy.field === view.aggregationField &&
                                                                        view.operation === cachedGroupBy.operation) {
                                                                        if (cachedGroupBy.groupByFields.length !== view.gbFields.length) {
                                                                            let reductionTimeStart = helper.time();
                                                                            computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields, async function (reducedResult, error) {
                                                                                let reductionTimeEnd = helper.time();
                                                                                if (error) {
                                                                                    gbRunning = false;
                                                                                    return res.send(error);
                                                                                }

                                                                                let viewNameSQL = view.SQLTable.split(' ');
                                                                                viewNameSQL = viewNameSQL[3];
                                                                                viewNameSQL = viewNameSQL.split('(')[0];

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
                                                                                let rows = helper.extractGBValues(reducedResult, view);
                                                                                let rowsDelta = helper.extractGBValues(groupBySqlResult, view);

                                                                                lastCol = view.SQLTable.split(' ');
                                                                                prelastCol = lastCol[lastCol.length - 4];
                                                                                lastCol = lastCol[lastCol.length - 2];

                                                                                let mergeTimeStart = helper.time();
                                                                                computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewNameSQL, view, lastCol, prelastCol, function (mergeResult, error) {
                                                                                    let mergeTimeEnd = helper.time();
                                                                                    if (error) {
                                                                                        gbRunning = false;
                                                                                        return res.send(error);
                                                                                    }
                                                                                    mergeResult.operation = view.operation;
                                                                                    mergeResult.field = view.aggregationField;
                                                                                    mergeResult.gbCreateTable = view.SQLTable;
                                                                                    mergeResult.viewName = req.params.viewName;
                                                                                    // save on cache before return
                                                                                    let cacheSaveTimeStart = helper.time();
                                                                                    cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                        helper.log('error:' + err);
                                                                                        gbRunning = false;
                                                                                        return res.send(err);
                                                                                    }).on('transactionHash', (txHash) => {
                                                                                        helper.log('transactionHash:' + txHash);
                                                                                    }).on('receipt', (receipt) => {
                                                                                        let cacheSaveTimeEnd = helper.time();
                                                                                        delete mergeResult.gbCreateTable;
                                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                            cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                    let totalEnd = helper.time();
                                                                                                    if (!err) {
                                                                                                        mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                                        mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                                        mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                        mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                        mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                        mergeResult.allTotal = totalEnd - totalStart;
                                                                                                        helper.printTimes(mergeResult);
                                                                                                        helper.log('receipt:'+ JSON.stringify(receipt));
                                                                                                        io.emit('view_results', mergeResult);
                                                                                                        gbRunning = false;
                                                                                                        res.status(200);
                                                                                                        return res.send(mergeResult);
                                                                                                    }
                                                                                                    gbRunning = false;
                                                                                                    return res.send(err);
                                                                                                });
                                                                                            });
                                                                                        } else {
                                                                                            let totalEnd = helper.time();
                                                                                            mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                            mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getLatestFactIdTime + getLatestFactTime + getAllGBsTime;
                                                                                            mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                            mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                            mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                            mergeResult.allTotal = totalEnd - totalStart;
                                                                                            helper.printTimes(mergeResult);
                                                                                            helper.log('receipt:' + JSON.stringify(receipt));
                                                                                            io.emit('view_results', mergeResult);
                                                                                            gbRunning = false;
                                                                                            res.status(200);
                                                                                            return res.send(mergeResult);
                                                                                        }
                                                                                    });
                                                                                });
                                                                            });
                                                                        } else {
                                                                            helper.log('GROUP-BY FIELDS OF DELTAS AND CACHED ARE THE SAME');
                                                                            // group by fields of deltas and cached are the same so
                                                                            // MERGE cached and groupBySqlResults
                                                                            let viewNameSQL = view.SQLTable.split(' ');
                                                                            viewNameSQL = viewNameSQL[3];
                                                                            viewNameSQL = viewNameSQL.split('(')[0];

                                                                            let lastCol = view.SQLTable.split(' ');
                                                                            let prelastCol = lastCol[lastCol.length - 4]; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                            lastCol = lastCol[lastCol.length - 2];

                                                                            let rows = helper.extractGBValues(cachedGroupBy, view);
                                                                            let rowsDelta = helper.extractGBValues(groupBySqlResult, view);

                                                                            let mergeTimeStart = helper.time();
                                                                            computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewNameSQL, view, lastCol, prelastCol, function (mergeResult, error) {
                                                                                let mergeTimeEnd = helper.time();
                                                                                // SAVE ON CACHE BEFORE RETURN
                                                                                helper.log('SAVE ON CACHE BEFORE RETURN');
                                                                                if (error) {
                                                                                    gbRunning = false;
                                                                                    return res.send(error);
                                                                                }
                                                                                mergeResult.operation = view.operation;
                                                                                mergeResult.field = view.aggregationField;
                                                                                mergeResult.gbCreateTable = view.SQLTable;
                                                                                mergeResult.viewName = req.params.viewName;
                                                                                let cacheSaveTimeStart = helper.time();
                                                                                cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                    helper.log('error:' + err);
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                }).on('transactionHash', (txHash) => {
                                                                                    helper.log('transactionHash:' + txHash);
                                                                                }).on('receipt', (receipt) => {
                                                                                    let cacheSaveTimeEnd = helper.time();
                                                                                    delete mergeResult.gbCreateTable;
                                                                                    if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                        cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                            contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                let totalEnd = helper.time();
                                                                                                if (!err) {
                                                                                                    mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getAllGBsTime + getLatestFactIdTime + getLatestFactTime;
                                                                                                    mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                                    mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                    mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                    mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                    mergeResult.allTotal = totalEnd - totalStart;
                                                                                                    helper.printTimes(mergeResult);
                                                                                                    helper.log('receipt:' + JSON.stringify(receipt));
                                                                                                    io.emit('view_results', mergeResult);
                                                                                                    gbRunning = false;
                                                                                                    res.status(200);
                                                                                                    return res.send(mergeResult);
                                                                                                }
                                                                                                gbRunning = false;
                                                                                                return res.send(err);
                                                                                            });
                                                                                        });
                                                                                    } else {
                                                                                        let totalEnd = helper.time();
                                                                                        mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime + getAllGBsTime + getLatestFactIdTime + getLatestFactTime;
                                                                                        mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                        mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                        mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                        mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                        mergeResult.allTotal = totalEnd - totalStart;
                                                                                        helper.printTimes(mergeResult);
                                                                                        helper.log('receipt:' + JSON.stringify(receipt));
                                                                                        io.emit('view_results', mergeResult);
                                                                                        gbRunning = false;
                                                                                        res.status(200);
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
                                        }
                                    });
                                } else {
                                    // No filtered group-bys found, proceed to group-by from the beginning
                                    let getLatestFactIdTimeStart = helper.time();
                                    contract.methods.dataId().call(function (err, latestId) {
                                        helper.log('LATEST ID IS:');
                                        helper.log(latestId);
                                        let getLatestFactIdTimeEnd = helper.time();
                                        let getLatestFactIdTime = getLatestFactIdTimeEnd - getLatestFactIdTimeStart;
                                        if (err) {
                                            helper.log(err);
                                            gbRunning = false;
                                            return res.send(err);
                                        }
                                        let bcTimeStart = helper.time();
                                        contractController.getAllFactsHeavy(latestId).then(retval => {
                                            let bcTimeEnd = helper.time();
                                            for (let i = 0; i < retval.length; i++) {
                                                delete retval[i].timestamp;
                                            }
                                            helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                                            let sqlTimeStart = helper.time();
                                            computationsController.calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                let sqlTimeEnd = helper.time();
                                                if (error) {
                                                    gbRunning = false;
                                                    return res.send(error);
                                                }
                                                groupBySqlResult.gbCreateTable = view.SQLTable;
                                                groupBySqlResult.field = view.aggregationField;
                                                groupBySqlResult.viewName = req.params.viewName;
                                                let cacheSaveTimeStart = helper.time();
                                                cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                    helper.log('error:' + err);
                                                    gbRunning = false;
                                                    return res.send(err);
                                                }).on('transactionHash', (txHash) => {
                                                    helper.log('transactionHash:' + txHash);
                                                }).on('receipt', (receipt) => {
                                                    let cacheSaveTimeEnd = helper.time();
                                                    delete groupBySqlResult.gbCreateTable;
                                                    if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                        cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                            contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                let totalEnd = helper.time();
                                                                if (!err) {
                                                                    groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                    groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getGroupIdTime + getAllGBsTime;
                                                                    groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                    groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                    groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                    helper.printTimes(groupBySqlResult);
                                                                    helper.log('receipt:' + JSON.stringify(receipt));
                                                                    io.emit('view_results', stringify(groupBySqlResult));
                                                                    gbRunning = false;
                                                                    res.status(200);
                                                                    return res.send(stringify(groupBySqlResult));
                                                                }
                                                                gbRunning = false;
                                                                return res.send(err);
                                                            });
                                                        });
                                                    } else {
                                                        let totalEnd = helper.time();
                                                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                        groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getGroupIdTime + getAllGBsTime;
                                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                                        helper.printTimes(groupBySqlResult);
                                                        helper.log('receipt:' + JSON.stringify(receipt));
                                                        io.emit('view_results', stringify(groupBySqlResult));
                                                        gbRunning = false;
                                                        res.status(200);
                                                        return res.send(stringify(groupBySqlResult));
                                                    }
                                                });
                                            });
                                        });
                                    });
                                }
                            } else {
                                helper.log(err);
                                gbRunning = false;
                                return res.send(err);
                            }
                        });
                    } else {
                        // No group bys exist in cache, we are in the initial state
                        // this means we should proceed to new group by calculation from the begining
                        let bcTimeStart = helper.time();
                        contract.methods.dataId().call(function (err, latestId) {
                            if (err) throw err;
                            contractController.getAllFactsHeavy(latestId).then(retval => {
                                let bcTimeEnd = helper.time();
                                if (retval.length === 0) {
                                    gbRunning = false;
                                    return res.send(stringify({ error: 'No facts exist in blockchain' }));
                                }
                                let facts = helper.removeTimestamps(retval);
                                helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                                let sqlTimeStart = helper.time();
                                computationsController.calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                    let sqlTimeEnd = helper.time();
                                    if (error) {
                                        gbRunning = false;
                                        return res.send(stringify(error))
                                    }
                                    groupBySqlResult.gbCreateTable = view.SQLTable;
                                    groupBySqlResult.field = view.aggregationField;
                                    groupBySqlResult.viewName = req.params.viewName;
                                    let cacheSaveTimeStart = helper.time();
                                    cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                        helper.log('error:', err);
                                        gbRunning = false;
                                        return res.send(err);
                                    }).on('transactionHash', (txHash) => {
                                        helper.log('transactionHash:' + txHash);
                                    }).on('receipt', (receipt) => {
                                        let cacheSaveTimeEnd = helper.time();
                                        delete groupBySqlResult.gbCreateTable;
                                        let totalEnd = helper.time();
                                        helper.log('receipt:' + JSON.stringify(receipt));
                                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                        groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                        helper.printTimes(groupBySqlResult);
                                        io.emit('view_results', stringify(groupBySqlResult));
                                        gbRunning = false;
                                        res.status(200);
                                        return res.send(stringify(groupBySqlResult));
                                    });
                                });
                            });
                        });
                    }
                } else {
                    helper.log(err);
                    gbRunning = false;
                    return res.send(err);
                }
            });
        } else {
            helper.log('cache enabled = FALSE');
            // cache not enabled, so just fetch everything everytime from blockchain and then make calculation in sql
            // just like the case that the cache is originally empty
            let bcTimeStart = helper.time();
           return contract.methods.dataId().call(function (err, latestId) {
                if (err) throw err;
               return contractController.getAllFacts(latestId).then(retval => {
                    let bcTimeEnd = helper.time();
                    if (retval.length === 0) {
                        gbRunning = false;
                        return res.send(stringify({ error: 'No facts exist in blockchain' }));
                    }
                    let facts = helper.removeTimestamps(retval);
                    helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                    let sqlTimeStart = helper.time();
                   return computationsController.calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                        let sqlTimeEnd = helper.time();
                        let totalEnd = helper.time();
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
                        res.status(200);
                        return res.send(stringify(groupBySqlResult));
                    });
                });
            });
        }
    }
});

app.get('/getcount',contractController.contractChecker, function (req, res) {
    contractController.getFactsCount().then(result => {
        if (result === -1) {
            res.send({ status: 'ERROR', options: 'Error getting count' });
        } else {
            res.send(result);
        }
    });
});

app.post('/addFact', contractController.contractChecker, function (req, res) {
    contractController.addFact(req.body).then(receipt => {
        res.send(receipt);
    }).catch(error => {
        helper.log(error);
        res.send(error);
    })
});

module.exports = app;