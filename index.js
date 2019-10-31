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
const viewMaterializationController = require('./controllers/viewMaterializationController');
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
                    viewMaterializationController.setContract(contract, account);
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
    contractController.getLatestId(function (err, result) {
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

app.get('/getViewByName/:viewName/:contract', contractController.contractChecker, async function (req, res) {
    config = helper.requireUncached('../config_private');
    let totalStart = helper.time();
    let factTbl = require('./templates/' + req.params.contract);
    let viewsDefined = factTbl.views;
    let view = helper.checkViewExists(viewsDefined, req.params.viewName, factTbl); // returns an empty object if view not exist, otherwise it returns the view object
    if (Object.keys(view).length === 0 && view.constructor === Object) {
        res.status(200);
        return res.send({ error: 'view not found' });
    }
    await helper.updateViewFrequency(factTbl, req.params.contract, view.id);
    helper.log('View by name endpoint hit again');
    if (!gbRunning && !running) {
        gbRunning = true;
        let gbFields = helper.extractGBFields(view);
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
                                let transformedArray = helper.transformGBMetadataFromBlockchain(resultGB);
                                transformedArray = helper.containsAllFields(transformedArray, view); // assigns the containsAllFields value
                                let filteredGBs = [];
                                let sortedByEvictionCost = [];
                                for (let i = 0; i < transformedArray.length; i++) { // filter out the group bys that DO NOT CONTAIN all the fields we need -> aka containsAllFields = false
                                    if (transformedArray[i].containsAllFields) {
                                        filteredGBs.push(transformedArray[i]);
                                    }
                                    sortedByEvictionCost.push(transformedArray[i]);
                                }

                                await contractController.getLatestId(function (err, latestId) {
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
                                    contractController.getLatestId(function (err, latestId) {
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
                                                        let cachedGroupBy = cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);

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

                                                                let times = {cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                                                    cacheRetrieveTimeStart: cacheRetrieveTimeStart,
                                                                    totalStart: totalStart};

                                                                viewMaterializationController.reduceGroupByFromCache(cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId, function (error, results) {
                                                                    gbRunning = false;
                                                                    if (error) {
                                                                        return res.send(error);
                                                                    }
                                                                    io.emit('view_results', stringify(results).replace('\\', ''));
                                                                    res.status(200);
                                                                    return res.send(stringify(results));
                                                                });
                                                            } else {
                                                                // some fields contained in a Group by but operation and aggregation fields differ
                                                                // this means we should proceed to new group by calculation from the begining
                                                                viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, getGroupIdTime, sortedByEvictionCost, function (error, result) {
                                                                    gbRunning = false;
                                                                    if(error){
                                                                        return res.send(error);
                                                                    }
                                                                    io.emit('view_results', stringify(result).replace('\\', ''));
                                                                    res.status(200);
                                                                    return res.send(stringify(result));
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
                                                                io.emit('view_results', stringify(cachedGroupBy).replace('\\', ''));
                                                                gbRunning = false;
                                                                return res.send(stringify(cachedGroupBy));
                                                            } else {
                                                                // same fields but different operation or different aggregate field
                                                                // this means we should proceed to new group by calculation from the begining
                                                                viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, getGroupIdTime, sortedByEvictionCost, function (error, result) {
                                                                    gbRunning = false;
                                                                    if(error){
                                                                        return res.send(error);
                                                                    }
                                                                    io.emit('view_results', stringify(result).replace('\\', ''));
                                                                    res.status(200);
                                                                    return res.send(stringify(result));
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

                                                                    let cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);

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

                                                                                let viewMeta = helper.extractViewMeta(view);
                                                                                // MERGE reducedResult with groupBySQLResult
                                                                                reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
                                                                                reducedResult.field = view.aggregationField;
                                                                                reducedResult.viewName = req.params.viewName;
                                                                                let rows = helper.extractGBValues(reducedResult, view);
                                                                                let rowsDelta = helper.extractGBValues(groupBySqlResult, view);

                                                                                let mergeTimeStart = helper.time();
                                                                                computationsController.mergeGroupBys(rows, rowsDelta, view.SQLTable, viewMeta.viewNameSQL, view, viewMeta.lastCol, viewMeta.prelastCol, function (mergeResult, error) {
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
                                                                                    }).on('receipt', (receipt) => {
                                                                                        let cacheSaveTimeEnd = helper.time();
                                                                                        delete mergeResult.gbCreateTable;
                                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                            contractController.deleteCachedResults(sortedByEvictionCost, function (err, latestGBDeleted) {
                                                                                                let totalEnd = helper.time();
                                                                                                if (!err) {
                                                                                                    mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                                    mergeResult.bcTime = (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + getLatestFactTime + getGroupIdTime + getAllGBsTime;
                                                                                                    mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                    mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                    mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                    mergeResult.allTotal = totalEnd - totalStart;
                                                                                                    helper.printTimes(mergeResult);
                                                                                                    helper.log('receipt:' + JSON.stringify(receipt));
                                                                                                    io.emit('view_results', mergeResult);
                                                                                                    gbRunning = false;
                                                                                                    res.status(200);
                                                                                                    return res.send(stringify(mergeResult));
                                                                                                }
                                                                                                gbRunning = false;
                                                                                                return res.send(err);
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
                                                                                            return res.send(stringify(mergeResult));
                                                                                        }
                                                                                    });
                                                                                });
                                                                            });
                                                                        } else {
                                                                            helper.log('GROUP-BY FIELDS OF DELTAS AND CACHED ARE THE SAME');
                                                                            // group by fields of deltas and cached are the same so
                                                                            // MERGE cached and groupBySqlResults
                                                                            let times = {bcTimeEnd: bcTimeEnd,
                                                                                bcTimeStart: bcTimeStart,
                                                                                getGroupIdTime: getGroupIdTime,
                                                                                getAllGBsTime: getAllGBsTime,
                                                                                getLatestFactIdTime: getLatestFactIdTime,
                                                                                getLatestFactTime: getLatestFactTime,
                                                                                sqlTimeEnd: sqlTimeEnd,
                                                                                sqlTimeStart: sqlTimeStart,
                                                                                cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                                                                cacheRetrieveTimeStart: cacheRetrieveTimeStart};

                                                                            viewMaterializationController.mergeCachedWithDeltasResultsSameFields(view, cachedGroupBy, groupBySqlResult, latestId, sortedByEvictionCost, times, function (err, result) {
                                                                                if(err){
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                }
                                                                                io.emit('view_results', stringify(result).replace('\\', ''));
                                                                                res.status(200);
                                                                                gbRunning = false;
                                                                                return res.send(stringify(result));
                                                                            })
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
                                    viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, getGroupIdTime, sortedByEvictionCost, function (error, result) {
                                        gbRunning = false;
                                        if (error) {
                                            gbRunning = false;
                                            return res.send(stringify(error))
                                        }
                                        io.emit('view_results', stringify(result).replace('\\', ''));
                                        res.status(200);
                                        gbRunning = false;
                                        return res.send(stringify(result));
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
                        viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, getGroupIdTime, [], function (error, result) {
                            gbRunning = false;
                            if (error) {
                                return res.send(stringify(error))
                            }
                            io.emit('view_results', stringify(result).replace('\\', ''));
                            res.status(200);
                            return res.send(stringify(result).replace('\\', ''));
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
            viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, 0, [], function (error, result) {
                gbRunning = false;
                if (error) {
                    return res.send(stringify(error))
                }
                io.emit('view_results', stringify(result).replace('\\', ''));
                res.status(200);
                return res.send(stringify(result).replace('\\', ''));
            });
        }
    }
});

app.get('/getcount', contractController.contractChecker, function (req, res) {
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