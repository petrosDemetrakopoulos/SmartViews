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
const computationsController = require('./controllers/computationsController');
const viewMaterializationController = require('./controllers/viewMaterializationController');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.WebsocketProvider(config.blockchainIP));
let createTable = '';
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
        computationsController.connectToSQL().then (() => {
            mysqlConnected = true;
            console.log('mySQL connected');
        }).catch(err => {
            console.error('error connecting to mySQL: ' + err.stack);
        })
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
    if (!running) {
        // a guard to check that this asynchronous process will not start again if called while loading data
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
    contractController.getLatestId().then(async result => {
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
    }).catch(err => {
        helper.log(err);
        res.send(err);
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
    let view = helper.checkViewExists(viewsDefined, req.params.viewName, factTbl);
    // returns an empty object if view not exist, otherwise it returns the view object
    if (Object.keys(view).length === 0 && view.constructor === Object) {
        res.status(200);
        return res.send({ error: 'view not found' });
    }
    let materializationDone = false;
    await helper.updateViewFrequency(factTbl, req.params.contract, view.id);
    if (!gbRunning && !running) {
        gbRunning = true; // a flag to handle retries of the request from the front-end
        let gbFields = helper.extractGBFields(view);
        view.gbFields = gbFields;
        let globalAllGroupBysTime = { getAllGBsTime: 0, getGroupIdTime: 0 };
        if (config.cacheEnabled) {
            helper.log('cache enabled = TRUE');
            await contractController.getAllGroupbys().then(async resultGB => {

                if(resultGB.times.getGroupIdTime !== null && resultGB.times.getGroupIdTime !== undefined) {
                    globalAllGroupBysTime.getGroupIdTime = resultGB.times.getGroupIdTime;
                }

                if(resultGB.times.getAllGBsTime !== null && resultGB.times.getAllGBsTime !== undefined) {
                    globalAllGroupBysTime.getAllGBsTime = resultGB.times.getAllGBsTime;
                }
                delete resultGB.times;

                if (Object.keys(resultGB).length > 1) {
                    let filteredGBs = helper.filterGBs(resultGB, view);
                    if (filteredGBs.length > 0) {
                        console.log("FILTERED GBS > 0");
                        let getLatestFactIdTimeStart = helper.time();
                        await contractController.getLatestId().then(async latestId => {
                            let sortedByEvictionCost = await helper.sortByEvictionCost(resultGB, latestId, view, factTbl);
                            let sortedByCalculationCost = await helper.sortByCalculationCost(filteredGBs, latestId);
                            let mostEfficient = sortedByCalculationCost[0];
                            let getLatestFactIdTime = helper.time() - getLatestFactIdTimeStart;

                            if (mostEfficient.latestFact >= (latestId - 1)) {
                                helper.log('NO NEW FACTS');
                                // NO NEW FACTS after the latest group by
                                // -> incrementally calculate the groupby requested by summing the one in redis cache
                                let allHashes = helper.reconstructSlicedCachedResult(mostEfficient);
                                let cacheRetrieveTimeStart = helper.time();
                                await cacheController.getManyCachedResults(allHashes).then(async allCached => {
                                    let cacheRetrieveTimeEnd = helper.time();

                                    let cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);
                                    if (cachedGroupBy) {
                                        let times = {
                                            cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                            cacheRetrieveTimeStart: cacheRetrieveTimeStart,
                                            totalStart: totalStart,
                                            getGroupIdTime: globalAllGroupBysTime.getGroupIdTime
                                        };
                                        await viewMaterializationController.calculateFromCache(cachedGroupBy, sortedByEvictionCost, view, gbFields, latestId, times).then(result =>  {
                                            gbRunning = false;
                                            materializationDone = true;
                                            io.emit('view_results', stringify(result).replace('\\', ''));
                                            res.status(200);
                                            return res.send(stringify(result).replace('\\', ''));
                                        }).catch(err => {
                                            helper.log(err);
                                            gbRunning = false;
                                            return res.send(err);
                                        });
                                    }
                                }).catch(err => {
                                    helper.log(err);
                                    gbRunning = false;
                                    return res.send(err);
                                });
                            } else {
                                helper.log('DELTAS DETECTED');
                                // we have deltas -> we fetch them
                                // CALCULATING THE VIEW JUST FOR THE DELTAS
                                // THEN MERGE IT WITH THE ONES IN CACHE
                                // THEN SAVE BACK IN CACHE
                                await viewMaterializationController.calculateForDeltasAndMergeWithCached(mostEfficient, latestId,
                                    createTable, view, gbFields,sortedByEvictionCost, globalAllGroupBysTime,
                                    getLatestFactIdTime, totalStart).then(results => {
                                    io.emit('view_results', results);
                                    gbRunning = false;
                                    materializationDone = true;
                                    res.status(200);
                                    return res.send(stringify(results).replace('\\', ''));
                                }).catch(err => {
                                    helper.log(err);
                                    gbRunning = false;
                                    return res.send(err);
                                });
                            }
                        }).catch(err => {
                            helper.log(err);
                            gbRunning = false;
                            return res.send(err);
                        });
                    } else {
                        // No filtered group-bys found, proceed to group-by from the beginning
                        console.log("NO FILTERED GROUP BYS FOUND");
                        await contractController.getLatestId(async latestId => {
                            let sortedByEvictionCost = await helper.sortByEvictionCost(resultGB, latestId, view, factTbl);
                            viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, globalAllGroupBysTime.getGroupIdTime, sortedByEvictionCost).then(result => {
                                gbRunning = false;
                                io.emit('view_results', stringify(result).replace('\\', ''));
                                res.status(200);
                                gbRunning = false;
                                materializationDone = true;
                                return res.send(stringify(result).replace('\\', ''));
                            }).catch(err => {
                                gbRunning = false;
                                return res.send(stringify(err));
                            });
                        }).catch(err => {
                            return res.send(stringify(err));
                        });
                    }
                }
            }).catch(err => {
                helper.log(err);
                gbRunning = false;
                return res.send(err);
            });
        }
        if(!materializationDone) {
            // cache not enabled, so just fetch everything everytime from blockchain and then make calculation in sql
            // just like the case that the cache is originally empty
            viewMaterializationController.calculateNewGroupByFromBeginning(view, totalStart, globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime, []).then(result => {
                gbRunning = false;
                io.emit('view_results', stringify(result).replace('\\', ''));
                res.status(200);
                return res.send(stringify(result).replace('\\', ''));
            }).catch(err => {
                gbRunning = false;
                return res.send(stringify(err))
            });
        }
    }
});

app.get('/getcount', contractController.contractChecker, function (req, res) {
    contractController.getFactsCount().then(result => {
        if (result === -1) {
            return res.send({ status: 'ERROR', options: 'Error getting count' });
        } else {
            res.status(200);
            return res.send(result);
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
