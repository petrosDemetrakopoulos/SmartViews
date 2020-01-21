require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const reload = require('require-reload')(require);
const stringify = require('fast-stringify');
let config = reload('./config_private');
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
const http = require('http').Server(app);
http.timeout = 0;
const io = require('socket.io')(http);
const contractGenerator = require('./helpers/contractGenerator');
const contractDeployer = require('./helpers/contractDeployer');
const contractController = require('./controllers/contractController');
const cacheController = require('./controllers/cacheController');
const computationsController = require('./controllers/computationsController');
const viewMaterializationController = require('./controllers/viewMaterializationController');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.WebsocketProvider(config.blockchainIP));
let createTable = '';
const contractsDeployed = new Map();

web3.eth.defaultAccount = web3.eth.accounts[0];

let contract = null;
let account = null;
app.get('/', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            /* istanbul ignore next */
            console.error('error reading templates directory: ' + err.stack);
            return;
        }
        web3.eth.getBlockNumber().then(blockNum => {
            if (blockNum >= 0) {
                blockchainReady = true;
            }
            return res.render('index', { 'templates': items,
                'redisStatus': cacheController.getRedisStatus(),
                'sqlStatus': mysqlConnected,
                'blockchainStatus': blockchainReady });
        });
    });
});

app.get('/dashboard', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        if (err) {
            /* istanbul ignore next */
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
    address = contractsDeployed.get(factTbl.name).address;
    let readyViews = factTbl.views;
    readyViews = readyViews.map(x => x.name);
    res.render('form', { 'template': templ, 'name': factTbl.name, 'address': address, 'readyViews': readyViews });
});
http.timeout = 0;
http.listen(3000, () => {
    console.log(`Smart-Views listening on http://localhost:3000/dashboard`);
    console.log(`Visit http://localhost:3000/ to view Blockchain, mySQL and Redis cache status`);
    console.log("timeout = " + http.timeout);
    let validations = helper.configFileValidations();
    if (process.env.ENVIRONMENT === 'LAB') {
        config = configLab;
    }
    if (validations.passed) {
        computationsController.connectToSQL().then(connected => {
            mysqlConnected = true;
            console.log('mySQL connected');
            helper.welcomeMessage();
        }).catch(err => {
            /* istanbul ignore next */
            console.error('error connecting to mySQL: ' + err.stack);
        });
    } else {
        /* istanbul ignore next */
        console.log('Config file validations failed');
        /* istanbul ignore next */
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
                    contractsDeployed.set(options.contractDeployed.contractName, options.contractDeployed);
                    contract = options.contractObject;
                    contractController.setContract(contract, account);
                    viewMaterializationController.setContract(contract, account);
                    res.send({ status: 'OK', options: options.options });
                })
                .catch(err => {
                    /* istanbul ignore next */
                    helper.log('error on deploy ' + err);
                    /* istanbul ignore next */
                    res.status(400);
                    /* istanbul ignore next */
                    res.send({ status: 'ERROR', message: 'Deployment failed' });
                })
        }
    });
});

app.get('/load_dataset/:dt', contractController.contractChecker, function (req, res) {
    let dt = require('./test_data/benchmarks/' + req.params.dt);
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
            res.send({ message: 'OK' });
        }).catch(error => {
            /* istanbul ignore next */
            helper.log(error);
            /* istanbul ignore next */
            res.send(helper.errorToJson(error));
        });
    }
});

app.get('/new_contract/:fn', function (req, res) {
    contractGenerator.generateContract(req.params.fn).then(result => {
        computationsController.setCreateTable(result.createTable);
        computationsController.setTableName(result.tableName);
        createTable = result.createTable;
        return res.send({ message: 'OK', 'filename': result.filename + '.sol', 'template': result.template });
    }).catch(err => {
        /* istanbul ignore next */
        console.log(err);
        /* istanbul ignore next */
        return res.send(helper.errorToJson(err));
    });
});

app.get('/getFactById/:id', contractController.contractChecker, function (req, res) {
    contractController.getFactById(req.params.id).then(result => {
        res.send(stringify(result).replace(/\\/g, ''));
    }).catch(error => {
        /* istanbul ignore next */
        helper.log(error);
        /* istanbul ignore next */
        res.send(helper.errorToJson(error));
    });
});

app.get('/getFactsFromTo/:from/:to', contractController.contractChecker, function (req, res) {
    let timeStart = helper.time();
    contractController.getFactsFromTo(parseInt(req.params.from), parseInt(req.params.to)).then(retval => {
        let timeFinish = helper.time() - timeStart;
        retval.push({ time: timeFinish });
        res.send(stringify(retval).replace(/\\/g, ''));
    }).catch(err => {
        /* istanbul ignore next */
        res.send(helper.errorToJson(err));
    });
});

app.get('/allfacts', contractController.contractChecker, function (req, res) {
    contractController.getLatestId().then(async result => {
        // async loop waiting to get all the facts separately
        let timeStart = helper.time();
        contractController.getAllFactsHeavy(result).then(retval => {
            let timeFinish = helper.time() - timeStart;
            retval.push({ time: timeFinish });
            res.send(stringify(retval).replace(/\\/g, ''));
        }).catch(error => {
            /* istanbul ignore next */
            console.log(error);
        });
    }).catch(err => {
        /* istanbul ignore next */
        helper.log(err);
        /* istanbul ignore next */
        res.send(helper.errorToJson(err));
    });
});

app.get('/groupbyId/:id', contractController.contractChecker, function (req, res) {
    contractController.getGroupByWithId(req.params.id).then(result => {
        return res.send(stringify(result).replace(/\\/g, ''));
    }).catch(error => {
        /* istanbul ignore next */
        helper.log(error);
        /* istanbul ignore next */
        return res.send(helper.errorToJson(error));
    });
});

app.get('/getViewByName/:viewName/:contract', contractController.contractChecker, async function (req, res) {
    if(process.env.TESTS) {
        config = reload('./config_private');
    }
    const totalStart = helper.time();
    let factTbl = require('./templates/' + req.params.contract);
    const viewsDefined = factTbl.views;
    let viewMap = new Map();
    for (let crnView in viewsDefined) {
        factTbl.views[crnView].id = crnView;
        viewMap.set(factTbl.views[crnView].name, factTbl.views[crnView]);
    }
    const view = helper.checkViewExists(viewMap, req.params.viewName);
    // returns an empty object if view not exist, otherwise it returns the view object
    if (Object.keys(view).length === 0 && view.constructor === Object) {
        res.status(200);
        return res.send({ status:'ERROR', message: 'view not found' });
    }
    await helper.updateViewFrequency(factTbl, req.params.contract, view.id);
    if (!gbRunning && !running) {
        gbRunning = true;
        viewMaterializationController.materializeView(view,
            req.params.contract, totalStart, createTable)
            .then(result => {
                gbRunning = false;
                res.status(200);
                io.emit('view_results', stringify(result).replace(/\\/g, ''));
                return res.send(stringify(result).replace(/\\/g, ''));
            }).catch(err => {
                /* istanbul ignore next */
                gbRunning = false;
            /* istanbul ignore next */
                return res.send(stringify(err))
            });
    }
});

app.get('/getcount', contractController.contractChecker, function (req, res) {
    contractController.getFactsCount().then(result => {
        if (result === -1) {
            return res.send({ status: 'ERROR', message: 'Error getting count' });
        } else {
            return res.send(result);
        }
    });
});

app.post('/addFact', contractController.contractChecker, function (req, res) {
    contractController.addFact(req.body).then(receipt => {
        res.send(receipt);
    }).catch(error => {
        /* istanbul ignore next */
        helper.log(error);
        /* istanbul ignore next */
        res.send(helper.errorToJson(error));
    })
});

module.exports = app;
