const express = require('express');
const bodyParser = require('body-parser');
const solc = require('solc');
const fs = require('fs');
const groupBy = require('group-by');
const config = require('./config_private');
let fact_tbl = require('./templates/fact_tbl');
const crypto = require('crypto');
let md5sum = crypto.createHash('md5');
abiDecoder = require('abi-decoder');
const app = express();
const jsonParser = bodyParser.json();
const helper = require('./helpers/helper');
const contractGenerator = require('./helpers/contractGenerator');
const transformations = require('./helpers/transformations');
app.use(jsonParser);
let running = false;
let gbRunning = false;
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');
app.use(express.static('public'));
const microtime = require('microtime');
let http = require('http').Server(app);
let io = require('socket.io')(http);
const jsonSql = require('json-sql')({separatedValues: false});

const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider(config.blockchainIP));
const redis = require('redis');
const client = redis.createClient(config.redisPort, config.redisIP);
client.on('connect', function () {
    console.log('Redis connected');
});
client.on('error', function (err) {
    console.log('Something went wrong ' + err);
});

const mysql = require('mysql');
let createTable = '';
let tableName = '';
let connection = null;
//let contractInstance = null;
let contractsDeployed = [];

web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let acc = null;
let mainTransactionObject = {};
app.get('/', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        res.render('index', { 'templates': items });
    })
});

app.get('/dashboard', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        web3.eth.getBlockNumber().then(blockNum => {
            res.render('dashboard', { 'templates': items, 'blockNum': blockNum });
        });
    });
});

app.get('/form/:contract', function (req, res) {
    let fact_tbl = require('./templates/' + req.params.contract);
    let templ = {};
    if ('template' in fact_tbl) {
        templ = fact_tbl['template'];
    } else {
        templ = fact_tbl;
    }
    let address = '0';
    for (let i = 0; i < contractsDeployed.length; i++) {
        if (contractsDeployed[i].contractName === fact_tbl.name) {
            address = contractsDeployed[i].address;
            break;
        }
    }
    let fbsField = fact_tbl.groupBys.TOP.children;

    let groupBys = helper.flatten(fbsField);
    groupBys = groupBys.map(function (obj) {
        return obj.fields;
    });
    let readyViews = fact_tbl.views;
    readyViews = readyViews.map(x => x.name);
    groupBys = helper.removeDuplicates(groupBys);
    groupBys.push(fact_tbl.groupBys.TOP.fields);
    console.log(groupBys);
    res.render('form',{'template':templ, 'name': fact_tbl.name, 'address': address, 'groupBys':groupBys, 'readyViews': readyViews});
});

http.listen(3000, () => {
    console.log(`Smart-Views listening on http://localhost:3000/dashboard`);
    let mysqlConfig = {};
    let validations = helper.configFileValidations();
    if(validations.passed) {
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
        console.log("Config file validations failed");
        console.log(validations);
        //if config validations fail, stop the server
        process.exit(1);
    }
});

async function deploy(account, contractPath) {
    const input = fs.readFileSync(contractPath);
    const output = solc.compile(input.toString(), 1);
    console.log(output);
    const bytecode = output.contracts[Object.keys(output.contracts)[0]].bytecode;
    const abi = JSON.parse(output.contracts[Object.keys(output.contracts)[0]].interface);

    contract = new web3.eth.Contract(abi);
    let contractInstance =  await contract.deploy({data: '0x' + bytecode})
        .send({
            from: account,
            gas: 150000000,
            gasPrice: '30000000000000'
        }, (err, txHash) => {
            console.log('send:', err, txHash);
        })
        .on('error', (err) => {
            console.log('error:', err);
        })
        .on('transactionHash', (err) => {
            console.log('transactionHash:', err);
        })
        .on('receipt', (receipt) => {
            console.log('receipt:', receipt);
            contract.options.address = receipt.contractAddress;
            contractsDeployed.push({contractName: Object.keys(output.contracts)[0].slice(1), address: receipt.contractAddress});
            console.log(contractsDeployed);
        });
    return contractInstance.options;
}

app.get('/deployContract/:fn', function (req, res) {
    web3.eth.getAccounts(function (err, accounts) {
        if (!err) {
            acc = accounts[1];
            mainTransactionObject = {
                from: acc,
                gas: 1500000000000,
                gasPrice: '30000000000000'
            };
            console.log(req.params.fn);
            deploy(accounts[0], './contracts/' + req.params.fn)
                .then(options => {
                    console.log('Success');
                    res.send({status:'OK', options: options});
                })
                .catch(err => {
                    console.log('error on deploy ' + err);
                    res.status(400);
                    res.send({status:'ERROR', options: 'Deployment failed'});
                })
        }
    });
});

async function addManyFacts(facts, sliceSize) {
    console.log('length = ' + facts.length);
    let allSlicesReady = [];
    if(sliceSize > 1) {
        let slices = [];
        let slicesNum = Math.ceil(facts.length / sliceSize);
        console.log("*will add " + slicesNum + " slices*");

        for (let j = 0; j < slicesNum; j++) {
            if (j === 0) {
                slices[j] = facts.filter((fct, idx) => idx < sliceSize);
            } else {
                slices[j] = facts.filter((fct, idx) => idx > j * sliceSize && idx < (j + 1) * sliceSize);
            }
        }

        allSlicesReady = slices.map(slc => {
            return slc.map(fct => {
                return JSON.stringify(fct);
            });
        });
    } else {
        allSlicesReady = facts.map(fact => {
            return [JSON.stringify(fact)];
        });
    }

    let i = 1;
    for(const slc of allSlicesReady){
        await contract.methods.addFacts(slc).send(mainTransactionObject, (err, txHash) => {
        }).on('error', (err) => {
            console.log('error:', err);
        }).on('transactionHash', (hash) => {
            console.log(i);
            io.emit('progress', i/allSlicesReady.length);
            i++;
        });
    }
    return Promise.resolve(true);
}


app.get('/load_dataset/:dt', function (req, res) {
    let dt = require('./testData/' + req.params.dt);
    console.log("ENDPOINT HIT AGAIN");
    console.log(running);
    if (contract) {
        if (!running) {
            running = true;
            let startTime = microtime.nowDouble();
            addManyFacts(dt,config.recordsSlice).then(retval => {
                let endTime = microtime.nowDouble();
                let timeDiff = endTime - startTime;
                running = false;
                io.emit("DONE","TRUE");
                console.log("Added " + dt.length + " records in " + timeDiff + " seconds");
                return res.send('DONE');
            }).catch(error => {
                console.log(error);
            })
        }
    } else {
        res.status(400);
        res.send({status: 'ERROR',options: 'Contract not deployed' });
    }
});

app.get('/new_contract/:fn', function (req, res) {
    contractGenerator.generateContract(req.params.fn).then(function(result){
        createTable = result.createTable;
        tableName = result.tableName;
        return res.send({ msg: 'OK', 'filename':result.filename + '.sol', 'template': result.template });
    } , function(err) {
        console.log(err);
        return res.send({ msg: 'error' });
    });
});

app.get('/getFactById/:id', function (req, res) {
    if (contract) {
        contract.methods.getFact(parseInt(req.params.id,10)).call(function (err, result) {
            if (!err) {
                let len = Object.keys(result).length;
                for (let j = 0; j < len / 2; j ++) {
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

async function getAllFactsHeavy(factsLength) {
    let allFacts = [];
    await contract.methods.getAllFacts(factsLength).call(function (err, result) {
        if (!err) {
            let len  = Object.keys(result).length;
            for (let  j = 0; j < len / 2; j ++) {
                delete result[j];
            }
            if ('payloads' in result) {
                for (let i = 0; i < result['payloads'].length; i++) {
                    let crnLn = JSON.parse(result['payloads'][i]);
                    crnLn.timestamp =  result['timestamps'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            console.log(err);
        }
    });
    return allFacts;
}

async function getAllFacts(factsLength) {
    let allFacts = [];
    for (let i = 0; i < factsLength; i++) {
        await contract.methods.facts(i).call(function (err, result2) {
            if (!err) {
                let len  = Object.keys(result2).length;
                for (let  j = 0; j < len / 2; j ++) {
                    delete result2[j];
                }
                console.log("got fact " + i);
                if ('payload' in result2) {
                    let crnLn = JSON.parse(result2['payload']);
                    crnLn.timestamp = result2['timestamp'];
                    allFacts.push(crnLn);
                }
            } else {
                console.log(err);
            }
        })
    }
    return allFacts;
}

async function getFactsFromTo(from, to) {
    let allFacts = [];
        await contract.methods.getFactsFromTo(from, to).call(function (err, result) {
            if (!err) {
                let len  = Object.keys(result).length;
                for (let  j = 0; j < len / 2; j ++) {
                    delete result[j];
                }
                if ('payloadsFromTo' in result) {
                    for (let i = 0; i < result['payloadsFromTo'].length; i++) {
                        let crnLn = JSON.parse(result['payloadsFromTo'][i]);
                        crnLn.timestamp =  result['timestampsFromTo'][i];
                        allFacts.push(crnLn);
                    }
                }
            } else {
                console.log(err);
            }
        });
    return allFacts;
}

app.get('/getFactsFromTo/:from/:to', function (req,res) {
    let timeStart = microtime.nowDouble();
   getFactsFromTo(parseInt(req.params.from), parseInt(req.params.to)).then(retval => {
       let timeFinish = microtime.nowDouble() - timeStart;
           retval.push({time: timeFinish});
           res.send(retval);
   }).catch(err =>{
       res.send(err);
   });
});

app.get('/allfacts', function (req, res) {
    if (contract) {
        contract.methods.dataId().call(function (err, result) {
            if (!err) {
                // async loop waiting to get all the facts separately
                let timeStart = microtime.nowDouble();
                getAllFactsHeavy(result).then(retval => {
                    let timeFinish = microtime.nowDouble() - timeStart;
                    console.log('Get all facts time: ' + timeFinish + ' s');
                    retval.push({time: timeFinish});
                    //retval.timeDone = microtime.nowDouble() - timeStart;
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
        res.send({status: 'ERROR',options: 'Contract not deployed' });
    }
});

app.get('/groupbyId/:id', function (req, res) {
    if (contract) {
        contract.methods.getGroupBy(parseInt(req.params.id,10)).call(function (err, result) {
            if (!err) {
                res.send(result)
            } else {
                console.log(err);
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: 'ERROR',options: 'Contract not deployed' });
    }
});

//this function  assigns a cost to each group by
function calculationCost(groupBys) {
    for(let i = 0; i < groupBys.length; i++){
        let crnGroupBy = groupBys[i];
        let crnCost = (0.5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        crnGroupBy.cost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function cacheEvictionCost(groupBys) {
    for(let i = 0; i < groupBys.length; i++){
        let crnGroupBy = groupBys[i];
        let crnCost = (5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        crnGroupBy.cacheEvictionCost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function cacheEvictionCostOfficial(groupBys, latestFact) { //the functio we write on paper
    //where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    //which is the cost to materialize view V from view Vi (where V < Vi)
    let a = 10; //factor of deltas
    let sizeDeltas = 0;
    let sizeCached = 0; //crnGroubBy
    for(let i = 0; i < groupBys.length; i++){
        let crnGroupBy = groupBys[i];
        console.log("@@");
        console.log(crnGroupBy);
        console.log("@@");
        sizeDeltas = latestFact - Number.parseInt(crnGroupBy.latestFact); //latestFact is the latest fact written in bc
        sizeCached = Number.parseInt(crnGroupBy.size);
        console.log("sizeCached = " + sizeCached);
        console.log("sizeDeltas = " + sizeDeltas);
        let crnCost =  a * sizeDeltas + sizeCached;
        console.log("final cost = " + crnCost);
        crnGroupBy.cacheEvictionCost = crnCost;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function containsAllFields(transformedArray, view) {
    for (let i = 0; i < transformedArray.length; i++) {
        let containsAllFields = true;
        let crnView = transformedArray[i];

        let cachedGBFields = JSON.parse(crnView.columns);
        for(let index in cachedGBFields.fields){
            cachedGBFields.fields[index] = cachedGBFields.fields[index].trim();
        }
        console.log(cachedGBFields);
        for (let j = 0; j < view.gbFields.length; j++) {
            console.log(view.gbFields[j]);
            if (!cachedGBFields.fields.includes(view.gbFields[j])) {
                containsAllFields = false
            }
        }
        transformedArray[i].containsAllFields = containsAllFields;
    }
    return transformedArray;
}

function saveOnCache(gbResult, operation, latestId){
    console.log("SAVE ON CACHE BEGUN");
    md5sum = crypto.createHash('md5');
    md5sum.update(JSON.stringify(gbResult));
    let hash = md5sum.digest('hex');
    let gbResultSize = Object.keys(gbResult).length;
    let slicedGbResult = [];
    if(config.autoCacheSlice === "manual") {
        if (gbResultSize > config.cacheSlice) {
            let crnSlice = [];
            let metaKeys = {
                operation: gbResult["operation"],
                groupByFields: gbResult["groupByFields"],
                field: gbResult["field"]
            };
            for (const key of Object.keys(gbResult)) {
                if (key !== "operation" && key !== "groupByFields" && key !== "field") {
                    console.log(key);
                    crnSlice.push({[key]: gbResult[key]});
                    if (crnSlice.length >= config.cacheSlice) {
                        slicedGbResult.push(crnSlice);
                        crnSlice = [];
                    }
                }
            }
            if (crnSlice.length > 0) {
                slicedGbResult.push(crnSlice); //we have a modulo, slices are not all evenly dιstributed, the last one contains less than all the previous ones
            }
            slicedGbResult.push(metaKeys);
        }
    } else {
        //redis allows 512MB per stored string, so we divide the result of our gb with 512MB to find cache slice
        //maxGbSize is the max number of bytes in a row of the result
        let mb512InBytes = 512 * 1024 * 1024;
        let maxGbSize = config.maxGbSize;
        console.log("GB RESULT SIZE in bytes = " + gbResultSize * maxGbSize);
        console.log("size a cache position can hold in bytes: " +  mb512InBytes);
        if ((gbResultSize * maxGbSize) > mb512InBytes) {
            //  let cacheSlice = mb512InBytes / config.maxGbSize;
            let crnSlice = [];
            let metaKeys = {
                operation: gbResult["operation"],
                groupByFields: gbResult["groupByFields"],
                field: gbResult["field"]
            };
            let rowsAddedInslice = 0;
            let crnSliceLengthInBytes = 0;
            for (const key of Object.keys(gbResult)) {
                if (key !== "operation" && key !== "groupByFields" && key !== "field") {
                    console.log(key);
                    crnSlice.push({[key]: gbResult[key]});
                    rowsAddedInslice++;
                    crnSliceLengthInBytes = rowsAddedInslice * maxGbSize;
                    console.log("Rows added in slice:");
                    console.log(rowsAddedInslice);
                    if (crnSliceLengthInBytes === (mb512InBytes - 40)) { //for hidden character like backslashes etc
                        slicedGbResult.push(crnSlice);
                        crnSlice = [];
                    }
                }
            }
            if (crnSlice.length > 0) {
                slicedGbResult.push(crnSlice); //we have a modulo, slices are not all evenly dιstributed, the last one contains less than all the previous ones
            }
            slicedGbResult.push(metaKeys);
        } else {
            console.log("NO SLICING NEEDED");
        }
    }
    let colSize = gbResult.groupByFields.length;
    let columns = JSON.stringify({fields: gbResult.groupByFields});
    let num = 0;
    let crnHash = "";
    if(slicedGbResult.length > 0) {
        for (const slice in slicedGbResult) {
            crnHash = hash + "_" + num;
            console.log(crnHash);
            client.set(crnHash, JSON.stringify(slicedGbResult[slice]), redis.print);
            num++;
        }
    } else {
        crnHash = hash + "_0";
        client.set(crnHash, JSON.stringify(gbResult), redis.print);
    }
   return contract.methods.addGroupBy(crnHash, Web3.utils.fromAscii(operation), latestId, colSize, gbResultSize, columns).send(mainTransactionObject);
}


function calculateNewGroupBy(facts, operation, gbFields, aggregationField, callback) {
    connection.query('DROP TABLE IF EXISTS ' + tableName, function (err, resultDrop) {
        if (err) {
            console.log(err);
            callback(null, err);
        }
        connection.query(createTable, function (error, results, fields) { //creating the SQL table for "Fact Table"
            if (error) {
                console.log(error);
                callback(null, error);
            }
            if(facts.length === 0){
                callback(null, {error: "No facts"});
            }
            let sql = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: facts
            });

            let editedQuery = sql.query.replace(/"/g, '');
            editedQuery = editedQuery.replace(/''/g, 'null');
            connection.query(editedQuery, function (error, results2, fields) { //insert facts
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
                                    name: 'SUM', args: [{field: aggregationField}]
                                }
                            },
                            {
                                func: {
                                    name: 'COUNT', args: [{field: aggregationField}]
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
                                    args: [{field: aggregationField}]
                                }
                            }]
                    });
                }
                let editedGB = gbQuery.query.replace(/"/g, '');
                connection.query(editedGB, function (error, results3, fields) {
                    if (error) {
                        console.log(error);
                        callback(null, error);
                    }
                    console.log('DROP TABLE ' + tableName);
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

function calculateReducedGroupBy(cachedGroupBy,view, gbFields, callback) {
    //this means we want to calculate a different group by than the stored one
    //but however it can be calculated just from redis cache
    //calculating the reduced Group By in SQL
        console.log(cachedGroupBy);
        let tableName = cachedGroupBy.gbCreateTable.split(" ");
        tableName = tableName[3];
        tableName = tableName.split('(')[0];
        console.log("TABLE NAME = " + tableName);
        connection.query(cachedGroupBy.gbCreateTable, async function (error, results, fields) {
            if (error) {
                callback(null, error);
            }
            let rows = [];
            let lastCol = "";
            let prelastCol = ""; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
            await Object.keys(cachedGroupBy).forEach(function (key, index) {
                if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                    let crnRow = JSON.parse(key);
                    lastCol = cachedGroupBy.gbCreateTable.split(" ");
                    prelastCol = lastCol[lastCol.length - 4];
                    lastCol = lastCol[lastCol.length - 2];
                    let gbVals = Object.values(cachedGroupBy);
                    if (view.operation === "AVERAGE") {
                        crnRow[prelastCol] = gbVals[index]["sum"];
                        crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                    } else {
                        crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                    }
                    rows.push(crnRow);
                }
            });
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
                let op = "";
                if (view.operation === "SUM" || view.operation === "COUNT") {
                    op = "SUM"; //operation is set to "SUM" both for COUNT and SUM operation
                } else if (view.operation === "MIN") {
                    op = "MIN"
                } else if (view.operation === "MAX") {
                    op = "MAX";
                }
                let gbQuery = jsonSql.build({
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
                if (view.operation === "AVERAGE") {
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
                connection.query(editedGBQuery, function (error, results, fields) {
                    if (error) {
                        console.log(error);
                        callback(null, error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        callback(results);
                    });
                });
            });
        });
}

function mergeGroupBys(groupByA, groupByB, gbCreateTable, tableName, view, lastCol, prelastCol, callback){
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

        connection.query(editedQueryA,  function (err, results, fields) {
            if(err) {
                console.log(err);
                callback(null, err);
            }
            connection.query(editedQueryB,  function (err, results, fields) {
                let gbQuery = {};
                if (err) {
                    console.log(err);
                    callback(null, err);
                }
                let op = "";
                if (view.operation === "SUM" || view.operation === "COUNT") {
                    op = "SUM"; //operation is set to "SUM" both for COUNT and SUM operation
                } else if (view.operation === "MIN") {
                    op = "MIN"
                } else if (view.operation === "MAX") {
                    op = "MAX";
                }
                gbQuery = jsonSql.build({
                    type: 'select',
                    table: tableName,
                    group: view.gbFields,
                    fields: [view.gbFields,
                        {
                            func: {
                                name: op,
                                args: [{field: lastCol}]
                            }
                        }]
                });
                if (view.operation === "AVERAGE") {
                    gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: view.gbFields,
                        fields: [view.gbFields,
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
                connection.query(editedGBQuery, async function (error, results, fields) {
                    if (error) {
                        console.log(error);
                        callback(null,error);
                    }
                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                        if (err) {
                            console.log(err);
                            callback(null, err);
                        }

                        let groupBySqlResult = {};
                        if (view.operation === "AVERAGE") {
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

function deleteFromCache(evicted, callback){
        let keysToDelete = [];
        let gbIdsToDelete = [];
        if(config.cacheEvictionPolicy === "FIFO"){
            for(let i = 0; i < config.maxCacheSize; i++){
                keysToDelete.push(evicted[i].hash);
                let crnHash = evicted[i].hash;
                let cachedGBSplited = crnHash.split("_");
                let cachedGBLength = parseInt(cachedGBSplited[1]);
                if(cachedGBLength > 0){  //reconstructing all the hashes in cache if it is sliced
                    for(let j = 0; j < cachedGBLength; j++){
                        keysToDelete.push(cachedGBSplited[0] + "_"+j);
                    }
                }
                gbIdsToDelete[i] = evicted[i].id;
            }
            console.log("keys to remove from cache are:");
            console.log(keysToDelete);
        }
        client.del(keysToDelete);
        callback(gbIdsToDelete);
}

app.get('/getViewByName/:viewName', function (req,res) {
    let totalStart = microtime.nowDouble();
    let fact_tbl = require('./templates/ABCD');
    //let fact_tbl = require('./templates/new_sales_min');
    let viewsDefined = fact_tbl.views;
    console.log(req.params.viewName);
    let found = false;
    let view = {};
    for(let crnView in viewsDefined){
        if(fact_tbl.views[crnView].name === req.params.viewName) {
            found = true;
            view = fact_tbl.views[crnView];
            break;
        }
    }

    if(!found){
        return res.send({error: "view not found"});
    }


    console.log("VIEW BY NAME ENDPOINT HIT AGAIN");
    console.log(gbRunning);
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
                console.log("cache enabled = TRUE");
            let getGroupIdTimeStart = microtime.nowDouble();
            contract.methods.groupId().call(function (err, result) {
                let getGroupIdTimeEnd = microtime.nowDouble();
                if (!err) {
                    if (result > 0) { //At least one group by already exists
                        let getAllGBsFromBCTimeStart = microtime.nowDouble();
                        contract.methods.getAllGroupBys(result).call(async function (err, resultGB) {
                            let getAllGBsFromBCTimeEnd = microtime.nowDouble();
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

                                transformedArray = containsAllFields(transformedArray, view); //assigns the containsAllFields value
                                let filteredGBs = [];
                                let sortedByEvictionCost = [];
                                for (let i = 0; i < transformedArray.length; i++) { //filter out the group bys that DO NOT CONTAIN all the fields we need -> aka containsAllFields = false
                                    if (transformedArray[i].containsAllFields) {
                                        filteredGBs.push(transformedArray[i]);
                                    }
                                    sortedByEvictionCost.push(transformedArray[i]);
                                }

                                await contract.methods.dataId().call(function (err, latestId) {
                                    sortedByEvictionCost = cacheEvictionCostOfficial(sortedByEvictionCost, latestId);
                                    filteredGBs = cacheEvictionCostOfficial(filteredGBs, latestId);
                                });
                                console.log("cache eviction costs assigned:");
                                console.log(sortedByEvictionCost);


                                sortedByEvictionCost.sort(function (a, b) {
                                    if (config.cacheEvictionPolicy === "FIFO") {
                                        return parseInt(a.gbTimestamp) - parseInt(b.gbTimestamp);
                                    } else if (config.cacheEvictionPolicy === "costFunction") {
                                        return parseFloat(a.cacheEvictionCost) - parseInt(b.cacheEvictionCost);
                                    }
                                });

                                console.log("SORTED GBs by eviction cost:");
                                console.log(sortedByEvictionCost); //TS ORDER ascending, the first ones are less "expensive" than the last ones.
                                console.log("________________________");
                                //assign costs
                               // filteredGBs = cacheEvictionCostOfficial(filteredGBs, latestId);

                                //pick the one with the less cost
                                filteredGBs.sort(function (a, b) {
                                    return parseFloat(a.cost) - parseFloat(b.cost)
                                }); //order ascending
                                let mostEfficient = filteredGBs[0]; // TODO: check what we do in case we have no groub bys that match those criteria
                                console.log("MOST EFFICIENT IS:");
                                console.log(mostEfficient);
                                let getLatestFactIdTimeStart = microtime.nowDouble();
                                contract.methods.dataId().call(function (err, latestId) {
                                    console.log("LATEST ID IS:");
                                    console.log(latestId);
                                    let getLatestFactIdTimeEnd = microtime.nowDouble();
                                    if (err) {
                                        console.log(err);
                                        gbRunning = false;
                                        return res.send(err);
                                    }
                                    if (mostEfficient.gbTimestamp > 0) {
                                        console.log("MOST EFF > 0");
                                        let getLatestFactTimeStart = microtime.nowDouble();
                                        contract.methods.getFact(latestId - 1).call(function (err, latestFact) {
                                            let getLatestFactTimeEnd = microtime.nowDouble();
                                            if (err) {
                                                console.log(err);
                                                gbRunning = false;
                                                return res.send(err);
                                            }
                                            console.log(latestFact);
                                            if (mostEfficient.gbTimestamp > latestFact.timestamp) {
                                                console.log("NO NEW FACTS");
                                                //NO NEW FACTS after the latest group by
                                                // -> incrementaly calculate the groupby requested by summing the one in redis cache
                                                let hashId = mostEfficient.hash.split("_")[1];
                                                let hashBody = mostEfficient.hash.split("_")[0];
                                                let allHashes = [];
                                                for (let i = 0; i <= hashId; i++) {
                                                    allHashes.push(hashBody + "_" + i);
                                                }
                                                let cacheRetrieveTimeStart = microtime.nowDouble();
                                                client.mget(allHashes, function (error, allCached) {
                                                    let cacheRetrieveTimeEnd = microtime.nowDouble();
                                                    if (error) {
                                                        console.log(error);
                                                        gbRunning = false;
                                                        return res.send(error);
                                                    }
                                                    let cachedGroupBy = {};
                                                    if (allCached.length === 1) { //it is <= of slice size, so it is not sliced
                                                        cachedGroupBy = JSON.parse(allCached[0]);
                                                    } else { //it is sliced
                                                        let mergedArray = [];
                                                        for (const index in allCached) {
                                                            let crnSub = allCached[index];
                                                            let crnSubArray = JSON.parse(crnSub);
                                                            for (const kv in crnSubArray) {
                                                                if (kv !== "operation" && kv !== "groupByFields" && kv !== "field") {
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
                                                        //this means we want to calculate a different group by than the stored one
                                                        //but however it can be calculated just from redis cache
                                                        if (cachedGroupBy.field === view.aggregationField &&
                                                            view.operation === cachedGroupBy.operation) {
                                                            //caclculating the reduced Group By in SQL
                                                            console.log(cachedGroupBy);
                                                            let tableName = cachedGroupBy.gbCreateTable.split(" ");
                                                            tableName = tableName[3];
                                                            tableName = tableName.split('(')[0];
                                                            console.log("TABLE NAME = " + tableName);
                                                            let sqlTimeStartCT = microtime.nowDouble();
                                                            connection.query(cachedGroupBy.gbCreateTable, async function (error, results, fields) {
                                                                let sqlTimeEndCT = microtime.nowDouble();
                                                                if (error) throw error;
                                                                let rows = [];
                                                                let lastCol = "";
                                                                let prelastCol = ""; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                await Object.keys(cachedGroupBy).forEach(function (key, index) {
                                                                    if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                                                                        let crnRow = JSON.parse(key);
                                                                        lastCol = cachedGroupBy.gbCreateTable.split(" ");
                                                                        prelastCol = lastCol[lastCol.length - 4];
                                                                        lastCol = lastCol[lastCol.length - 2];
                                                                        let gbVals = Object.values(cachedGroupBy);
                                                                        if (view.operation === "AVERAGE") {
                                                                            crnRow[prelastCol] = gbVals[index]["sum"];
                                                                            crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                                                                        } else {
                                                                            crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                                                                        }
                                                                        rows.push(crnRow);
                                                                    }
                                                                });
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
                                                                    let op = "";
                                                                    let gbQuery = {};
                                                                    if (view.operation === "SUM" || view.operation === "COUNT") {
                                                                        op = "SUM"; //operation is set to "SUM" both for COUNT and SUM operation
                                                                    } else if (view.operation === "MIN") {
                                                                        op = "MIN"
                                                                    } else if (view.operation === "MAX") {
                                                                        op = "MAX";
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
                                                                    if (view.operation === "AVERAGE") {
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
                                                                            if (view.operation === "AVERAGE") {
                                                                                groupBySqlResult = transformations.transformReadyAverage(results, view.gbFields, view.aggregationField);
                                                                            } else {
                                                                                groupBySqlResult = transformations.transformGBFromSQL(results, op, lastCol, gbFields);
                                                                            }
                                                                            groupBySqlResult.operation = view.operation;
                                                                            groupBySqlResult.field = view.aggregationField;
                                                                            let cacheSaveTimeStart = microtime.nowDouble();
                                                                            return saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                console.log('error:', err);
                                                                                gbRunning = false;
                                                                                return res.send(err);
                                                                            }).on('transactionHash', (err) => {
                                                                                console.log('transactionHash:', err);
                                                                            }).on('receipt', (receipt) => {
                                                                                console.log('receipt:', receipt);
                                                                                let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                    deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                        contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            if (!err) {
                                                                                                groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                groupBySqlResult.sqlTime = (sqlTimeEndCT - sqlTimeStartCT) + (sqlTimeEndInsert - sqlTimeStartInsert) + (sqlTimeEndSelect - sqlTimeStartSelect) + (sqlTimeEndDrop - sqlTimeStartDrop);
                                                                                                groupBySqlResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                groupBySqlResult.totalTime = groupBySqlResult.cacheSaveTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheRetrieveTime;
                                                                                                groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                                io.emit('view_results', JSON.stringify(groupBySqlResult).replace("\\", ""));
                                                                                                gbRunning = false;
                                                                                                return res.send(JSON.stringify(groupBySqlResult).replace("\\", ""));
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
                                                                                    io.emit('view_results', JSON.stringify(groupBySqlResult).replace("\\", ""));
                                                                                    gbRunning = false;
                                                                                    return res.send(JSON.stringify(groupBySqlResult).replace("\\", ""));
                                                                                }
                                                                            });
                                                                        });
                                                                    });
                                                                });
                                                            });
                                                        } else {
                                                            //some fields contained in a Group by but operation and aggregation fields differ
                                                            //this means we should proceed to new group by calculation from the begining
                                                            let bcTimeStart = microtime.nowDouble();
                                                            getAllFacts(latestId).then(retval => {
                                                                let bcTimeEnd = microtime.nowDouble();
                                                                for (let i = 0; i < retval.length; i++) {
                                                                    delete retval[i].timestamp;
                                                                }
                                                                console.log("CALCULATING NEW GB FROM BEGGINING");
                                                                let sqlTimeStart = microtime.nowDouble();
                                                                calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                    let sqlTimeEnd = microtime.nowDouble();
                                                                    if (error) {
                                                                        gbRunning = false;
                                                                        return res.send(error);
                                                                    }
                                                                    groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                    groupBySqlResult.field = view.aggregationField;
                                                                    let cacheSaveTimeStart = microtime.nowDouble();
                                                                    saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                        console.log('error:', err);
                                                                        gbRunning = false;
                                                                        return res.send(err);
                                                                    }).on('transactionHash', (err) => {
                                                                        console.log('transactionHash:', err);
                                                                    }).on('receipt', (receipt) => {
                                                                        let cacheSaveTimeEnd = microtime.nowDouble();
                                                                        delete groupBySqlResult.gbCreateTable;
                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                            deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                    let totalEnd = microtime.nowDouble();
                                                                                    if (!err) {
                                                                                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                        groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                        console.log('receipt:', receipt);
                                                                                        io.emit('view_results', JSON.stringify(groupBySqlResult));
                                                                                        gbRunning = false;
                                                                                        return res.send(JSON.stringify(groupBySqlResult));
                                                                                    }
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                });
                                                                            });
                                                                        } else {
                                                                            let totalEnd = microtime.nowDouble();
                                                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                            groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                            groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                            console.log('receipt:', receipt);
                                                                            io.emit('view_results', JSON.stringify(groupBySqlResult));
                                                                            gbRunning = false;
                                                                            return res.send(JSON.stringify(groupBySqlResult));
                                                                        }
                                                                    });
                                                                });
                                                            });

                                                        }
                                                    } else {
                                                        if (cachedGroupBy.field === view.aggregationField &&
                                                            view.operation === cachedGroupBy.operation) {
                                                            let totalEnd = microtime.nowDouble();
                                                            //this means we just have to return the group by stored in cache
                                                            //field, operation are same and no new records written
                                                            cachedGroupBy.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                            cachedGroupBy.totalTime = cachedGroupBy.cacheRetrieveTime;
                                                            cachedGroupBy.allTotal = totalEnd - totalStart;
                                                            io.emit('view_results', JSON.stringify(cachedGroupBy));
                                                            gbRunning = false;
                                                            return res.send(JSON.stringify(cachedGroupBy));
                                                        } else {
                                                            //same fields but different operation or different aggregate field
                                                            //this means we should proceed to new group by calculation from the begining
                                                            let bcTimeStart = microtime.nowDouble();
                                                            getAllFacts(latestId).then(retval => {
                                                                let bcTimeEnd = microtime.nowDouble();
                                                                for (let i = 0; i < retval.length; i++) {
                                                                    delete retval[i].timestamp;
                                                                }
                                                                console.log("CALCULATING NEW GB FROM BEGGINING");
                                                                let sqlTimeStart = microtime.nowDouble();
                                                                calculateNewGroupBy(retval, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                                                    let sqlTimeEnd = microtime.nowDouble();
                                                                    if (error) {
                                                                        gbRunning = false;
                                                                        return res.send(error);
                                                                    }
                                                                    groupBySqlResult.gbCreateTable = view.SQLTable;
                                                                    groupBySqlResult.field = view.aggregationField;
                                                                    let cacheSaveTimeStart = microtime.nowDouble();
                                                                    saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
                                                                        console.log('error:', err);
                                                                        gbRunning = false;
                                                                        return res.send(err);
                                                                    }).on('transactionHash', (err) => {
                                                                        console.log('transactionHash:', err);
                                                                    }).on('receipt', (receipt) => {
                                                                        let cachSaveTimeEnd = microtime.nowDouble();
                                                                        delete groupBySqlResult.gbCreateTable;
                                                                        if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                            deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                    let totalEnd = microtime.nowDouble();
                                                                                    if (!err) {
                                                                                        groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                                        groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                                        groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                                        console.log('receipt:', receipt);
                                                                                        io.emit('view_results', JSON.stringify(groupBySqlResult));
                                                                                        gbRunning = false;
                                                                                        return res.send(JSON.stringify(groupBySqlResult));
                                                                                    }
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                });
                                                                            });
                                                                        } else {
                                                                            let totalEnd = microtime.nowDouble();
                                                                            groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                                                                            groupBySqlResult.cacheSaveTime = cachSaveTimeEnd - cacheSaveTimeStart;
                                                                            groupBySqlResult.totalTime = groupBySqlResult.bcTime + groupBySqlResult.sqlTime + groupBySqlResult.cacheSaveTime;
                                                                            groupBySqlResult.allTotal = totalEnd - totalStart;
                                                                            console.log('receipt:', receipt);
                                                                            io.emit('view_results', JSON.stringify(groupBySqlResult));
                                                                            gbRunning = false;
                                                                            return res.send(JSON.stringify(groupBySqlResult));
                                                                        }
                                                                    });
                                                                });
                                                            });
                                                        }
                                                    }
                                                });
                                            } else {
                                                console.log("DELTAS DETECTED");
                                                //we have deltas -> we fetch them
                                                //CALCULATING THE VIEW JUST FOR THE DELTAS
                                                // THEN MERGE IT WITH THE ONES IN CACHE
                                                // THEN SAVE BACK IN CACHE
                                                let bcTimeStart = microtime.nowDouble();
                                                getFactsFromTo(mostEfficient.latestFact, latestId - 1).then(deltas => {
                                                    let bcTimeEnd = microtime.nowDouble();
                                                    connection.query(createTable, function (error, results, fields) {
                                                        if (error) throw error;
                                                        deltas = helper.removeTimestamps(deltas);
                                                        console.log("CALCULATING GB FOR DELTAS:");
                                                        let sqlTimeStart = microtime.nowDouble();
                                                        calculateNewGroupBy(deltas, view.operation, view.gbFields, view.aggregationField, async function (groupBySqlResult, error) {
                                                            let sqlTimeEnd = microtime.nowDouble();
                                                            if (error) {
                                                                gbRunning = false;
                                                                return res.send(error);
                                                            }
                                                            let hashId = mostEfficient.hash.split("_")[1];
                                                            let hashBody = mostEfficient.hash.split("_")[0];
                                                            let allHashes = [];
                                                            for (let i = 0; i <= hashId; i++) {
                                                                allHashes.push(hashBody + "_" + i);
                                                            }

                                                            let cacheRetrieveTimeStart = microtime.nowDouble();
                                                            client.mget(allHashes, async function (error, allCached) {
                                                                let cacheRetrieveTimeEnd = microtime.nowDouble();
                                                                if (error) {
                                                                    console.log(error);
                                                                    gbRunning = false;
                                                                    return res.send(error);
                                                                }

                                                                let cachedGroupBy = {};
                                                                if (allCached.length === 1) { //it is <= of slice size, so it is not sliced
                                                                    cachedGroupBy = JSON.parse(allCached[0]);
                                                                } else { //it is sliced
                                                                    let mergedArray = [];
                                                                    for (const index in allCached) {
                                                                        let crnSub = allCached[index];
                                                                        let crnSubArray = JSON.parse(crnSub);
                                                                        for (const kv in crnSubArray) {
                                                                            if (kv !== "operation" && kv !== "groupByFields" && kv !== "field") {
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

                                                                            let viewNameSQL = view.SQLTable.split(" ");
                                                                            viewNameSQL = viewNameSQL[3];
                                                                            viewNameSQL = viewNameSQL.split('(')[0];

                                                                            let rows = [];
                                                                            let rowsDelta = [];
                                                                            let lastCol = "";
                                                                            let prelastCol = null; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                            lastCol = view.SQLTable.split(" ");
                                                                            prelastCol = lastCol[lastCol.length - 4];
                                                                            lastCol = lastCol[lastCol.length - 2];

                                                                            //MERGE reducedResult with groupBySQLResult
                                                                            let op = "";
                                                                            if (view.operation === "SUM" || view.operation === "COUNT") {
                                                                                op = "SUM"; //operation is set to "SUM" both for COUNT and SUM operation
                                                                            } else if (view.operation === "MIN") {
                                                                                op = "MIN"
                                                                            } else if (view.operation === "MAX") {
                                                                                op = "MAX";
                                                                            }

                                                                            reducedResult = transformations.transformGBFromSQL(reducedResult, op, lastCol, gbFields);
                                                                            reducedResult.field = view.aggregationField;

                                                                            await Object.keys(reducedResult).forEach(function (key, index) {
                                                                                if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                                                                                    let crnRow = JSON.parse(key);
                                                                                    lastCol = view.SQLTable.split(" ");
                                                                                    prelastCol = lastCol[lastCol.length - 4];
                                                                                    lastCol = lastCol[lastCol.length - 2];
                                                                                    let gbVals = Object.values(reducedResult);
                                                                                    if (view.operation === "AVERAGE") {
                                                                                        crnRow[prelastCol] = gbVals[index]["sum"];
                                                                                        crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                                                                                    } else {
                                                                                        crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                                                                                    }
                                                                                    rows.push(crnRow);
                                                                                }
                                                                            });

                                                                            await Object.keys(groupBySqlResult).forEach(function (key, index) {
                                                                                if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                                                                                    let crnRow = JSON.parse(key);
                                                                                    lastCol = view.SQLTable.split(" ");
                                                                                    prelastCol = lastCol[lastCol.length - 4];
                                                                                    lastCol = lastCol[lastCol.length - 2];
                                                                                    let gbVals = Object.values(groupBySqlResult);
                                                                                    if (view.operation === "AVERAGE") {
                                                                                        crnRow[prelastCol] = gbVals[index]["sum"];
                                                                                        crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                                                                                    } else {
                                                                                        crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                                                                                    }
                                                                                    rowsDelta.push(crnRow);
                                                                                }
                                                                            });

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
                                                                                //save on cache before return
                                                                                let cacheSaveTimeStart = microtime.nowDouble();
                                                                                saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                    console.log('error:', err);
                                                                                    gbRunning = false;
                                                                                    return res.send(err);
                                                                                }).on('transactionHash', (err) => {
                                                                                    console.log('transactionHash:', err);
                                                                                }).on('receipt', (receipt) => {
                                                                                    let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                    delete mergeResult.gbCreateTable;
                                                                                    if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                        deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                            contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                                let totalEnd = microtime.nowDouble();
                                                                                                if (!err) {
                                                                                                    mergeResult.sqlTime = (sqlTimeEnd - sqlTimeStart) + (reductionTimeEnd - reductionTimeStart) + (mergeTimeEnd - mergeTimeStart);
                                                                                                    mergeResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                                    mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                    mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                    mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                    mergeResult.allTotal = totalEnd - totalStart;
                                                                                                    console.log("sql time = " + mergeResult.sqlTime);
                                                                                                    console.log("bc time = " + mergeResult.bcTime);
                                                                                                    console.log("cache save time = " + mergeResult.cacheSaveTime);
                                                                                                    console.log("cache retrieve time = " + mergeResult.cacheRetrieveTime);
                                                                                                    console.log("total time = " + mergeResult.totalTime);
                                                                                                    console.log("all total time = " + mergeResult.allTotal);
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
                                                                                        mergeResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                        mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                        mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                        mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                        mergeResult.allTotal = totalEnd - totalStart;
                                                                                        console.log("sql time = " + mergeResult.sqlTime);
                                                                                        console.log("bc time = " + mergeResult.bcTime);
                                                                                        console.log("cache save time = " + mergeResult.cacheSaveTime);
                                                                                        console.log("cache retrieve time = " + mergeResult.cacheRetrieveTime);
                                                                                        console.log("total time = " + mergeResult.totalTime);
                                                                                        console.log("all total time = " + mergeResult.allTotal);
                                                                                        console.log('receipt:', receipt);
                                                                                        io.emit('view_results', mergeResult);
                                                                                        gbRunning = false;
                                                                                        return res.send(mergeResult);
                                                                                    }
                                                                                });
                                                                            });
                                                                        });
                                                                    } else {
                                                                        console.log("GB FIELDS OF DELTAS AND CACHED ARE THE SAME")
                                                                        //group by fields of deltas and cached are the same so
                                                                        //MERGE cached and groupBySqlResults
                                                                        let viewNameSQL = view.SQLTable.split(" ");
                                                                        viewNameSQL = viewNameSQL[3];
                                                                        viewNameSQL = viewNameSQL.split('(')[0];

                                                                        let rows = [];
                                                                        let rowsDelta = [];
                                                                        let lastCol = "";
                                                                        let prelastCol = null; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
                                                                        await Object.keys(cachedGroupBy).forEach(function (key, index) {
                                                                            if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                                                                                let crnRow = JSON.parse(key);
                                                                                lastCol = view.SQLTable.split(" ");
                                                                                prelastCol = lastCol[lastCol.length - 4];
                                                                                lastCol = lastCol[lastCol.length - 2];
                                                                                let gbVals = Object.values(cachedGroupBy);
                                                                                if (view.operation === "AVERAGE") {
                                                                                    crnRow[prelastCol] = gbVals[index]["sum"];
                                                                                    crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                                                                                } else {
                                                                                    crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                                                                                }
                                                                                rows.push(crnRow);
                                                                            }
                                                                        });

                                                                        await Object.keys(groupBySqlResult).forEach(function (key, index) {
                                                                            if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable') {
                                                                                let crnRow = JSON.parse(key);
                                                                                lastCol = view.SQLTable.split(" ");
                                                                                prelastCol = lastCol[lastCol.length - 4];
                                                                                lastCol = lastCol[lastCol.length - 2];
                                                                                let gbVals = Object.values(cachedGroupBy);
                                                                                if (view.operation === "AVERAGE") {
                                                                                    crnRow[prelastCol] = gbVals[index]["sum"];
                                                                                    crnRow[lastCol] = gbVals[index]["count"]; //BUG THERE ON AVERAGEEE
                                                                                } else {
                                                                                    crnRow[lastCol] = gbVals[index]; //BUG THERE ON AVERAGEEE
                                                                                }
                                                                                rowsDelta.push(crnRow);
                                                                            }
                                                                        });
                                                                        let mergeTimeStart = microtime.nowDouble();
                                                                        mergeGroupBys(rows, rowsDelta, view.SQLTable, viewNameSQL, view, lastCol, prelastCol, function (mergeResult, error) {
                                                                            let mergeTimeEnd = microtime.nowDouble();
                                                                            //SAVE ON CACHE BEFORE RETURN
                                                                            console.log("SAVE ON CACHE BEFORE RETURN");
                                                                            if (error) {
                                                                                gbRunning = false;
                                                                                return res.send(error);
                                                                            }
                                                                            mergeResult.operation = view.operation;
                                                                            mergeResult.field = view.aggregationField;
                                                                            mergeResult.gbCreateTable = view.SQLTable;
                                                                            let cacheSaveTimeStart = microtime.nowDouble();
                                                                            saveOnCache(mergeResult, view.operation, latestId - 1).on('error', (err) => {
                                                                                console.log('error:', err);
                                                                                gbRunning = false;
                                                                                return res.send(err);
                                                                            }).on('transactionHash', (err) => {
                                                                                console.log('transactionHash:', err);
                                                                            }).on('receipt', (receipt) => {
                                                                                let cacheSaveTimeEnd = microtime.nowDouble();
                                                                                delete mergeResult.gbCreateTable;
                                                                                if (sortedByEvictionCost.length >= config.maxCacheSize) {
                                                                                    deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
                                                                                        contract.methods.deleteGBsById(gbIdsToDelete).call(function (err, latestGBDeleted) {
                                                                                            let totalEnd = microtime.nowDouble();
                                                                                            if (!err) {
                                                                                                mergeResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                                mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                                mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                                mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                                mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                                mergeResult.allTotal = totalEnd - totalStart;
                                                                                                console.log("sql time = " + mergeResult.sqlTime);
                                                                                                console.log("bc time = " + mergeResult.bcTime);
                                                                                                console.log("cache save time = " + mergeResult.cacheSaveTime);
                                                                                                console.log("cache retrieve time = " + mergeResult.cacheRetrieveTime);
                                                                                                console.log("total time = " + mergeResult.totalTime);
                                                                                                console.log("all total time = " + mergeResult.allTotal);
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
                                                                                    mergeResult.bcTime = bcTimeEnd - bcTimeStart;
                                                                                    mergeResult.sqlTime = (mergeTimeEnd - mergeTimeStart) + (sqlTimeEnd - sqlTimeStart);
                                                                                    mergeResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                                                                    mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                                                                    mergeResult.totalTime = mergeResult.bcTime + mergeResult.sqlTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                                                                    mergeResult.allTotal = totalEnd - totalStart;
                                                                                    console.log("sql time = " + mergeResult.sqlTime);
                                                                                    console.log("bc time = " + mergeResult.bcTime);
                                                                                    console.log("cache save time = " + mergeResult.cacheSaveTime);
                                                                                    console.log("cache retrieve time = " + mergeResult.cacheRetrieveTime);
                                                                                    console.log("total time = " + mergeResult.totalTime);
                                                                                    console.log("all total time = " + mergeResult.allTotal);
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
                                        //  return res.send({allGbs: filteredGBs, mostEfficient: mostEfficient});
                                    }
                                });
                            } else {
                                console.log(err);
                                gbRunning = false;
                                return res.send(err);
                            }
                        });
                    } else {
                        //No group bys exist in cache, we are in the initial state
                        //this means we should proceed to new group by calculation from the begining
                        let bcTimeStart = microtime.nowDouble();
                        contract.methods.dataId().call(function (err, latestId) {
                            if (err) throw err;
                            getAllFacts(latestId).then(retval => {
                                let bcTimeEnd = microtime.nowDouble();
                                if (retval.length === 0) {
                                    gbRunning = false;
                                    return res.send(JSON.stringify({error: "No facts exist in blockchain"}));
                                }
                                let facts = helper.removeTimestamps(retval);
                                console.log("CALCULATING NEW GB FROM BEGGINING");
                                let sqlTimeStart = microtime.nowDouble();
                                calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                                    let sqlTimeEnd = microtime.nowDouble();
                                    if (error) {
                                        gbRunning = false;
                                        return res.send(JSON.stringify(error))
                                    }
                                    groupBySqlResult.gbCreateTable = view.SQLTable;
                                    groupBySqlResult.field = view.aggregationField;
                                    let cacheSaveTimeStart = microtime.nowDouble();
                                    saveOnCache(groupBySqlResult, view.operation, latestId - 1).on('error', (err) => {
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
                                        groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                                        groupBySqlResult.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime + groupBySqlResult.cacheSaveTime;
                                        groupBySqlResult.allTotal = totalEnd - totalStart;
                                        console.log("sql time = " + groupBySqlResult.sqlTime);
                                        console.log("bc time = " + groupBySqlResult.bcTime);
                                        console.log("cache save time = " + groupBySqlResult.cacheSaveTime);
                                        console.log("total time = " + groupBySqlResult.totalTime);
                                        console.log("all total time = " + groupBySqlResult.allTotal);
                                        io.emit('view_results', JSON.stringify(groupBySqlResult));
                                        gbRunning = false;
                                        return res.send(groupBySqlResult);
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
                console.log("cache enabled = FALSE");
                //cache not enabled, so just fetch everything everytime from blockchain and then make calculation in sql
                //just like the case that the cache is originally empty
                let bcTimeStart = microtime.nowDouble();
                contract.methods.dataId().call(function (err, latestId) {
                    if (err) throw err;
                    getAllFacts(latestId).then(retval => {
                        let bcTimeEnd = microtime.nowDouble();
                        if (retval.length === 0) {
                            gbRunning = false;
                            return res.send(JSON.stringify({error: "No facts exist in blockchain"}));
                        }
                        let facts = helper.removeTimestamps(retval);
                        console.log("CALCULATING NEW GB FROM BEGGINING");
                        let sqlTimeStart = microtime.nowDouble();
                        calculateNewGroupBy(facts, view.operation, view.gbFields, view.aggregationField, function (groupBySqlResult, error) {
                            let sqlTimeEnd = microtime.nowDouble();
                            let totalEnd = microtime.nowDouble();
                            if (error) {
                                gbRunning = false;
                                return res.send(JSON.stringify(error))
                            }
                            groupBySqlResult.gbCreateTable = view.SQLTable;
                            groupBySqlResult.field = view.aggregationField;
                            groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                            groupBySqlResult.bcTime = bcTimeEnd - bcTimeStart;
                            groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime;
                            groupBySqlResult.allTotal = totalEnd - totalStart;
                            console.log("sql time = " + groupBySqlResult.sqlTime);
                            console.log("bc time = " + groupBySqlResult.bcTime);
                            console.log("total time = " + groupBySqlResult.totalTime);
                            console.log("all total time = " + groupBySqlResult.allTotal);
                            io.emit('view_results', JSON.stringify(groupBySqlResult));
                            gbRunning = false;
                            return res.send(groupBySqlResult);
                        });
                    });
                });
            }
        } else {
            res.status(400);
            gbRunning = false;
            return res.send({status: 'ERROR', options: 'Contract not deployed'});
        }
    }
});

app.get('/getcount', function (req, res) {
    if (contract) {
        contract.methods.dataId().call(function (err, result) {
            if (!err) {
                res.send(result);
            } else {
                console.log(err);
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.post('/addFact', function (req, res) {
    if (contract) {
        let addFactPromise = contract.methods.addFact(JSON.stringify(req.body));
        addFactPromise.send(mainTransactionObject, (err, txHash) => {
            console.log('send:', err, txHash);
        }).on('error', (err) => {
            console.log('error:', err);
            res.send(err);
        }).on('transactionHash', (err) => {
            console.log('transactionHash:', err);
        }).on('receipt', (receipt) => {
            console.log('receipt:', receipt);
            res.send(receipt);
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});