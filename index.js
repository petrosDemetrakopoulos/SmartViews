const express = require('express');
const bodyParser = require('body-parser');
const solc = require('solc');
const fs = require('fs');
const delay = require('delay');
const groupBy = require('group-by');
const dataset = require('./dataset_1k');
let fact_tbl = require('./templates/fact_tbl');
const crypto = require('crypto');
let md5sum = crypto.createHash('md5');
const csv = require('fast-csv');
abiDecoder = require('abi-decoder');
const app = express();
const jsonParser = bodyParser.json();
const helper = require('./helper');
const transformations = require('./transformations');
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
const csvtojson = require('csvtojson');
const jsonSql = require('json-sql')({separatedValues: false});

const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider('http://localhost:8545'));
const redis = require('redis');
const client = redis.createClient(6379, '127.0.0.1');
client.on('connect', function () {
    console.log('Redis client connected');
});
client.on('error', function (err) {
    console.log('Something went wrong ' + err);
});

const mysql = require('mysql');
let createTable = '';
let tableName = '';
let connection = null;
let contractInstance = null;
let contractsDeployed = [];

web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let DataHandler = null;
let acc = null;
app.get('/', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        res.render('index', { 'templates': items });
    })
});

io.on('connection', function (socket) {
    console.log('a user connected');
});

app.get('/dashboard', function (req, res) {
    fs.readdir('./templates', function (err, items) {
        web3.eth.getBlockNumber().then(blockNum => {
            res.render('dashboard', { 'templates': items, 'blockNum': blockNum });
        });
    });
});

app.get('/benchmark', function (req, res) {
    // var stream = fs.createReadStream('../dataset.csv');
    csvtojson({ delimiter: '|' })
        .fromFile('../dataset.csv')
        .then((jsonObj)=>{
            console.log(jsonObj);
            let timeStart = microtime.nowDouble();
            let gbResult = groupBy(jsonObj,'Occupation');
            console.log(microtime.nowDouble() - timeStart + ' seconds');
            res.send(gbResult);
            /**
             * [
             * 	{a:'1', b:'2', c:'3'},
             * 	{a:'4', b:'5'. c:'6'}
             * ]
             */
        });
});

function calculateGBPython(data, gbField, aggregateField, operation, cb) {
    let spawn = require('child_process').spawn;
    let process = spawn('python', ['./gb.py', data, gbField, aggregateField, operation]);
    let result = '';
    try {
        process.stdout.on('data', function (data) {
            console.log(data.toString());
            result += data.toString();
        });
        process.on('exit', function () {
            return cb(result)
        });
    } catch(e) {
        process.stdout.on('error', function (err) {
            return cb(null,err);
        });
    }
}

app.get('/python', function (req, res) {
    let spawn = require('child_process').spawn;
    let process = spawn('python', ['./gb.py', jsonData]);

    process.stdout.on('data', function (data) {
        res.send(data.toString());
    });
    process.stdout.on('error', function (error) {
        console.log(error);
        res.send(error.toString());
    });

    process.stdout.on('close', (code, signal) => {
        console.log(`child process terminated due to receipt of signal ${signal} ${code}`);
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
    groupBys = helper.removeDuplicates(groupBys);
    groupBys.push(fact_tbl.groupBys.TOP.fields);
    console.log(groupBys);
    res.render('form',{'template':templ, 'name': fact_tbl.name, 'address': address, 'groupBys':groupBys});
});

http.listen(3000, () => {
    console.log(`Example app listening on http://localhost:3000`);
    let mysqlConfig = {};
    if(process.env.NODE_ENV === 'development'){
        mysqlConfig = {
            host: 'localhost',
            user: 'root',
            password: 'Xonelgataandrou1!',
            database: 'Ptychiaki'
        };
    } else if(process.env.NODE_ENV === 'lab'){
        mysqlConfig = {
            host: 'localhost',
            user: 'root',
            password: 'Iwanttobelive1',
            database: 'Ptychiaki'
        };
    }
    connection = mysql.createConnection(mysqlConfig);
    connection.connect(function (err) {
        if (err) {
            console.error('error connecting to mySQL: ' + err.stack);
            return;
        }
        console.log('mySQL connected');
    });
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

app.get('/readFromFile', function (req, res) {
    csv
        .fromPath('dataset.txt',{delimiter: '|'})
        .on('data', function (data) {
            console.log(data);
        })
        .on('end', function () {
            console.log('done');
            res.send('done');
        })
});

app.get('/deployContract/:fn', function (req, res) {
    web3.eth.getAccounts(function (err, accounts) {
        if (!err) {
            acc = accounts[1];
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

async function addManyFactsNew(facts, sliceSize) {
    console.log('length = ' + facts.length);
    const transactionObject = {
        from: acc,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
    let proms = [];
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
        // for (const slc of slices) {
        //     let crnProms = [];
        //     crnProms = slc.map(fct => {
        //         return JSON.stringify(fct);
        //     });
        //     allSlicesReady.push(crnProms);
        // }
    } else {
        allSlicesReady = facts.map(fact => {
            return [JSON.stringify(fact)];
        });
    }

    let i = 0;
    for(const slc of allSlicesReady){
        let transPromise = await contract.methods.addFacts(slc).send(transactionObject, (err, txHash) => {
        }).on('error', (err) => {
            console.log('error:', err);
        }).on('transactionHash', (hash) => {
            io.emit('progress', i/allSlicesReady.length);
        });
        i++;
    }

    // for (const fact of facts) {
    //     let strFact = JSON.stringify(fact);
    //     proms.push(strFact);
    //     console.log(strFact);
    // }
    // console.log("done loop");
    // console.log(proms.length);
    // let transPromise = await contract.methods.addFacts(proms).send(transactionObject, (err, txHash) => {
    // console.log(err);
    //     console.log(txHash);
    // }).on('error', (err) => {
    //     console.log('error:', err);
    // }).on('transactionHash', (hash) => {
    //     console.log("***");
    //     console.log(hash);
    //     console.log("***");
    // });
    return Promise.resolve(true);
}

async function addManyFacts(facts) {
    console.log('length = ' + facts.length);
    const transactionObject = {
        from: acc,
        gas: 1500000,
        gasPrice: '30000000000000'
    };
    let proms = [];
    let i = 0;
    for (const fact of facts) {
        let strFact = JSON.stringify(fact);
        let transPromise = await contract.methods.addFact(strFact).send(transactionObject, (err, txHash) => {
            //console.log('send:', err, txHash);
        }).on('error', (err) => {
            console.log('error:', err);
        }).on('transactionHash', (err) => {
                //console.log('transactionHash:', err);
                io.emit('progress', i/facts.length);
                console.log(i);
            });
            // .on('receipt', (receipt) => {
            //     // console.log('receipt:', receipt);
            //     io.emit('progress', i/facts.length);
            //     console.log(i);
            // }).
        i++;
    }
    // console.log('LOOP ENDED EXECUTING BATCH');
    // batch.execute();
    return Promise.resolve(true);
}

app.get('/load_dataset/:dt', function (req, res) {
    let dt = require('./' + req.params.dt);
    console.log("ENDPOINT HIT AGAIN");
    if (contract) {
        if (!running) {
            running = true;
            let startTime = microtime.nowDouble();
            addManyFactsNew(dt,200).then(retval => {
                let endTime = microtime.nowDouble();
                let timeDiff = endTime - startTime;
                console.log(retval);
                console.log('DONE');
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
    let fact_tbl = require('./templates/' + req.params.fn);
    createTable = fact_tbl.template.create_table;
    tableName = fact_tbl.template.table_name;
    let contrPayload = '';
    let firstLine = 'pragma solidity ^0.4.24;\npragma experimental ABIEncoderV2;\n';
    let secondLine = 'contract ' + fact_tbl.name + ' { \n';
    let thirdLine = '\tuint public dataId;\n';
    let fourthLine = '\tuint public groupId;\n\n';
    let fifthLine = '\tuint public lastCount;\n' +
        '\tuint public lastSUM;\n' +
        '\tuint public lastMin;\n' +
        '\tuint public lastMax;\n' +
        "\tuint public lastAverage;\n" +
        "\tbytes32 MIN_LITERAL = \"MIN\";\n" +
        "\tbytes32 MAX_LITERAL = \"MAX\";\n" +
        "\tbytes32 AVERAGE_LITERAL = \"AVERAGE\";\n" +
        "\tbytes32 COUNT_LITERAL = \"COUNT\";\n" +
        "\tbytes32 SUM_LITERAL = \"SUM\";\n";
    let constr = "\tconstructor() {\n" +
        "\t\tdataId = 0;\n" +
        "\t\tgroupId = 0;\n" +
        "\t\tlastCount = 0;\n" +
        "\t\tlastSUM = 0;\n" +
        "\t\tlastMin = 0;\n" +
        "\t\tlastMax = 0;\n" +
        "\t\tlastAverage = 0;\n" +
        "\t}\n";
    let properties = '';
    let struct = "\tstruct " + fact_tbl.struct_Name + "{ \n";
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        properties += "\t\t" + crnProp.data_type + " " + crnProp.key + ";\n";
    }
    let groupStruct = "\tstruct groupBy{ \n  \t\tstring hash;\n" + '  \t\tuint latestFact;\n' +
        "        uint timestamp;\n\t}\n";
    let groupMapping =  "\tmapping(uint => groupBy) public groupBys;\n\n";
    properties += "\t\tuint timestamp;\n";
    let closeStruct = "\t}\n";
    let mapping = "\tmapping(uint =>" + fact_tbl.struct_Name +") public facts;\n\n";
    let addParams = '';
    let addFact = "\tfunction addFact(";


    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            addParams += crnProp.data_type + ' ' + crnProp.key + ") ";
        } else {
            addParams += crnProp.data_type + ' ' + crnProp.key + ",";
        }
    }
    let retParams = 'public returns (';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            retParams += crnProp.data_type + ' ' + ", uint ID){\n";
        } else {
            retParams += crnProp.data_type + ' ' + ",";
        }
    }
    addFact = addFact + addParams + retParams;
    let setters = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        setters += "\t\tfacts[dataId]." + crnProp.key  + "= " +  crnProp.key + ";\n";
    }
    setters += "\t\tfacts[dataId].timestamp = now;\n \t\tdataId += 1;\n";
    let retStmt = "\t\treturn (";
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        retStmt += "facts[dataId-1]." + crnProp.key  + ",";
    }
    retStmt += "dataId -1);\n\t}\n\n";

    let getParams = '';
    let getFact = "\tfunction getFact(uint id) public constant returns (";
    let retVals = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParams += crnProp.data_type + " " + crnProp.key +", uint timestamp" + "){\n";
            retVals += "facts[id]." + crnProp.key + ", facts[id].timestamp" +  ");\n\t}\n\n";
        } else {
            getParams += crnProp.data_type + " " + crnProp.key + ",";
            retVals += "facts[id]." + crnProp.key + ",";
        }
    }
    let retFact = "\t\treturn (" + retVals ;

    let addGroupBy = "\tfunction addGroupBy(string hash, bytes32 category, uint latestFact) public returns(string groupAdded, uint groupID){\n" +
        "    \t\tgroupBys[groupId].hash = hash;\n" +
        "    \t\tgroupBys[groupId].timestamp = now;\n" +
        "    \t\tgroupBys[groupId].latestFact = latestFact;\n" +
        "\t\t\tif(category == COUNT_LITERAL){\n" +
        "\t\t\t\tlastCount  = groupID;\n" +
        "\t\t\t} else if(category == SUM_LITERAL){\n" +
        "\t\t\t\tlastSUM = groupID;\n" +
        "\t\t\t} else if(category == MIN_LITERAL){\n" +
        "\t\t\t\tlastMin = groupID;\n" +
        "\t\t\t} else if(category == MAX_LITERAL){\n" +
        "\t\t\t\tlastMax = groupID;\n" +
        "\t\t\t} else if(category == AVERAGE_LITERAL){\n" +
        "\t\t\t\tlastAverage = groupID;\n" +
        "\t\t\t}\n" +
        "    \t\tgroupId += 1;\n" +
        "    \t\treturn (groupBys[groupId-1].hash, groupId-1);\n" +
        "    \t}\n\n";

    let getGroupBy = "\tfunction getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp, uint latFact){\n" +
        "    \t\treturn(groupBys[idGroup].hash, groupBys[idGroup].timestamp, groupBys[idGroup].latestFact);\n" +
        "    \t}\n\n";

    let getLatestGroupBy = "function getLatestGroupBy(bytes32 operation) public constant returns(string latestGroupBy, uint ts, uint latFactInGb){\n" +
        "\t\tif(groupId > 0){\n" +
        "\t\t\tif(operation == COUNT_LITERAL){\n" +
        "\t\t\t\tif(lastCount >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastCount].hash, groupBys[lastCount].timestamp, groupBys[lastCount].latestFact);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == SUM_LITERAL){\n" +
        "\t\t\t\tif(lastSUM >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp, groupBys[lastCount].latestFact);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == MIN_LITERAL){\n" +
        "\t\t\t\tif(lastMin >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMin].hash, groupBys[lastMin].timestamp, groupBys[lastCount].latestFact);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == MAX_LITERAL){\n" +
        "\t\t\t\tif(lastMax >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMax].hash, groupBys[lastMax].timestamp, groupBys[lastCount].latestFact);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == AVERAGE_LITERAL){\n" +
        "\t\t\t\tif(lastAverage >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp, groupBys[lastCount].latestFact);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t}\n" +
        "\t\t}\n" +
        "\t\t\treturn (\"\",0,0);\n" +
        "\t}\n\n";

    let retValsLatest = '';
    let getParamsLatest = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsLatest += crnProp.data_type + ' ' + crnProp.key + '){\n';
            retValsLatest += 'facts[dataId-1].' + crnProp.key + ');\n\t';
        } else {
            getParamsLatest += crnProp.data_type + ' ' + crnProp.key + ',';
            retValsLatest += "facts[dataId-1]." + crnProp.key + ',';
        }
    }
    let retFactLatest = '\t\t\treturn (' + retValsLatest ;
    let emptyRetFactLatest = '';

    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            if (crnProp.data_type === 'string') {
                emptyRetFactLatest += "\"\"" + ");\n\t";
            } else {
                emptyRetFactLatest += "0" + ");\n\t";
            }
        } else {
            if (crnProp.data_type === 'string') {
                emptyRetFactLatest += "\"\"" + ', ';
            } else {
                emptyRetFactLatest += '0, ';
            }
        }
    }

    let getLatestFact = "\tfunction getLatestFact() public constant returns (" + getParamsLatest +
        "\t\tif(dataId > 0){\n" + retFactLatest +
        "\t} else {\n" +
        "\t\t\treturn (" + emptyRetFactLatest +
        "\t}\n" +
        "\t}\n\n";


    let getAllFacts = '\tfunction getAllFacts(uint id) public returns (';
    let getParamsAll = '';
    let retValsAll = '';
    let assignements = '';
    let retStmtAll = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsAll += crnProp.data_type + "[] " + crnProp.key +"s, uint[] timestamps" + "){\n";
            retValsAll += "\t\t" + crnProp.data_type + "[] memory " + crnProp.key + "ss = new " + crnProp.data_type + "[](id);\n";
            retValsAll += "\t\tuint[] memory timestampss = new uint[](id);\n";
            assignements += "\t\t\t" + crnProp.key + "ss[i] = fact." + crnProp.key+';\n';
            assignements += '\t\t\t' + 'timestampss[i] = fact.timestamp;\n';
            assignements += '\t\t}\n';
            retStmtAll += crnProp.key + 'ss,';
            retStmtAll += 'timestampss);\n'
        } else {
            getParamsAll += crnProp.data_type + '[] ' + crnProp.key + 's,';
            retValsAll += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](id);\n';
            assignements += '\t\t\t' +  crnProp.key + 'ss[i] = fact.' + crnProp.key+';\n';
            retStmtAll += crnProp.key + 'ss,';
        }
    }
    let loopLine = '\t\tfor(uint i =0; i < id; i++){\n';
    let firstLoopLine = '\t\t\t' + fact_tbl.struct_Name + ' storage fact = facts[i];\n';


    let getAllRet = '\t\treturn (';
    getAllRet += retStmtAll;
    loopLine += firstLoopLine + assignements + getAllRet + '\t}\n';

    getAllFacts = getAllFacts + getParamsAll + retValsAll + loopLine + '\n';

    let getFactFromTo = '\tfunction getFactsFromTo(uint from, uint to) public returns (';
    let getParamsFromTo = '';
    let retValsFromTo = '';
    let assignementsFromTo = '';
    let retStmtFromTo = '';
    let arrCounter = "\t\t\tuint j = 0;\n";
    let counterIncr = "\t\t\tj++;\n";

    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsFromTo += crnProp.data_type + "[] " + crnProp.key +"sFromTo, uint[] timestampsFromTo" + "){\n";
            retValsFromTo += "\t\t" + crnProp.data_type + "[] memory " + crnProp.key + "ss = new " + crnProp.data_type + "[](to - from);\n";
            retValsFromTo += "\t\tuint[] memory timestampss = new uint[](to - from);\n";
            assignementsFromTo += "\t\t\t" + crnProp.key + "ss[j] = fact." + crnProp.key+';\n';
            assignementsFromTo += '\t\t\t' + 'timestampss[j] = fact.timestamp;\n';
            assignementsFromTo += counterIncr;
            assignementsFromTo += '\t\t}\n';
            retStmtFromTo += crnProp.key + 'ss,';
            retStmtFromTo += 'timestampss);\n'
        } else {
            getParamsFromTo += crnProp.data_type + '[] ' + crnProp.key + 's,';
            retValsFromTo += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](to - from);\n';
            assignementsFromTo += '\t\t\t' +  crnProp.key + 'ss[j] = fact.' + crnProp.key+';\n';
            assignementsFromTo += counterIncr;
            retStmtFromTo += crnProp.key + 'ss,';
        }
    }


    let loopLineFromTo = '\t\tfor(uint i = from; i < to; i++){\n';
    let firstLoopLineFromTo = '\t\t\t' + fact_tbl.struct_Name + ' storage fact = facts[j];\n';


    let getRetFromTo = '\t\treturn (';
    getRetFromTo += retStmtFromTo;
    loopLineFromTo += firstLoopLineFromTo + assignementsFromTo + getRetFromTo + '\t}\n';

    getFactFromTo = getFactFromTo + getParamsFromTo + retValsFromTo + arrCounter + loopLineFromTo;
    let addManyFacts = "function addFacts(string[] payloadsss) public returns (string, uint IDMany){\n" +
        "\t\tfor(uint i =0; i < payloadsss.length; i++){\n" +
        "\t\t\tfacts[dataId].payload= payloadsss[i];\n" +
        "\t\t\tfacts[dataId].timestamp = now;\n" +
        "\t\t\tdataId += 1;\n" +
        "\t\t}\n" +
        "\t\treturn (facts[dataId-1].payload,dataId -1);\n" +
        "\t}";
    contrPayload = firstLine + secondLine + thirdLine + fourthLine + fifthLine +  constr + struct + properties + closeStruct + groupStruct + groupMapping +  mapping + addFact + setters + retStmt + getFact + getParams + retFact + addGroupBy + getGroupBy + getLatestGroupBy + getAllFacts + getFactFromTo + addManyFacts +  '\n}';
    fs.writeFile('contracts/' + fact_tbl.name + '.sol', contrPayload, function (err) {
        if (err) {
            res.send({ msg: 'error' });
            return console.log(err);
        }
        console.log('The file was saved!');
        let templ = {};
        if ('template' in fact_tbl) {
            templ = fact_tbl['template'];
        } else {
            templ = fact_tbl;
        }
        res.send({ msg: 'OK', 'filename':fact_tbl.name + '.sol', 'template': templ });
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
                console.log('ERRRRRR');
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
});

app.get('/allFacts', function (req, res) {
    getAllFactsHeavy(50).then(retval => {
        res.send(retval);
    }).catch(error => {
        console.log(error);
    })
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
            console.log('ERRRRRR');
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
                if ('payload' in result2) {
                    let crnLn = JSON.parse(result2['payload']);
                    crnLn.timestamp = result2['timestamp'];
                    allFacts.push(crnLn);
                }
            } else {
                console.log(err);
                console.log('ERRRRRR');
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
                console.log('ERRRRRR');
            }
        });
    return allFacts;
}

app.get('/getFactsFromTo/:from/:to', function (req,res) {
    let timeStart = microtime.nowDouble();
   getFactsFromTo(parseInt(req.params.from), parseInt(req.params.to)).then(retval => {
       let timeFinish = microtime.nowDouble() - timeStart;
           console.log(retval);
           retval.push({time: timeFinish});
           res.send(retval);
   }).catch(err =>{
       res.send(err);
   });
});

app.get('/getallfacts', function (req, res) {
    if (contract) {
        contract.methods.dataId().call(function (err, result) {
            console.log('********');
            console.log(result);
            console.log('*****');
            if (!err) {
                // async loop waiting to get all the facts separately
                let timeStart = microtime.nowDouble();
                getAllFactsHeavy(result).then(retval => {
                    let timeFinish = microtime.nowDouble() - timeStart;
                    console.log('####');
                    console.log('Get all facts time: ' + timeFinish + ' s');
                    console.log('####');
                    retval.push({time: timeFinish});
                    //retval.timeDone = microtime.nowDouble() - timeStart;
                    res.send(retval);
                }).catch(error => {
                    console.log(error);
                });
            } else {
                console.log(err);
                console.log('ERRRRRR');
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
                console.log('ERRRRRR');
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: 'ERROR',options: 'Contract not deployed' });
    }
});

app.post('/addFacts', function (req, res) {
    if (contract) {
        if (req.body.products.length === req.body.quantities.length === req.body.customers.length) {
            contract.methods.addFacts(req.body.products, req.body.quantities, req.body.customers).call(function (err, result) {
                if (!err) {
                    res.send(result)
                } else {
                    console.log(err);
                    console.log('ERRRRRR');
                    res.send(err);
                }
            })
        } else {
            res.status(400);
            res.send({status: 'ERROR',options: 'Arrays must have the same dimension' });
        }
    } else {
        res.status(400);
        res.send({status: 'ERROR',options: 'Contract not deployed' });
    }
});

app.get('/groupby/:field/:operation/:aggregateField', function (req, res) {
    // LOGIC: IF latestGroupByTS >= latestFactTS RETURN LATEST GROUPBY FROM REDIS
    //      ELSE CALCULATE GROUBY FOR THE DELTAS (AKA THE ROWS ADDED AFTER THE LATEST GROUPBY) AND APPEND TO THE ALREADY SAVED IN REDIS
    console.log("gb hit again");
    if(!gbRunning) {
        running = true;
        let gbFields = [];
        if (req.params.field.indexOf('|') > -1) {
            // more than 1 group by fields
            gbFields = req.params.field.split('|');
        } else {
            gbFields.push(req.params.field);
        }
        console.log(gbFields);
        let python = false;
        if (contract) {
            let timeStart = 0;
            contract.methods.dataId().call(function (err, latestId) {
                contract.methods.getLatestGroupBy(Web3.utils.fromAscii(req.params.operation)).call(function (err, latestGroupBy) {
                    console.log('LATEST GB IS: ');
                    console.log(latestGroupBy);
                    if (latestGroupBy.ts > 0) {
                        contract.methods.getFact(latestId - 1).call(function (err, latestFact) {
                            if (latestGroupBy.ts >= latestFact.timestamp) {
                                // check what is the latest groupBy
                                // if latest groupby contains all fields for the new groupby requested
                                // -> incrementaly calculate the groupby requested by summing the one in redis cache
                                let timeCacheStart = microtime.nowDouble();
                                client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                    if (error) {
                                        console.log(error);
                                        gbRunning = false;
                                        io.emit('gb_results', error);
                                        res.send(error);
                                    } else {
                                        let timeCacheFinish = microtime.nowDouble();
                                        let timeCache = timeCacheFinish - timeFetchStart;
                                        cachedGroupBy = JSON.parse(cachedGroupBy);
                                        console.log('**');
                                        console.log(cachedGroupBy);
                                        console.log('**');
                                        let containsAllFields = true;
                                        for (let i = 0; i < gbFields.length; i++) {
                                            if (!cachedGroupBy.groupByFields.includes(gbFields[i])) {
                                                containsAllFields = false
                                            }
                                        }
                                        if (containsAllFields && cachedGroupBy.groupByFields.length !== gbFields.length) { //it is a different groupby thna the stored
                                            if (cachedGroupBy.field === req.params.aggregateField &&
                                                req.params.operation === cachedGroupBy.operation) {
                                                let respObj = transformations.calculateReducedGB(req.params.operation, req.params.aggregateField, cachedGroupBy, gbFields);
                                                io.emit('gb_results', JSON.stringify(respObj));
                                                res.send(JSON.stringify(respObj));
                                            }
                                        } else {
                                            console.log('getting it from redis');
                                            timeStart = microtime.nowDouble();
                                            client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                                if (error) {
                                                    console.log(error);
                                                    io.emit('gb_results', error);
                                                    res.send(error);
                                                } else {
                                                    console.log('GET result ->' + cachedGroupBy);
                                                    let timeFinish = microtime.nowDouble();
                                                    cachedGroupBy = JSON.parse(cachedGroupBy);
                                                    cachedGroupBy.cacheTime = timeCache;
                                                    cachedGroupBy.executionTime = timeFinish - timeStart;
                                                    gbRunning = false;
                                                    io.emit('gb_results', JSON.stringify(cachedGroupBy));
                                                    res.send(JSON.stringify(cachedGroupBy));
                                                }
                                            });
                                        }
                                    }
                                });

                            } else {
                                // CALCULATE GROUPBY FOR DELTAS (fact.timestamp > latestGroupBy timestamp)   AND THEN APPEND TO REDIS
                                //  getFactsFromTo(latestGroupBy.latFactInGb, latestId)
                                //  getAllFacts(latestId).then(retval => {
                                let timeFetchStart = microtime.nowDouble();
                                getFactsFromTo(latestGroupBy.latFactInGb, latestId).then(retval => { // getting just the deltas from the blockchain
                                    let timeFetchEnd = microtime.nowDouble();
                                    // get (fact.timestamp > latestGroupBy timestamp)
                                    let deltas = [];
                                    for (let i = 0; i < retval.length; i++) {
                                        let crnFact = retval[i];
                                        //    if (crnFact.timestamp > latestGroupBy.ts) {
                                        deltas.push(crnFact);
                                        //  }
                                    }
                                    timeStart = microtime.nowDouble();

                                    // python
                                    if (python) {
                                        calculateGBPython(deltas.toString(), req.params.field, req.params.aggregateField, req.params.operation).then(result => {
                                            result = JSON.parse(result);
                                            console.log('$$$$$');
                                            console.log(result);
                                            console.log('$$$$$');
                                            result = JSON.parse(result);
                                            result.operation = req.params.operation;
                                            result.field = req.params.aggregateField;
                                            result.time = microtime.nowDouble() - timeStart;
                                            let deltaGroupBy = result;
                                            client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                                if (error) {
                                                    console.log(error);
                                                    gbRunning = false;
                                                    res.send(error);
                                                } else {
                                                    console.log('GET result ->' + cachedGroupBy);

                                                    // IF COUNT / SUM -> ADD
                                                    // ELIF MIN -> NEW_MIN = MIN OF MINS

                                                    let ObCachedGB = JSON.parse(cachedGroupBy);
                                                    let updatedGB = {};
                                                    if (ObCachedGB['operation'] === 'SUM') {
                                                        updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                    } else if (ObCachedGB['operation'] === 'COUNT') {
                                                        updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                    } else if (ObCachedGB['operation'] === 'MAX') {
                                                        updatedGB = helper.maxObjects(ObCachedGB, deltaGroupBy)
                                                    } else if (ObCachedGB['operation'] === 'MIN') {
                                                        updatedGB = helper.minObjects(ObCachedGB, deltaGroupBy)
                                                    } else { // AVERAGE
                                                        updatedGB = helper.averageObjects(ObCachedGB, deltaGroupBy)
                                                    }
                                                    let timeFinish = microtime.nowDouble();
                                                    client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                                    updatedGB.executionTime = timeFinish - timeStart;
                                                    updatedGB.blockchainFetchTime = timeFetchEnd - timeFetchStart;
                                                    gbRunning = false;
                                                    io.emit('gb_results', JSON.stringify(updatedGB));
                                                    res.send(JSON.stringify(updatedGB));
                                                }
                                            });
                                        });
                                    } else {

                                        // calculate groupby for deltas in SQL
                                        let SQLCalculationTimeStart = microtime.nowDouble();
                                        connection.query(createTable, function (error, results, fields) {
                                            if (error) throw error;
                                            for (let i = 0; i < deltas.length; i++) {
                                                delete deltas[i].timestamp;
                                            }

                                            let sql = jsonSql.build({
                                                type: 'insert',
                                                table: tableName,
                                                values: deltas
                                            });

                                            let editedQuery = sql.query.replace(/"/g, '');
                                            editedQuery = editedQuery.replace(/''/g, 'null');
                                            console.log(editedQuery);
                                            connection.query(editedQuery, function (error, results2, fields) {
                                                let gbQuery = null;
                                                if (req.params.operation === 'AVERAGE') {
                                                    gbQuery = jsonSql.build({
                                                        type: 'select',
                                                        table: tableName,
                                                        group: gbFields,
                                                        fields: [gbFields,
                                                            {
                                                                func: {
                                                                    name: 'SUM',
                                                                    args: [
                                                                        {field: req.params.aggregateField}
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                func: {
                                                                    name: 'COUNT',
                                                                    args: [
                                                                        {field: req.params.aggregateField}
                                                                    ]
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
                                                                    name: req.params.operation,
                                                                    args: [
                                                                        {field: req.params.aggregateField}
                                                                    ]
                                                                }
                                                            }]
                                                    });
                                                }
                                                let editedGB = gbQuery.query.replace(/"/g, '');
                                                connection.query(editedGB, function (error, results3, fields) {
                                                    connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                                                        let SQLCalculationTimeEnd = microtime.nowDouble();
                                                        if (!err) {
                                                            let deltaGroupBy = transformations.transformGBFromSQL(results3, req.params.operation, req.params.aggregateField, gbFields);
                                                            let cacheTimeStart = microtime.nowDouble();
                                                            client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                                                let cacheTimeEnd = microtime.nowDouble();
                                                                if (error) {
                                                                    console.log(error);
                                                                    io.emit('gb_results', error);
                                                                    res.send(error);
                                                                } else {
                                                                    console.log('GET result ->' + cachedGroupBy);
                                                                    // IF COUNT / SUM -> ADD
                                                                    // ELIF MIN -> NEW_MIN = MIN OF MINS
                                                                    cachedGroupBy = JSON.parse(cachedGroupBy);
                                                                    console.log('**');
                                                                    console.log(cachedGroupBy);
                                                                    console.log('**');
                                                                    let containsAllFields = true;
                                                                    let ObCachedGB = {};
                                                                    for (let i = 0; i < gbFields.length; i++) {
                                                                        if (!cachedGroupBy.groupByFields.includes(gbFields[i])) {
                                                                            containsAllFields = false
                                                                        }
                                                                    }
                                                                    if (containsAllFields && cachedGroupBy.groupByFields.length !== gbFields.length) { // it is a different groupby than the stored
                                                                        if (cachedGroupBy.field === req.params.aggregateField &&
                                                                            req.params.operation === cachedGroupBy.operation) {
                                                                            ObCachedGB = transformations.calculateReducedGB(req.params.operation, req.params.aggregateField, cachedGroupBy, gbFields);
                                                                        }
                                                                    } else {
                                                                        //ObCachedGB = JSON.parse(cachedGroupBy);
                                                                        ObCachedGB = cachedGroupBy;
                                                                    }

                                                                    let updatedGB = {};
                                                                    if (ObCachedGB['operation'] === 'SUM') {
                                                                        updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                                    } else if (ObCachedGB['operation'] === 'COUNT') {
                                                                        updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                                    } else if (ObCachedGB['operation'] === 'MAX') {
                                                                        updatedGB = helper.maxObjects(ObCachedGB, deltaGroupBy)
                                                                    } else if (ObCachedGB['operation'] === 'MIN') {
                                                                        updatedGB = helper.minObjects(ObCachedGB, deltaGroupBy)
                                                                    } else { // AVERAGE
                                                                        updatedGB = helper.averageObjects(ObCachedGB, deltaGroupBy)
                                                                    }
                                                                    let timeFinish = microtime.nowDouble();
                                                                    client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                                                    updatedGB.executionTime = timeFinish - timeStart;
                                                                    updatedGB.sqlCalculationTime = SQLCalculationTimeEnd - SQLCalculationTimeStart;
                                                                    updatedGB.cacheTime = cacheTimeEnd - cacheTimeStart;
                                                                    updatedGB.blockchainFetchTime = timeFetchEnd - timeFetchStart;
                                                                    gbRunning = false;
                                                                    io.emit('gb_results', JSON.stringify(updatedGB));
                                                                    res.send(JSON.stringify(updatedGB));
                                                                }
                                                            });
                                                        } else {
                                                            io.emit('gb_results', 'error');
                                                            res.send('error');
                                                        }
                                                    });
                                                });
                                            });
                                        });

                                        // let deltaGroupBy = groupBy(deltas, req.params.field);
                                        // deltaGroupBy = transformGB(deltaGroupBy, req.params.operation, req.params.aggregateField);
                                        // client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                        //     if (error) {
                                        //         console.log(error);
                                        //         res.send(error);
                                        //     } else {
                                        //         console.log('GET result ->' + cachedGroupBy);
                                        //
                                        //         //IF COUNT / SUM -> ADD
                                        //         //ELIF MIN -> NEW_MIN = MIN OF MINS
                                        //
                                        //         let ObCachedGB = JSON.parse(cachedGroupBy);
                                        //         let updatedGB = {};
                                        //         if (ObCachedGB['operation'] === 'SUM') {
                                        //             updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                        //         } else if (ObCachedGB['operation'] === 'COUNT') {
                                        //             updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                        //         } else if (ObCachedGB['operation'] === 'MAX') {
                                        //             updatedGB = maxObjects(ObCachedGB,deltaGroupBy)
                                        //         } else if (ObCachedGB['operation'] === 'MIN') {
                                        //             updatedGB = minObjects(ObCachedGB,deltaGroupBy)
                                        //         } else { //AVERAGE
                                        //             updatedGB = averageObjects(ObCachedGB,deltaGroupBy)
                                        //         }
                                        //         let timeFinish = microtime.nowDouble();
                                        //         client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                        //         updatedGB.executionTime = timeFinish - timeStart;
                                        //         res.send(JSON.stringify(updatedGB));
                                        //     }
                                        // });
                                    }

                                    //      console.log('DELTAS GB---->');
                                    //      console.log(deltaGroupBy);
                                    //      console.log('DELTAS GB---->');
                                    //      console.log(latestGroupBy);
                                }).catch(error => {
                                    console.log(error);
                                });
                            }
                        }).catch(error => {
                            console.log(error);
                        });
                    } else {
                        // NO GROUP BY, SHOULD CALCULATE IT FROM THE BEGGINING
                        let timeFetchStart = microtime.nowDouble();
                        getAllFacts(latestId).then(retval => {
                            let timeFetchEnd = microtime.nowDouble();
                            timeStart = microtime.nowDouble();
                            let groupByResult;
                            let timeFinish = 0;
                            const transactionObject = {
                                from: acc,
                                gas: 15000000,
                                gasPrice: '30000000000000'
                            };
                            if (python) {
                                calculateGBPython(retval, req.params.field, req.params.aggregateField, req.params.operation, function (results, err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    timeFinish = microtime.nowDouble();
                                    console.log('$$$$$');
                                    console.log(results);
                                    console.log('$$$$$');
                                    results = JSON.parse(results);
                                    results.operation = req.params.operation;
                                    results.field = req.params.aggregateField;
                                    results.time = timeFinish - timeStart;
                                    groupByResult = results;
                                    groupByResult = JSON.stringify(groupByResult);
                                    md5sum = crypto.createHash('md5');
                                    md5sum.update(groupByResult);
                                    let hash = md5sum.digest('hex');
                                    console.log(hash);
                                    client.set(hash, groupByResult, redis.print);
                                    contract.methods.addGroupBy(hash, Web3.utils.fromAscii(req.params.operation), latestId).send(transactionObject, (err, txHash) => {
                                        console.log('send:', err, txHash);
                                    }).on('error', (err) => {
                                        console.log('error:', err);
                                        io.emit('gb_results', err);
                                        res.send(err);
                                    }).on('transactionHash', (err) => {
                                        console.log('transactionHash:', err);
                                    }).on('receipt', (receipt) => {
                                        console.log('receipt:', receipt);
                                        groupByResult = JSON.parse(groupByResult);
                                        groupByResult.receipt = receipt;
                                        io.emit('gb_results', JSON.stringify(groupByResult));
                                        res.send(JSON.stringify(groupByResult));
                                    })
                                });
                            } else {
                                let SQLCalculationTimeStart = microtime.nowDouble();
                                connection.query(createTable, function (error, results, fields) {
                                    if (error) throw error;
                                    for (let i = 0; i < retval.length; i++) {
                                        delete retval[i].timestamp;
                                    }

                                    let sql = jsonSql.build({
                                        type: 'insert',
                                        table: tableName,
                                        values: retval
                                    });

                                    let editedQuery = sql.query.replace(/"/g, '');
                                    editedQuery = editedQuery.replace(/''/g, 'null');
                                    console.log(editedQuery);
                                    connection.query(editedQuery, function (error, results2, fields) {
                                        let gbQuery = null;
                                        if (req.params.operation === 'AVERAGE') {
                                            gbQuery = jsonSql.build({
                                                type: 'select',
                                                table: tableName,
                                                group: gbFields,
                                                fields: [gbFields,
                                                    {
                                                        func: {
                                                            name: 'SUM', args: [{field: req.params.aggregateField}]
                                                        }
                                                    },
                                                    {
                                                        func: {
                                                            name: 'COUNT', args: [{field: req.params.aggregateField}]
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
                                                            name: req.params.operation,
                                                            args: [{field: req.params.aggregateField}]
                                                        }
                                                    }]
                                            });
                                        }
                                        let editedGB = gbQuery.query.replace(/"/g, '');
                                        connection.query(editedGB, function (error, results3, fields) {
                                            connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                                                let SQLCalculationTimeEnd = microtime.nowDouble();
                                                if (!err) {
                                                    let groupBySqlResult = transformations.transformGBFromSQL(results3, req.params.operation, req.params.aggregateField, gbFields);
                                                    let timeFinish = microtime.nowDouble();
                                                    md5sum = crypto.createHash('md5');
                                                    md5sum.update(JSON.stringify(groupBySqlResult));
                                                    let hash = md5sum.digest('hex');
                                                    console.log(hash);
                                                    console.log('**');
                                                    console.log(JSON.stringify(groupBySqlResult));
                                                    console.log('**');
                                                    client.set(hash, JSON.stringify(groupBySqlResult), redis.print);
                                                    contract.methods.addGroupBy(hash, Web3.utils.fromAscii(req.params.operation), latestId).send(transactionObject, (err, txHash) => {
                                                        console.log('send:', err, txHash);
                                                    }).on('error', (err) => {
                                                        console.log('error:', err);
                                                        res.send(err);
                                                    }).on('transactionHash', (err) => {
                                                        console.log('transactionHash:', err);
                                                    }).on('receipt', (receipt) => {
                                                        console.log('receipt:', receipt);
                                                        let execT = timeFinish - timeStart;
                                                        groupBySqlResult.executionTime = execT;
                                                        groupBySqlResult.blockchainFetchTime = timeFetchEnd - timeFetchStart;
                                                        groupBySqlResult.sqlCalculationTime = SQLCalculationTimeEnd - SQLCalculationTimeStart;
                                                        gbRunning = false;
                                                        io.emit('gb_results', JSON.stringify(groupBySqlResult));
                                                        res.send(JSON.stringify(groupBySqlResult));
                                                    })
                                                } else {
                                                    gbRunning = false;
                                                    io.emit('gb_results', 'error');
                                                    res.send('error');
                                                }
                                            });
                                        });
                                    });
                                });
                            }
                        }).catch(error => {
                            console.log(error);
                        });
                    }
                }).catch(error => {
                    console.log(error);
                });
            });
        } else {
            gbRunning = false;
            res.status(400);
            io.emit('gb_results', JSON.stringify({status: 'ERROR', options: 'Contract not deployed'}));
            res.send({status: 'ERROR', options: 'Contract not deployed'});
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
                console.log('ERRRRRR');
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
        const transactionObject = {
            from: acc,
            gas: 1500000,
            gasPrice: '30000000000000'
        };
        console.log(req.body);
        let vals = req.body.values;
        for (let i = 0; i < req.body.values.length; i++) {
            let crnVal = req.body.values[i];
            if (crnVal.type === 'bytes32') {
                req.body.values[i].value = web3.utils.fromAscii(req.body.values[i].value);
            }
        }
        let valsLength = vals.length;
        let addFactPromise;
        if (valsLength === 1) {
            addFactPromise = contract.methods.addFact(vals[0].value);
        } else if (valsLength === 2) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value);
        } else if (valsLength === 3) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value);
        } else if (valsLength === 4) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value);
        } else if (valsLength === 5) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value);
        } else if (valsLength === 6) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value);
        } else if (valsLength === 7) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value);
        } else if (valsLength === 8) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value);
        } else if (valsLength === 9) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value, vals[8].value);
        } else if (valsLength === 10) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value, vals[8].value, vals[9].value);
        } else if (valsLength === 52) {
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value, vals[8].value, vals[9].value,
                vals[10].value, vals[11].value, vals[12].value, vals[13].value, vals[14].value, vals[15].value, vals[16].value, vals[17].value, vals[18].value, vals[19].value,
                vals[20].value, vals[21].value, vals[22].value, vals[23].value, vals[24].value, vals[25].value, vals[26].value, vals[27].value, vals[28].value, vals[29].value,
                vals[30].value, vals[31].value, vals[32].value, vals[33].value, vals[34].value, vals[35].value, vals[36].value, vals[37].value, vals[38].value, vals[39].value,
                vals[40].value, vals[41].value, vals[42].value, vals[43].value, vals[44].value, vals[45].value, vals[46].value, vals[47].value, vals[48].value, vals[49].value,
                vals[50].value, vals[51].value);
        } else {
            res.status(400);
            res.send({ status: 'ERROR', options: 'Contract not supporting more than 10 fields' });
        }
        addFactPromise.send(transactionObject, (err, txHash) => {
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