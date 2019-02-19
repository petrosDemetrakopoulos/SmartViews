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
const csv = require("fast-csv");
abiDecoder = require('abi-decoder');
const app = express();
const jsonParser = bodyParser.json();
const helper = require('./helper');
app.use(jsonParser);
let running = false;
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');
app.use(express.static('public'));
const microtime = require('microtime');
let http = require('http').Server(app);
let io = require('socket.io')(http);
const csvtojson = require("csvtojson");
const jsonSql = require('json-sql')({separatedValues: false});

const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));
const redis = require('redis');
const client = redis.createClient(6379,"127.0.0.1");
client.on('connect', function(){
    console.log('Redis client connected');
});

const mysql      = require('mysql');
let connection = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : 'Xonelgataandrou1!',
    database : 'Ptychiaki'
});
let createTable = '';
let tableName = '';
connection.connect(function(err) {
    if (err) {
        console.error('error connecting to mySQL: ' + err.stack);
        return;
    }
    console.log('mySQL connected');
});

client.on('error', function (err) {
    console.log('Something went wrong ' + err);
});
let contractInstance = null;
let contractsDeployed = [];


web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let DataHandler = null;
let acc = null;
app.get('/', function (req,res) {
    fs.readdir('./templates', function(err, items) {
        res.render("index",{"templates":items});
    });
});

io.on('connection', function(socket){
    console.log('a user connected');
});

app.get('/dashboard', function(req, res) {
    fs.readdir('./templates', function(err, items) {
        web3.eth.getBlockNumber().then(blockNum => {
            res.render("dashboard",{"templates":items, "blockNum": blockNum});
        });
    });
});

app.get('/benchmark', function(req,res) {
    // var stream = fs.createReadStream("../dataset.csv");
    csvtojson({delimiter:"|"})
        .fromFile("../dataset.csv")
        .then((jsonObj)=>{
            console.log(jsonObj);
            let timeStart = microtime.nowDouble();
            let gbResult = groupBy(jsonObj,"Occupation");
            console.log(microtime.nowDouble() - timeStart + " seconds");
            res.send(gbResult);
            /**
             * [
             * 	{a:"1", b:"2", c:"3"},
             * 	{a:"4", b:"5". c:"6"}
             * ]
             */
        });
});

function calculateGBPython(data, gbField, aggregateField, operation, cb){
    let spawn = require("child_process").spawn;
    let process = spawn('python', ["./gb.py", data, gbField, aggregateField, operation]);
    let result = '';
    try {
        process.stdout.on('data', function(data) {
            console.log(data.toString());
            result += data.toString();
        });
        process.on('exit', function() {
            return cb(result)
        });
    } catch(e){
        process.stdout.on('error', function(err) {
            return cb(null,err);
        });
    }
}

app.get('/python', function (req,res) {
    let spawn = require("child_process").spawn;
    let process = spawn('python', ["./gb.py", jsonData]);

    process.stdout.on('data', function(data) {
        res.send(data.toString());
    });
    process.stdout.on('error', function(error) {
        console.log(error);
        res.send(error.toString());
    });

    process.stdout.on('close', (code, signal) => {
        console.log(
            `child process terminated due to receipt of signal ${signal} ${code}`);
    });

});

app.get('/form/:contract', function(req, res) {
    let fact_tbl = require('./templates/' + req.params.contract);
    let templ = {};
    if('template' in fact_tbl){
        templ = fact_tbl['template'];
    } else {
        templ = fact_tbl;
    }
    let address = "0";
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
    res.render("form",{"template":templ, "name": fact_tbl.name, "address": address, "groupBys":groupBys});
});

http.listen(3000, () => console.log(`Example app listening on http://localhost:3000`));

async function deploy(account, contractPath){
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

app.get('/readFromFile', function(req, res) {
    csv
        .fromPath("dataset.txt",{delimiter: "|"})
        .on("data", function(data){
            console.log(data);
        })
        .on("end", function(){
            console.log("done");
            res.send("done");
        });
});

app.get('/deployContract/:fn', function(req, res) {
    web3.eth.getAccounts(function(err,accounts) {
        if (!err) {
            acc = accounts[1];
            console.log(req.params.fn);
            deploy(accounts[0], './contracts/' + req.params.fn)
                .then(options => {
                    console.log('Success');
                    res.send({status:"OK", options: options});
                })
                .catch(err => {
                    console.log('error on deploy ' + err);
                    res.status(400);
                    res.send({status:"ERROR", options: "Deployment failed"});
                });
        }
    });
});

async function addManyFacts(facts){
    console.log("length = " + facts.length);
    const transactionObject = {
        from: acc,
        gas: 1500000,
        gasPrice: '30000000000000'
    };
    let proms = [];
    let i = 0;
    for (const fact of facts){
        let strFact = JSON.stringify(fact);
        let transPromise = await contract.methods.addFact(strFact).send(transactionObject, (err, txHash) => {
            //console.log('send:', err, txHash);
        }).on('error', (err) => {
            console.log('error:', err);
        })
            .on('transactionHash', (err) => {
                //console.log('transactionHash:', err);
            })
            .on('receipt', (receipt) => {
                // console.log('receipt:', receipt);
                io.emit('progress', i/facts.length);
                console.log(i);
            });
        i++;
    }
    // console.log("LOOP ENDED EXECUTING BATCH");
    // batch.execute();
    return Promise.resolve(true);
}

app.get('/load_dataset/:dt', function(req, res) {
    let dt = require("./" + req.params.dt);
    if(contract) {
        if(!running) {
            running = true;
            addManyFacts(dt).then(retval => {
                console.log(retval);
                console.log("DONE");
                running = false;
                res.send("DONE");
            }).catch(error => {
                console.log(error);
            });
        }
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.get('/new_contract/:fn', function(req, res) {
    let fact_tbl = require('./templates/' + req.params.fn);
    createTable = fact_tbl.template.create_table;
    tableName = fact_tbl.template.table_name;
    let contrPayload = "";
    let firstLine = "pragma solidity ^0.4.24;\npragma experimental ABIEncoderV2;\n";
    let secondLine = "contract " + fact_tbl.name + " { \n";
    let thirdLine = "\tuint public dataId;\n";
    let fourthLine = "\tuint public groupId;\n\n";
    let fifthLine = "\tuint public lastCount;\n" +
        "\tuint public lastSUM;\n" +
        "\tuint public lastMin;\n" +
        "\tuint public lastMax;\n" +
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
    let properties = "";
    let struct = "\tstruct " + fact_tbl.struct_Name + "{ \n";
    for(let i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        properties += "\t\t" + crnProp.data_type + " " + crnProp.key + ";\n";
    }
    let groupStruct = "\tstruct groupBy{ \n  \t\tstring hash;\n" +
        "        uint timestamp;\n\t}\n";
    let groupMapping =  "\tmapping(uint => groupBy) public groupBys;\n\n";
    properties += "\t\tuint timestamp;\n";
    let closeStruct = "\t}\n";
    let mapping = "\tmapping(uint =>" + fact_tbl.struct_Name +") public facts;\n\n";
    let addParams = "";
    let addFact = "\tfunction addFact(";


    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            addParams += crnProp.data_type + " " + crnProp.key + ") ";
        } else {
            addParams += crnProp.data_type + " " + crnProp.key + ",";
        }
    }
    let retParams = "public returns (";
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            retParams += crnProp.data_type + " " + ", uint ID){\n";
        } else {
            retParams += crnProp.data_type + " " + ",";
        }
    }
    addFact = addFact + addParams + retParams;
    let setters = "";
    for (let i =0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        setters += "\t\tfacts[dataId]." + crnProp.key  + "= " +  crnProp.key + ";\n";
    }
    setters += "\t\tfacts[dataId].timestamp = now;\n \t\tdataId += 1;\n";
    let retStmt = "\t\treturn (";
    for (let i =0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        retStmt += "facts[dataId-1]." + crnProp.key  + ",";
    }
    retStmt += "dataId -1);\n\t}\n\n";

    let getParams = "";
    let getFact = "\tfunction getFact(uint id) public constant returns (";
    let retVals = "";
    for (let i =0; i < fact_tbl.properties.length; i++) {
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

    let addGroupBy = "\tfunction addGroupBy(string hash, bytes32 category) public returns(string groupAdded, uint groupID){\n" +
        "    \t\tgroupBys[groupId].hash = hash;\n" +
        "    \t\tgroupBys[groupId].timestamp = now;\n" +
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

    let getGroupBy = "\tfunction getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){\n" +
        "    \t\treturn(groupBys[idGroup].hash, groupBys[idGroup].timestamp);\n" +
        "    \t}\n\n";

    let getLatestGroupBy = "function getLatestGroupBy(bytes32 operation) public constant returns(string latestGroupBy, uint ts){\n" +
        "\t\tif(groupId > 0){\n" +
        "\t\t\tif(operation == COUNT_LITERAL){\n" +
        "\t\t\t\tif(lastCount >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastCount].hash, groupBys[lastCount].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == SUM_LITERAL){\n" +
        "\t\t\t\tif(lastSUM >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == MIN_LITERAL){\n" +
        "\t\t\t\tif(lastMin >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMin].hash, groupBys[lastMin].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == MAX_LITERAL){\n" +
        "\t\t\t\tif(lastMax >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMax].hash, groupBys[lastMax].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (operation == AVERAGE_LITERAL){\n" +
        "\t\t\t\tif(lastAverage >= 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t}\n" +
        "\t\t}\n" +
        "\t\t\treturn (\"\",0);\n" +
        "\t}\n\n";

    let retValsLatest = "";
    let getParamsLatest = "";
    for (let i =0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsLatest += crnProp.data_type + " " + crnProp.key + "){\n";
            retValsLatest += "facts[dataId-1]." + crnProp.key + ");\n\t";
        } else {
            getParamsLatest += crnProp.data_type + " " + crnProp.key + ",";
            retValsLatest += "facts[dataId-1]." + crnProp.key + ",";
        }
    }
    let retFactLatest = "\t\t\treturn (" + retValsLatest ;
    let emptyRetFactLatest = "";

    for (let i =0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            if(crnProp.data_type === "string") {
                emptyRetFactLatest += "\"\"" + ");\n\t";
            } else {
                emptyRetFactLatest += "0" + ");\n\t";
            }
        } else {
            if(crnProp.data_type === "string") {
                emptyRetFactLatest += "\"\"" + ", ";
            } else {
                emptyRetFactLatest += "0, ";
            }
        }
    }

    let getLatestFact = "\tfunction getLatestFact() public constant returns (" + getParamsLatest +
        "\t\tif(dataId > 0){\n" + retFactLatest +
        "\t} else {\n" +
        "\t\t\treturn (" + emptyRetFactLatest +
        "\t}\n" +
        "\t}\n\n";


    let getAllFacts = "\tfunction getAllFacts(uint id) public returns (";
    let getParamsAll = "";
    let retValsAll = "";
    let assignements = "";
    let retStmtAll = "";
    for(let i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsAll += crnProp.data_type + "[] " + crnProp.key +"s, uint[] timestamps" + "){\n";
            retValsAll += "\t\t" + crnProp.data_type + "[] memory " + crnProp.key + "ss = new " + crnProp.data_type + "[](id);\n";
            retValsAll += "\t\tuint[] memory timestampss = new uint[](id);\n";
            assignements += "\t\t\t" +  crnProp.key + "ss[i] = fact." + crnProp.key+";\n";
            assignements += "\t\t\t" +  "timestampss[i] = fact.timestamp;\n";
            assignements += "\t\t}\n";
            retStmtAll += crnProp.key + "ss,";
            retStmtAll += "timestampss);\n"
        } else {
            getParamsAll += crnProp.data_type + "[] " + crnProp.key + "s,";
            retValsAll += "\t\t" + crnProp.data_type + "[] memory " + crnProp.key + "ss = new " + crnProp.data_type + "[](id);\n";
            assignements += "\t\t\t" +  crnProp.key + "ss[i] = fact." + crnProp.key+";\n";
            retStmtAll += crnProp.key + "ss,";
        }
    }
    let loopLine = "\t\tfor(uint i =0; i < id; i++){\n";
    let firstLoopLine = "\t\t\t" + fact_tbl.struct_Name + " storage fact = facts[i];\n";


    let getAllRet = "\t\treturn (";
    getAllRet += retStmtAll;
    loopLine += firstLoopLine + assignements + getAllRet + "\t}\n";


    getAllFacts = getAllFacts + getParamsAll + retValsAll + loopLine;



    contrPayload = firstLine + secondLine + thirdLine + fourthLine + fifthLine +  constr + struct + properties + closeStruct + groupStruct + groupMapping +  mapping + addFact + setters + retStmt + getFact + getParams + retFact + addGroupBy + getGroupBy + getLatestGroupBy + getAllFacts +  "\n}";
    fs.writeFile("contracts/" + fact_tbl.name + ".sol", contrPayload, function(err) {
        if(err) {
            res.send({msg:"error"});
            return console.log(err);
        }
        console.log("The file was saved!");
        let templ = {};
        if('template' in fact_tbl){
            templ = fact_tbl['template'];
        } else {
            templ = fact_tbl;
        }
        res.send({msg:"OK","filename":fact_tbl.name + ".sol", "template":templ});
    });

});

app.get('/getFactById/:id', function (req,res) {
    if (contract) {
        contract.methods.getFact(parseInt(req.params.id,10)).call(function (err, result) {
            if (!err) {
                let len  = Object.keys(result).length;
                for(let  j = 0; j < len /2; j ++){
                    delete result[j];
                }
                res.send(result);
            } else {
                console.log(err);
                console.log("ERRRRRR");
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.get('/allFacts', function (req,res) {
    getAllFactsHeavy(50).then(retval => {
        res.send(retval);
    }).catch(error => {
        console.log(error);
    });
});

async function getAllFactsHeavy(factsLength){
    let allFacts = [];
    await contract.methods.getAllFacts(factsLength).call(function(err, result) {
        if(!err){
            let len  = Object.keys(result).length;
            for(let  j = 0; j < len /2; j ++){
                delete result[j];
            }
            if('payloads' in result){
                for(let i = 0; i < result['payloads'].length; i++){
                    let crnLn = JSON.parse(result['payloads'][i]);
                    crnLn.timestamp =  result['timestamps'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            console.log(err);
            console.log("ERRRRRR");
        }
    });
    return allFacts;
}

async function getAllFacts(factsLength){
    let allFacts = [];
    for (let i = 0; i < factsLength; i++) {
        await contract.methods.facts(i).call(function(err, result2) {
            if(!err){
                let len  = Object.keys(result2).length;
                for(let  j = 0; j < len /2; j ++){
                    delete result2[j];
                }
                if('payload' in result2){
                    let crnLn = JSON.parse(result2['payload']);
                    crnLn.timestamp = result2['timestamp'];
                    allFacts.push(crnLn);
                }
            } else {
                console.log(err);
                console.log("ERRRRRR");
            }
        })
    }
    return allFacts;
}

app.get('/getallfacts', function(req, res) {
    if(contract) {
        contract.methods.dataId().call(function(err, result) {
            console.log("********");
            console.log(result);
            console.log("*****");
            if(!err) {
                //async loop waiting to get all the facts separately
                let timeStart = microtime.nowDouble();
                getAllFactsHeavy(result).then(retval => {
                    console.log("####");
                    console.log("Get all facts time: " + (microtime.nowDouble() - timeStart) + "s");
                    console.log("####");
                    //retval.timeDone = microtime.nowDouble() - timeStart;
                    res.send(retval);
                }).catch(error => {
                    console.log(error);
                });
            } else {
                console.log(err);
                console.log("ERRRRRR");
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.get('/groupbyId/:id', function (req,res) {
    if(contract) {
        contract.methods.getGroupBy(parseInt(req.params.id,10)).call(function (err, result) {
            if(!err) {
                res.send(result)
            } else {
                console.log(err);
                console.log("ERRRRRR");
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.post('/addFacts', function (req,res) {
    if(contract) {
        if(req.body.products.length === req.body.quantities.length === req.body.customers.length) {
            contract.methods.addFacts(req.body.products, req.body.quantities, req.body.customers).call(function (err, result) {
                if (!err) {
                    res.send(result)
                } else {
                    console.log(err);
                    console.log("ERRRRRR");
                    res.send(err);
                }
            })
        } else {
            res.status(400);
            res.send({status: "ERROR",options: "Arrays must have the same dimension" });
        }
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

function transformGBFromSQL(groupByResult, operation, aggregateField, gbField) {
    console.log(groupByResult);
    console.log(gbField);
    let transformed = {};
    if (operation === 'COUNT') {
        console.log("OPERATION = COUNT");
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'COUNT';
    } else if (operation === "SUM") {
        console.log("OPERATION = SUM");
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'SUM';
    }  else if (operation === "MIN"){
        console.log("OPERATION = MIN");
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MIN(' + aggregateField + ')'];
            delete groupByResult[i]['MIN(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MIN';
    } else if (operation === "MAX") {
        console.log("OPERATION = MAX");
        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['MAX(' + aggregateField + ')'];
            delete groupByResult[i]['MAX(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = crnCount;
        }
        transformed['operation'] = 'MAX';
    } else { // AVERAGE
        console.log("OPERATION = AVERAGE");

        for (let i = 0; i < groupByResult.length; i++) {
            let crnCount = groupByResult[i]['COUNT(' + aggregateField + ')'];
            let crnSum = groupByResult[i]['SUM(' + aggregateField + ')'];
            delete groupByResult[i]['COUNT(' + aggregateField + ')'];
            delete groupByResult[i]['SUM(' + aggregateField + ')'];
            let filtered = groupByResult[i];
            transformed[JSON.stringify(filtered)] = {count: crnCount, sum: crnSum, average:crnSum / crnCount};
        }

        transformed['operation'] = 'AVERAGE';
    }
    transformed['groupByFields'] = gbField;
    transformed["field"] = aggregateField;
    return transformed;
}

function transformGB(groupByResult, operation, aggregateField){
    if (operation === "COUNT") {
        console.log("OPERATION = COUNT");
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            for (let row in crnGoup){
                cnt++;
            }
            groupByResult[key] = cnt;
        }
        groupByResult["operation"] = "COUNT"
    } else if(operation === "SUM") {
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            for (let row in crnGoup){
                cnt += Number(crnGoup[row][aggregateField]);
            }
            groupByResult[key] = cnt;
        }
        groupByResult["operation"] = "SUM";
        groupByResult["field"] = aggregateField;

    } else if(operation === "MIN"){
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let min = Number(crnGoup[0][aggregateField]);
            for (let row in crnGoup){
                if(Number(crnGoup[row][aggregateField]) < min){
                    min = Number(crnGoup[row][aggregateField])
                }
            }
            groupByResult[key] = min;
        }
        groupByResult["operation"] = "MIN";
        groupByResult["field"] = aggregateField;

    } else if(operation === "MAX"){
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let max = Number(crnGoup[0][aggregateField]);
            for (let row in crnGoup){
                if(Number(crnGoup[row][aggregateField]) > max){
                    max = Number(crnGoup[row][aggregateField])
                }
            }
            groupByResult[key] = max;
        }
        groupByResult["operation"] = "MAX";
        groupByResult["field"] = aggregateField;
    } else { //AVERAGE
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            let sum = 0;
            for (let row in crnGoup){
                sum += Number(crnGoup[row][aggregateField]);
                cnt += 1;
            }
            groupByResult[key] = {"average":sum / cnt, "count": cnt, "sum": sum};
        }
        groupByResult["operation"] = "AVERAGE";
        groupByResult["field"] = aggregateField;
    }
    return groupByResult;
}
function calculateReducedGB(operation, aggregateField, cachedGroupBy, gbFields){
    let transformedArray = [];
    let originalArray = [];
    let i = 0;
    //logic to incrementally calculate the new gb
    Object.keys(cachedGroupBy).forEach(function(key,index) {
        if(key !== 'operation' && key !== 'groupByFields' && key !== 'field') {
            let crnUniqueVal = JSON.parse(key);
            console.log("crnuniqueVal BEFORE");
            console.log(crnUniqueVal);
            console.log("***");
            originalArray[i] = cachedGroupBy[key];
            Object.keys(crnUniqueVal).forEach(function(key2,index2) {
                console.log("gbFields = " + gbFields);
                console.log("key2 = " + key2);
                if(gbFields.indexOf(key2) <= -1){
                    delete crnUniqueVal[key2];
                }
                transformedArray[i] = JSON.stringify(crnUniqueVal);
            });
            console.log("crnuniqueVal AFTER");
            console.log(crnUniqueVal);
            i++;
            console.log("***");
        }
        console.log("transformed array = " + transformedArray);
        console.log("original array = " + originalArray);
    });
    let uniqueKeys = new Set(transformedArray);
    let uniqueKeysArray = Array.from(uniqueKeys);
    let respObj = {};
    if (operation === 'SUM' || operation === 'COUNT') {
        let sumPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            sumPerKey[j] = 0;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            sumPerKey[indexOfUK] += originalArray[j];
        }
        for (let j = 0; j < sumPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = sumPerKey[j];
        }
        console.log(uniqueKeysArray);
        console.log(sumPerKey);
    } else if (operation === 'MIN') {
        let minPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            minPerKey[j] = Math.max;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            if (originalArray[j] < minPerKey[indexOfUK]) {
                minPerKey[indexOfUK] = originalArray[j];
            }
        }
        for (let j = 0; j < minPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = minPerKey[j];
        }
    } else if (operation === 'MAX') {
        let maxPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            maxPerKey[j] = Math.min;
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            if (originalArray[j] > maxPerKey[indexOfUK]) {
                maxPerKey[indexOfUK] = originalArray[j];
            }
        }
        for (let j = 0; j < maxPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = maxPerKey[j];
        }
    } else { // AVERAGE
        let avgPerKey = [];
        for (let j = 0; j < uniqueKeysArray.length; j++) {
            avgPerKey[j] = JSON.stringify({count: 0, sum: 0, average: 0});
        }
        for (let j = 0; j < transformedArray.length; j++) {
            let crnObj = transformedArray[j];
            let indexOfUK = uniqueKeysArray.indexOf(crnObj);
            let parsedObj = JSON.parse(avgPerKey[j]);
            let newSum = parsedObj['sum'] +  originalArray[j]['sum'];
            let newCount =  parsedObj['count'] +  originalArray[j]['count'];
            avgPerKey[indexOfUK] = {count: newCount, sum: newSum, average: newSum/newCount};
        }
        for (let j = 0; j < avgPerKey.length; j++) {
            let crnKey = uniqueKeysArray[j];
            respObj[crnKey] = avgPerKey[j];
        }
        console.log(uniqueKeysArray);
        console.log(avgPerKey);
    }
    return respObj;
}
app.get('/groupby/:field/:operation/:aggregateField', function (req,res) {
    //LOGIC: IF latestGroupByTS >= latestFactTS RETURN LATEST GROUPBY FROM REDIS
    //      ELSE CALCULATE GROUBY FOR THE DELTAS (AKA THE ROWS ADDED AFTER THE LATEST GROUPBY) AND APPEND TO THE ALREADY SAVED IN REDIS
    let gbFields = [];
    if(req.params.field.indexOf('|') > -1){
        // more than 1 group by fields
        gbFields = req.params.field.split('|');
    } else {
        gbFields.push(req.params.field);
    }
    console.log(gbFields);
    let python = false;
    if(contract) {
        let timeStart = 0;
        contract.methods.dataId().call(function (err,latestId) {
            contract.methods.getLatestGroupBy(Web3.utils.fromAscii(req.params.operation)).call(function (err, latestGroupBy) {
                console.log("LATEST GB IS: ");
                console.log(latestGroupBy);
                if(latestGroupBy.ts > 0) {
                    contract.methods.getFact(latestId-1).call(function (err, latestFact) {
                        if (latestGroupBy.ts >= latestFact.timestamp) {

                            // check what is the latest groupBy
                            // if latest groupby contains all fields for the new groupby requested
                            // -> incrementaly calculate the groupby requested by summing the one in redis cache
                            //

                            client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                if (error) {
                                    console.log(error);
                                    res.send(error);
                                } else {
                                    cachedGroupBy = JSON.parse(cachedGroupBy);
                                    console.log("**");
                                    console.log(cachedGroupBy);
                                    console.log("**");
                                    let containsAllFields = true;
                                    for (let i = 0; i < gbFields.length; i++) {
                                        if (!cachedGroupBy.groupByFields.includes(gbFields[i])) {
                                            containsAllFields = false
                                        }
                                    }
                                    if(containsAllFields && cachedGroupBy.groupByFields.length !== gbFields.length) { //it is a different groupby thna the stored
                                        if (cachedGroupBy.field === req.params.aggregateField &&
                                            req.params.operation === cachedGroupBy.operation) {
                                            let respObj = calculateReducedGB(req.params.operation, req.params.aggregateField, cachedGroupBy, gbFields);
                                            res.send(JSON.stringify(respObj));
                                        }
                                    } else {
                                        console.log("getting it from redis");
                                        timeStart = microtime.nowDouble();
                                        client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                            if (error) {
                                                console.log(error);
                                                res.send(error);
                                            } else {
                                                console.log('GET result ->' + cachedGroupBy);
                                                let timeFinish = microtime.nowDouble();
                                                cachedGroupBy = JSON.parse(cachedGroupBy);
                                                cachedGroupBy.executionTime = timeFinish - timeStart;
                                                res.send(JSON.stringify(cachedGroupBy));
                                            }
                                        });
                                    }
                                }
                            });

                        } else {
                            //CALCULATE GROUPBY FOR DELTAS (fact.timestamp > latestGroupBy timestamp)   AND THEN APPEND TO REDIS
                            getAllFacts(latestId).then(retval => {
                                // get (fact.timestamp > latestGroupBy timestamp)
                                let deltas = [];
                                for (let i = 0; i < retval.length; i++){
                                    let crnFact = retval[i];
                                    if(crnFact.timestamp > latestGroupBy.ts) {
                                        deltas.push(crnFact);
                                    }
                                }
                                timeStart = microtime.nowDouble();

                                //python
                                if(python) {
                                    calculateGBPython(deltas.toString(), req.params.field, req.params.aggregateField, req.params.operation).then(result => {
                                        result = JSON.parse(result);
                                        console.log("$$$$$");
                                        console.log(result);
                                        console.log("$$$$$");
                                        result = JSON.parse(result);
                                        result.operation = req.params.operation;
                                        result.field = req.params.aggregateField;
                                        result.time = microtime.nowDouble() - timeStart;
                                        let deltaGroupBy = result;
                                        client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                            if (error) {
                                                console.log(error);
                                                res.send(error);
                                            } else {
                                                console.log('GET result ->' + cachedGroupBy);

                                                //IF COUNT / SUM -> ADD
                                                //ELIF MIN -> NEW_MIN = MIN OF MINS

                                                let ObCachedGB = JSON.parse(cachedGroupBy);
                                                let updatedGB = {};
                                                if(ObCachedGB["operation"] === "SUM"){
                                                    updatedGB = helper.sumObjects(ObCachedGB,deltaGroupBy);
                                                } else if(ObCachedGB["operation"] === "COUNT"){
                                                    updatedGB = helper.sumObjects(ObCachedGB,deltaGroupBy);
                                                } else if(ObCachedGB["operation"] === "MAX"){
                                                    updatedGB = helper.maxObjects(ObCachedGB,deltaGroupBy)
                                                } else if(ObCachedGB["operation"] === "MIN"){
                                                    updatedGB = helper.minObjects(ObCachedGB,deltaGroupBy)
                                                } else { //AVERAGE
                                                    updatedGB = helper.averageObjects(ObCachedGB,deltaGroupBy)
                                                }
                                                let timeFinish = microtime.nowDouble();
                                                client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                                updatedGB.executionTime = timeFinish - timeStart;
                                                res.send(JSON.stringify(updatedGB));
                                            }
                                        });
                                    });
                                } else {

                                    // calculate groupby for deltas in SQL
                                    connection.query(createTable, function (error, results, fields) {
                                        if (error) throw error;
                                        for(let i = 0; i < deltas.length; i++){
                                            delete deltas[i].timestamp;
                                        }

                                        let sql = jsonSql.build({
                                            type: 'insert',
                                            table: tableName,
                                            values: deltas
                                        });

                                        let editedQuery = sql.query.replace(/"/g, "");
                                        editedQuery = editedQuery.replace(/''/g, "null");
                                        console.log(editedQuery);
                                        connection.query(editedQuery, function (error, results2, fields) {
                                            let gbQuery = null;
                                            if(req.params.operation === 'AVERAGE'){
                                                gbQuery = jsonSql.build({
                                                    type: 'select',
                                                    table: tableName,
                                                    group: gbFields,
                                                    fields: [ gbFields,
                                                        {func: {
                                                                name: 'SUM',
                                                                args: [
                                                                    {field: req.params.aggregateField}
                                                                ]
                                                            }
                                                        },
                                                        {func: {
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
                                            let editedGB = gbQuery.query.replace(/"/g, "");
                                            connection.query(editedGB, function (error, results3, fields) {
                                                connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                                                    if(!err){
                                                        let deltaGroupBy = transformGBFromSQL(results3,req.params.operation, req.params.aggregateField, gbFields);
                                                        client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                                            if (error) {
                                                                console.log(error);
                                                                res.send(error);
                                                            } else {
                                                                console.log('GET result ->' + cachedGroupBy);
                                                                //IF COUNT / SUM -> ADD
                                                                //ELIF MIN -> NEW_MIN = MIN OF MINS

                                                                let ObCachedGB = JSON.parse(cachedGroupBy);
                                                                let updatedGB = {};
                                                                if(ObCachedGB["operation"] === "SUM"){
                                                                    updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                                } else if(ObCachedGB["operation"] === "COUNT"){
                                                                    updatedGB = helper.sumObjects(ObCachedGB, deltaGroupBy);
                                                                } else if(ObCachedGB["operation"] === "MAX"){
                                                                    updatedGB = helper.maxObjects(ObCachedGB, deltaGroupBy)
                                                                } else if(ObCachedGB["operation"] === "MIN"){
                                                                    updatedGB = helper.minObjects(ObCachedGB, deltaGroupBy)
                                                                } else { // AVERAGE
                                                                    updatedGB = helper.averageObjects(ObCachedGB, deltaGroupBy)
                                                                }
                                                                let timeFinish = microtime.nowDouble();
                                                                client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                                                updatedGB.executionTime = timeFinish - timeStart;
                                                                res.send(JSON.stringify(updatedGB));
                                                            }
                                                        });
                                                    } else {
                                                        res.send("error");
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
                                    //         if(ObCachedGB["operation"] === "SUM"){
                                    //             updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                    //         } else if(ObCachedGB["operation"] === "COUNT"){
                                    //             updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                    //         } else if(ObCachedGB["operation"] === "MAX"){
                                    //             updatedGB = maxObjects(ObCachedGB,deltaGroupBy)
                                    //         } else if(ObCachedGB["operation"] === "MIN"){
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

                                //      console.log("DELTAS GB---->");
                                //      console.log(deltaGroupBy);
                                //      console.log("DELTAS GB---->");
                                //      console.log(latestGroupBy);

                            }).catch(error => {
                                console.log(error);
                            });
                        }
                    }).catch(error => {
                        console.log(error);
                    });
                } else {
                    //NO GROUP BY, SHOULD CALCULATE IT FROM THE BEGGINING
                    getAllFacts(latestId).then(retval => {
                        timeStart = microtime.nowDouble();
                        let groupByResult;
                        let timeFinish = 0;
                        const transactionObject = {
                            from: acc,
                            gas: 15000000,
                            gasPrice: '30000000000000'
                        };
                        if(python) {
                            calculateGBPython(retval, req.params.field, req.params.aggregateField, req.params.operation, function (results, err) {
                                if(err){
                                    console.log(err);
                                }
                                timeFinish = microtime.nowDouble();
                                console.log("$$$$$");
                                console.log(results);
                                console.log("$$$$$");
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
                                contract.methods.addGroupBy(hash, Web3.utils.fromAscii(req.params.operation)).send(transactionObject,  (err, txHash) => {
                                    console.log('send:', err, txHash);
                                }).on('error', (err) => {
                                    console.log('error:', err);
                                    res.send(err);
                                }).on('transactionHash', (err) => {
                                    console.log('transactionHash:', err);
                                }).on('receipt', (receipt) => {
                                    console.log('receipt:', receipt);
                                    groupByResult = JSON.parse(groupByResult);
                                    groupByResult.receipt = receipt;
                                    res.send(JSON.stringify(groupByResult));
                                });
                            });
                        } else {
                            connection.query(createTable, function (error, results, fields) {
                                if (error) throw error;
                                for(let i =0; i < retval.length; i++){
                                    delete retval[i].timestamp;
                                }

                                let sql = jsonSql.build({
                                    type: 'insert',
                                    table: tableName,
                                    values: retval
                                });

                                let editedQuery = sql.query.replace(/"/g, "");
                                editedQuery = editedQuery.replace(/''/g, "null");
                                console.log(editedQuery);
                                connection.query(editedQuery, function (error, results2, fields) {
                                    let gbQuery = null;
                                    if(req.params.operation === 'AVERAGE'){
                                        gbQuery = jsonSql.build({
                                            type: 'select',
                                            table: tableName,
                                            group: gbFields,
                                            fields: [ gbFields,
                                                {func: {
                                                        name: 'SUM',
                                                        args: [
                                                            {field: req.params.aggregateField}
                                                        ]
                                                    }
                                                },
                                                {func: {
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
                                    let editedGB = gbQuery.query.replace(/"/g, "");
                                    connection.query(editedGB, function (error, results3, fields) {
                                        connection.query('DROP TABLE ' + tableName, function (err, resultDrop) {
                                            if(!err){
                                                let groupBySqlResult = transformGBFromSQL(results3,req.params.operation, req.params.aggregateField, gbFields);
                                                let timeFinish = microtime.nowDouble();
                                                md5sum = crypto.createHash('md5');
                                                md5sum.update(JSON.stringify(groupBySqlResult));
                                                let hash = md5sum.digest('hex');
                                                console.log(hash);
                                                console.log("**");
                                                console.log(JSON.stringify(groupBySqlResult));
                                                console.log("**");
                                                client.set(hash, JSON.stringify(groupBySqlResult), redis.print);
                                                contract.methods.addGroupBy(hash, Web3.utils.fromAscii(req.params.operation)).send(transactionObject,  (err, txHash) => {
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
                                                    res.send(JSON.stringify(groupBySqlResult));
                                                });
                                            } else {
                                                res.send("error");
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
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.get('/getcount', function(req, res) {
    if (contract) {
        contract.methods.dataId().call(function(err, result) {
            if (!err) {
                res.send(result);
            } else {
                console.log(err);
                console.log("ERRRRRR");
                res.send(err);
            }
        })
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.post('/addFact', function(req, res) {
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
            if (crnVal.type === "bytes32") {
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
            res.send({status: "ERROR",options: "Contract not supporting more than 10 fields" });
        }
        addFactPromise.send(transactionObject,  (err, txHash) => {
            console.log('send:', err, txHash);
        }).on('error', (err) => {
            console.log('error:', err);
            res.send(err);
        })
            .on('transactionHash', (err) => {
                console.log('transactionHash:', err);
            })
            .on('receipt', (receipt) => {
                console.log('receipt:', receipt);
                res.send(receipt);
            });
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});