const express = require('express');
const bodyParser = require('body-parser');
const solc = require('solc');
const fs = require('fs');
const delay = require('delay');
const groupBy = require('group-by');
let fact_tbl = require('./templates/fact_tbl');
var crypto = require('crypto');
var md5sum = crypto.createHash('md5');
abiDecoder = require('abi-decoder');
const app = express();
var jsonParser = bodyParser.json();
app.use(jsonParser);

app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');
app.use(express.static('public'));

const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));
var redis = require('redis');
var client = redis.createClient(6379,"127.0.0.1");
client.on('connect', function(){
    console.log('Redis client connected');
});

client.on('error', function (err) {
    console.log('Something went wrong ' + err);
});
let contractInstance = null;


web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let DataHandler = null;
let acc = null;
app.get('/', function (req,res) {
    fs.readdir('./templates', function(err, items) {
        res.render("index",{"templates":items});
    });
});

app.listen(3000, () => console.log(`Example app listening on http://localhost:3000`));

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
                });
            return contractInstance.options;
}

app.get('/deployContract/:fn', function (req, res) {
    web3.eth.getAccounts(function (err,accounts) {
        if (!err) {
        acc = accounts[1];
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

app.get('/new_contract/:fn', function (req, res) {
    let fact_tbl = require('./templates/' + req.params.fn);
    let contrPayload = "";
    let firstLine = "pragma solidity ^0.4.0;\n\n";
    let secondLine = "contract " + fact_tbl.name + " { \n";
    let thirdLine = "\tuint public dataId;\n";
    let fourthLine = "\tuint public groupId;\n\n";
    let fifthLine = "\tuint public lastCount;\n" +
        "\tuint public lastSUM;\n" +
        "\tuint public lastMin;\n" +
        "\tuint public lastMax;\n" +
        "\tuint public lastAverage;\n" +
        "\tbytes constant MIN_LITERAL = \"MIN\";\n" +
        "\tbytes constant MAX_LITERAL = \"MAX\";\n" +
        "\tbytes constant AVERAGE_LITERAL = \"AVERAGE\";\n" +
        "\tbytes constant COUNT_LITERAL = \"COUNT\";\n" +
        "\tbytes constant SUM_LITERAL = \"SUM\";\n";
    let constr = "\tconstructor() {\n" +
        "\t\tdataId = 0;\n" +
        "\t\tgroupId = 0;\n" +
        "\t\tlastCount = 0;\n" +
        "\t\tlastSUM = 0;\n" +
        "\t\tlastMin = 0;\n" +
        "\t\tlastMax = 0;\n" +
        "\t\tlastAverage = 0;\n" +
        "\t}\n";
    var properties = "";
    var struct = "\tstruct " + fact_tbl.struct_Name + "{ \n";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        properties += "\t\t" + crnProp.data_type + " " + crnProp.key + ";\n";
    }
    var groupStruct = "\tstruct groupBy{ \n  \t\tstring hash;\n" +
        "        uint timestamp;\n\t}\n";
    let groupMapping =  "\tmapping(uint => groupBy) public groupBys;\n\n";
    properties += "\t\tuint timestamp;\n";
    let closeStruct = "\t}\n";
    let mapping = "\tmapping(uint =>" + fact_tbl.struct_Name +") public facts;\n\n";
    let addParams = "";
    let addFact = "\tfunction addFact(";
    for(var i = 0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            addParams += crnProp.data_type + " " + crnProp.key + ") ";
        } else {
            addParams += crnProp.data_type + " " + crnProp.key + ",";
        }
    }
    let retParams = "public returns (";
    for(var i = 0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            retParams += crnProp.data_type + " " + ", uint ID){\n";
        } else {
            retParams += crnProp.data_type + " " + ",";
        }
    }
    addFact = addFact + addParams + retParams;
    var setters = "";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        setters += "\t\tfacts[dataId]." + crnProp.key  + "= " +  crnProp.key + ";\n";
    }
    setters += "\t\tfacts[dataId].timestamp = now;\n \t\tdataId += 1;\n";
    var retStmt = "\t\treturn (";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        retStmt += "facts[dataId-1]." + crnProp.key  + ",";
    }
    retStmt += "dataId -1);\n\t}\n\n";

    let getParams = "";
    var getFact = "\tfunction getFact(uint id) public constant returns (";
    var retVals = "";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParams += crnProp.data_type + " " + crnProp.key +", uint timestamp" + "){\n";
            retVals += "facts[id]." + crnProp.key + ", facts[id].timestamp" +  ");\n\t}\n\n";
        } else {
            getParams += crnProp.data_type + " " + crnProp.key + ",";
            retVals += "facts[id]." + crnProp.key + ",";
        }
    }
    var retFact = "\t\treturn (" + retVals ;

    var addGroupBy = "\tfunction addGroupBy(string hash, bytes category) public returns(string groupAdded, uint groupID){\n" +
        "    \t\tgroupBys[groupId].hash = hash;\n" +
        "    \t\tgroupBys[groupId].timestamp = now;\n" +
        "\t\t\tif(keccak256(category) == keccak256(COUNT_LITERAL)){\n" +
        "\t\t\t\tlastCount  = groupID;\n" +
        "\t\t\t} else if(keccak256(category) == keccak256(SUM_LITERAL)){\n" +
        "\t\t\t\tlastSUM = groupID;\n" +
        "\t\t\t} else if(keccak256(category) == keccak256(MIN_LITERAL)){\n" +
        "\t\t\t\tlastMin = groupID;\n" +
        "\t\t\t} else if(keccak256(category) == keccak256(MAX_LITERAL)){\n" +
        "\t\t\t\tlastMax = groupID;\n" +
        "\t\t\t} else if(keccak256(category) == keccak256(AVERAGE_LITERAL)){\n" +
        "\t\t\t\tlastAverage = groupID;\n" +
        "\t\t\t}\n" +
        "    \t\tgroupId += 1;\n" +
        "    \t\treturn (groupBys[groupId-1].hash, groupId-1);\n" +
        "    \t}";

    var getGroupBy = "\tfunction getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){\n" +
        "    \t\treturn(groupBys[idGroup].hash, groupBys[idGroup].timestamp);\n" +
        "    \t}\n\n";

    var getLatestGroupBy = "function getLatestGroupBy(bytes operation) public constant returns(string latestGroupBy, uint ts){\n" +
        "\t\tif(groupId > 0){\n" +
        "\t\t\tif(keccak256(operation) == keccak256(COUNT_LITERAL)){\n" +
        "\t\t\t\tif(lastCount > 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastCount].hash, groupBys[lastCount].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (keccak256(operation) == keccak256(SUM_LITERAL)){\n" +
        "\t\t\t\tif(lastSUM > 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (keccak256(operation) == keccak256(MIN_LITERAL)){\n" +
        "\t\t\t\tif(lastMin > 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMin].hash, groupBys[lastMin].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (keccak256(operation) == keccak256(MAX_LITERAL)){\n" +
        "\t\t\t\tif(lastMax > 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastMax].hash, groupBys[lastMax].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t} else if (keccak256(operation) == keccak256(AVERAGE_LITERAL)){\n" +
        "\t\t\t\tif(lastAverage > 0){\n" +
        "\t\t\t\t\treturn (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp);\n" +
        "\t\t\t\t}\n" +
        "\t\t\t}\n" +
        "\t\t}\n" +
        "\t\t\treturn (\"\",0);\n" +
        "\t}";

    var retValsLatest = "";
    let getParamsLatest = "";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsLatest += crnProp.data_type + " " + crnProp.key + "){\n";
            retValsLatest += "facts[dataId-1]." + crnProp.key + ");\n\t";
        } else {
            getParamsLatest += crnProp.data_type + " " + crnProp.key + ",";
            retValsLatest += "facts[dataId-1]." + crnProp.key + ",";
        }
    }
    var retFactLatest = "\t\t\treturn (" + retValsLatest ;
    var emptyRetFactLatest = "";

    for(var i =0; i < fact_tbl.properties.length; i++){
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

    var getLatestFact = "\tfunction getLatestFact() public constant returns (" + getParamsLatest +
        "\t\tif(dataId > 0){\n" + retFactLatest +
        "\t} else {\n" +
        "\t\t\treturn (" + emptyRetFactLatest +
        "\t}\n" +
        "\t}\n\n";

    contrPayload = firstLine + secondLine + thirdLine + fourthLine + fifthLine +  constr + struct + properties + closeStruct + groupStruct + groupMapping +  mapping + addFact + setters + retStmt + getFact + getParams + retFact + addGroupBy + getGroupBy + getLatestGroupBy +   "\n}";
    fs.writeFile("contracts/" + fact_tbl.name + ".sol", contrPayload, function(err) {
        if(err) {
            res.send({msg:"error"});
            return console.log(err);
        }
        console.log("The file was saved!");
        res.send({msg:"OK","filename":fact_tbl.name + ".sol", "template":fact_tbl});
    });

});

app.get('/getFactById/:id', function (req,res) {
    if(contract) {
        contract.methods.getFact(parseInt(req.params.id,10)).call(function (err, result) {
            if(!err) {
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

async function getAllFacts(factsLength){
    let allFacts = [];
    for (let i = 0; i < factsLength; i++){
        await contract.methods.facts(i).call(function (err, result2) {
            if(!err){
                let len  = Object.keys(result2).length;
                for(let  j = 0; j < len /2; j ++){
                    delete result2[j];
                }
                allFacts.push(result2);
            } else {
                console.log(err);
                console.log("ERRRRRR");
            }
        })
    }
    return allFacts;
}

app.get('/getallfacts', function (req,res) {
    if(contract) {
        contract.methods.dataId().call(function (err, result) {
            console.log("********");
            console.log(result);
            console.log("*****");
            if(!err) {
              //async loop waiting to get all the facts separately
                getAllFacts(result).then(retval => {
                    console.log(retval);
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

function transformGB(groupByResult, operation, aggregateField){
    if(operation === "COUNT"){
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
    } else if(operation === "SUM"){
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let cnt = 0;
            for (let row in crnGoup){
                cnt += Number(crnGoup[row][aggregateField]);
            }
            groupByResult[key] = cnt;
        }
        groupByResult["operation"] = "SUM";
        groupByResult["field"] = req.params.aggregateField;

    } else if(operation === "MIN"){
        for (let key in groupByResult) {
            let crnGoup = groupByResult[key];
            let min = Number(crnGoup[row][aggregateField]);
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
            let max = Number(crnGoup[row][req.params.aggregateField]);
            for (let row in crnGoup){
                if(Number(crnGoup[row][req.params.aggregateField]) > max){
                    max = Number(crnGoup[row][req.params.aggregateField])
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

function sumObjects(ob1, ob2) {
    let sum = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                sum[key] = ob1[key] + ob2[key]
            }
        }
    });
    sum["operation"] = ob1["operation"];
    sum["field"] = ob1["field"];
    return sum;
}

function maxObjects(ob1, ob2) {
    let max = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                if(ob1[key] >= ob2[key]) {
                    max[key] = ob1[key];
                } else {
                    max[key] = ob2[key];
                }
            }
        }
    });
    max["operation"] = ob1["operation"];
    max["field"] = ob1["field"];
    return max;
}

function minObjects(ob1, ob2) {
    let min = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                if(ob1[key] <= ob2[key]) {
                    min[key] = ob1[key];
                } else {
                    min[key] = ob2[key];
                }
            }
        }
    });
    min["operation"] = ob1["operation"];
    min["field"] = ob1["field"];
    return max;
}

function averageObjects(ob1, ob2) {
    let avg = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                let sum_new = ob1[key]["sum"] + ob2[key]["sum"];
                let count_new = ob1[key]["count"] + ob2[key]["count"];
                let avg_new = sum_new / count_new;
                avg[key] = {"average": avg_new, "count": count_new, "sum": sum_new};
            }
        }
    });
    avg["operation"] = ob1["operation"];
    avg["field"] = ob1["field"];
    return avg;
}

app.get('/groupby/:field/:operation/:aggregateField', function (req,res) {
    //LOGIC: IF latestGroupByTS >= latestFactTS RETURN LATEST GROUPBY FROM REDIS
    //      ELSE CALCULATE GROUBY FOR THE DELTAS (AKA THE ROWS ADDED AFTER THE LATEST GROUPBY) AND APPEND TO THE ALREADY SAVED IN REDIS
    if(contract) {
        contract.methods.dataId().call(function (err,latestId) {

                contract.methods.getLatestGroupBy(Web3.utils.fromAscii(req.params.operation)).call(function (err, latestGroupBy) {
                    if(latestGroupBy.ts > 0) {
                        contract.methods.getFact(latestId-1).call(function (err, latestFact) {
                            console.log("LATEST FACT IS");
                            console.log(latestFact);
                            if (latestGroupBy.ts >= latestFact.timestamp) {
                                console.log("getting it from redis");
                                client.get(latestGroupBy.latestGroupBy, function (error, cachedGroupBy) {
                                    if (error) {
                                        console.log(error);
                                        res.send(error);
                                    } else {
                                        console.log('GET result ->' + cachedGroupBy);
                                        res.send(cachedGroupBy);
                                    }
                                });
                            } else {
                                //CALCULATE GROUPBY FOR DELTAS (fact.timestamp > latestGroupBy timestamp)   AND THEN APPEND TO REDIS
                                getAllFacts(latestId).then(retval => {
                                    // get (fact.timestamp > latestGroupBy timestamp)
                                    let deltas = [];
                                    for (var i = 0; i < retval.length; i++){
                                        let crnFact = retval[i];
                                        if(crnFact.timestamp > latestGroupBy.ts) {
                                            deltas.push(crnFact);
                                        }
                                    }
                                    console.log("DELTAS---->");
                                    console.log(deltas);
                                    console.log("DELTAS---->");

                                    let deltaGroupBy = groupBy(deltas, req.params.field);
                                    deltaGroupBy = transformGB(deltaGroupBy, req.params.operation, req.params.aggregateField);
                                    console.log("DELTAS GB---->");
                                    console.log(deltaGroupBy);
                                    console.log("DELTAS GB---->");
                                    console.log(latestGroupBy);


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
                                                updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                            } else if(ObCachedGB["operation"] === "COUNT"){
                                                updatedGB = sumObjects(ObCachedGB,deltaGroupBy);
                                            } else if(ObCachedGB["operation"] === "MAX"){
                                                updatedGB = maxObjects(ObCachedGB,deltaGroupBy)
                                            } else if(ObCachedGB["operation"] === "MIN"){
                                                updatedGB = minObjects(ObCachedGB,deltaGroupBy)
                                            } else { //AVERAGE
                                                updatedGB = averageObjects(ObCachedGB,deltaGroupBy)
                                            }


                                            client.set(latestGroupBy.latestGroupBy, JSON.stringify(updatedGB), redis.print);
                                            res.send(JSON.stringify(updatedGB));
                                        }
                                    });

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
                            console.log(retval);
                            let groupByResult = groupBy(retval,req.params.field);
                            console.log(groupByResult);
                            groupByResult = transformGB(groupByResult, req.params.operation, req.params.aggregateField);
                            groupByResult = JSON.stringify(groupByResult);
                            md5sum = crypto.createHash('md5');
                            md5sum.update(groupByResult);
                            var hash = md5sum.digest('hex');
                            console.log(hash);
                            client.set(hash, groupByResult, redis.print);

                            const transactionObject = {
                                from: acc,
                                gas: 15000000,
                                gasPrice: '30000000000000'
                            };

                            contract.methods.addGroupBy(hash, Web3.utils.fromAscii(req.params.operation)).send(transactionObject,  (err, txHash) => {
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
                                    res.send(JSON.stringify(receipt) + "\n" + groupByResult);
                                });
                            // res.send(groupByResult);
                        }).catch(error => {
                            console.log(error);
                        });
                    }
                }).catch(error => {
                    console.log(error);
                });


        // contract.methods.dataId().call(function (err, result) {
        //     if(!err) {
        //         //async loop waiting to get all the facts separately
        //         getAllFacts(result).then(retval => {
        //             console.log(retval);
        //             let groupByResult = groupBy(retval,req.params.field);
        //             groupByResult = JSON.stringify(groupByResult);
        //
        //             md5sum = crypto.createHash('md5');
        //             md5sum.update(groupByResult);
        //             var hash = md5sum.digest('hex');
        //             console.log(hash);
        //             client.set(hash, groupByResult, redis.print);
        //
        //                 const transactionObject = {
        //                     from: acc,
        //                     gas: 1500000,
        //                     gasPrice: '30000000000000'
        //                 };
        //
        //                 contract.methods.addGroupBy(hash).send(transactionObject,  (err, txHash) => {
        //                     console.log('send:', err, txHash);
        //                 }).on('error', (err) => {
        //                     console.log('error:', err);
        //                     res.send(err);
        //                 })
        //                     .on('transactionHash', (err) => {
        //                         console.log('transactionHash:', err);
        //                     })
        //                     .on('receipt', (receipt) => {
        //                         console.log('receipt:', receipt);
        //                         res.send(receipt);
        //                     });
        //
        //            // res.send(groupByResult);
        //         }).catch(error => {
        //             console.log(error);
        //         });
        //     } else {
        //         console.log(err);
        //         console.log("ERRRRRR");
        //         res.send(err);
        //     }
        // })
        });
    } else {
        res.status(400);
        res.send({status: "ERROR",options: "Contract not deployed" });
    }
});

app.get('/getcount', function (req,res) {
    if(contract) {
        contract.methods.dataId().call(function (err, result) {
            if(!err) {
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

app.post('/addFact', function (req,res) {
    if(contract) {
        const transactionObject = {
            from: acc,
            gas: 1500000,
            gasPrice: '30000000000000'
        };
        console.log(req.body);
        let vals = req.body.values;
        for(var i = 0; i < req.body.values.length; i++){
            let crnVal = req.body.values[i];
            if(crnVal.type === "bytes32"){
                req.body.values[i].value = web3.utils.fromAscii(req.body.values[i].value);
            }
        }
        let valsLength = vals.length;
        let addFactPromise;
        if(valsLength === 1){
            addFactPromise = contract.methods.addFact(vals[0].value);
        } else if (valsLength === 2){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value);
        } else if (valsLength === 3){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value);
        } else if (valsLength === 4){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value);
        } else if (valsLength === 5){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value);
        } else if (valsLength === 6){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value);
        } else if (valsLength === 7){
        addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value);
        } else if (valsLength === 8){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value);
        } else if (valsLength === 9){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value, vals[8].value);
        } else if (valsLength === 10){
            addFactPromise = contract.methods.addFact(vals[0].value, vals[1].value, vals[2].value, vals[3].value, vals[4].value, vals[5].value, vals[6].value, vals[7].value, vals[8].value, vals[9].value);
        }
        else {
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