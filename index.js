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
                    gas: 1500000,
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
    let constr = "\tconstructor() {\n" +
        "\t\tdataId = 0;\n" + "\t\tgroupId = 0;\n" +
        "\t}\n\n";
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
            retVals += "facts[id]." + crnProp.key + "facts[id].timestamp" +  ");\n\t}\n\n";
        } else {
            getParams += crnProp.data_type + " " + crnProp.key + ",";
            retVals += "facts[id]." + crnProp.key + ",";
        }
    }
    var retFact = "\t\treturn (" + retVals ;

    var addGroupBy = "\tfunction addGroupBy(string hash) public returns(string groupAdded, uint groupID){\n" +
        "    \t\tgroupBys[groupId].hash = hash;\n" +
        "    \t\tgroupBys[groupId].timestamp = now;\n" +
        "    \t\tgroupId += 1;\n" +
        "    \t\treturn (groupBys[groupId-1].hash, groupId-1);\n" +
        "    \t}\n\n";

    var getGroupBy = "\tfunction getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){\n" +
        "    \t\treturn(groupBys[idGroup].hash, groupBys[idGroup].timestamp);\n" +
        "    \t}\n\n";

    var getLatestGroupBy = "\tfunction getLatestGroupBy() public constant returns(string latestGroupBy, uint ts){\n" +
        "\t\tif(groupId > 0){\n" +
        "\t\t\treturn (groupBys[groupId-1].hash, groupBys[groupId-1].timestamp);\n" +
        "\t\t} else {\n" +
        "\t\t\treturn (\"\",0);\n" +
        "\t\t}\n" +
        "\t}\n\n";

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

    contrPayload = firstLine + secondLine + thirdLine + fourthLine +  constr + struct + properties + closeStruct + groupStruct + groupMapping +  mapping + addFact + setters + retStmt + getFact + getParams + retFact + addGroupBy + getGroupBy + getLatestGroupBy +   "\n}";
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
                console.log(result2);
               allFacts.push(result2)
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

app.get('/groupby/:field', function (req,res) {
    //LOGIC: IF latestGroupByTS >= latestFactTS RETURN LATEST GROUPBY FROM REDIS
    //      ELSE CALCULATE GROUBY FOR THE DELTAS (AKA THE ROWS ADDED AFTER THE LATEST GROUPBY) AND APPEND TO THE ALREADY SAVED IN REDIS
    if(contract) {
        contract.methods.dataId().call(function (err, latestId) {
            if(!err) {
                contract.methods.getLatestGroupBy().call(function (err, latestGroupBy) {
                    if(latestGroupBy.ts > 0) {
                        contract.methods.getFact(latestId).call(function (err, latestFact) {
                            if (latestGroupBy.ts >= latestFact.timestamp) {
                                console.log("getting it from redis");
                                client.get(latestGroupBy.hash, function (error, cachedGroupBy) {
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
                            }
                        }).catch(error => {
                            console.log(error);
                        });
                    } else {
                        //NO GROUP BY, SHOULD CALCULATE IT FROM THE BEGGINING
                        getAllFacts(latestId).then(retval => {
                            console.log(retval);
                            let groupByResult = groupBy(retval,req.params.field);
                            groupByResult = JSON.stringify(groupByResult);


                            md5sum.update(groupByResult);
                            var hash = md5sum.digest('hex');
                            console.log(hash);
                            client.set(hash, groupByResult, redis.print);

                            const transactionObject = {
                                from: acc,
                                gas: 1500000,
                                gasPrice: '30000000000000'
                            };

                            contract.methods.addGroupBy(hash).send(transactionObject,  (err, txHash) => {
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

                            // res.send(groupByResult);
                        }).catch(error => {
                            console.log(error);
                        });
                    }
                }).catch(error => {
                    console.log(error);
                });
            }else {
                console.log(err);
                console.log("ERRRRRR");
                res.send(err);
            }
        });


        contract.methods.dataId().call(function (err, result) {
            if(!err) {
                //async loop waiting to get all the facts separately
                getAllFacts(result).then(retval => {
                    console.log(retval);
                    let groupByResult = groupBy(retval,req.params.field);
                    groupByResult = JSON.stringify(groupByResult);


                    md5sum.update(groupByResult);
                    var hash = md5sum.digest('hex');
                    console.log(hash);
                    client.set(hash, groupByResult, redis.print);

                        const transactionObject = {
                            from: acc,
                            gas: 1500000,
                            gasPrice: '30000000000000'
                        };

                        contract.methods.addGroupBy(hash).send(transactionObject,  (err, txHash) => {
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

                   // res.send(groupByResult);
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
       // let productId = parseInt(req.body.productId,10);
      //  let quantity = parseInt(req.body.quantity,10);
      //  let customerId = parseInt(req.body.customerId,10);
        const transactionObject = {
            from: acc,
            gas: 1500000,
            gasPrice: '30000000000000'
        };
        console.log(req.body);
        let vals = req.body.values;
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