const express = require('express');
const bodyParser = require('body-parser');
const solc = require('solc');
const fs = require('fs');
const delay = require('delay');
const groupBy = require('group-by');
let fact_tbl = require('./templates/fact_tbl');
abiDecoder = require('abi-decoder');
const app = express();
var jsonParser = bodyParser.json();
app.use(jsonParser);

app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');

const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));
let contractInstance = null;


web3.eth.defaultAccount = web3.eth.accounts[0];
let contract = null;
let DataHandler = null;
let acc = null;
app.get('/', function (req,res) {
    fs.readdir('./templates', function(err, items) {
        console.log(items);
        for (var i=0; i<items.length; i++) {
            console.log(items[i]);
        }
        res.render("index",{"templates":items});
    });
});

app.listen(3000, () => console.log(`Example app listening on http://localhost:3000`));

async function deploy(account, contractPath){
            console.log(contractPath);
            const input = fs.readFileSync(contractPath);
            console.log(input);
            const output = solc.compile(input.toString(), 1);
            console.log(output);
            const bytecode = output.contracts[Object.keys(output.contracts)[0]].bytecode;
            const abi = JSON.parse(output.contracts[Object.keys(output.contracts)[0]].interface);
            console.log(abi);

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
    let thirdLine = "\tuint public dataId;\n\n";
    let constr = "\tconstructor() {\n" +
        "\t\tdataId = 0;\n" +
        "\t}\n\n";
    var properties = "";
    var struct = "\tstruct " + fact_tbl.struct_Name + "{ \n";
    for(var i =0; i < fact_tbl.properties.length; i++){
        let crnProp = fact_tbl.properties[i];
        properties += "\t\t" + crnProp.data_type + " " + crnProp.key + ";\n";
    }
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
            getParams += crnProp.data_type + " " + crnProp.key + "){\n";
            retVals += "facts[id]." + crnProp.key + ");\n\t}"
        } else {
            getParams += crnProp.data_type + " " + crnProp.key + ",";
            retVals += "facts[id]." + crnProp.key + ","
        }
    }
    var retFact = "\t\treturn (" + retVals;

    contrPayload = firstLine + secondLine + thirdLine + constr + struct + properties + closeStruct + mapping + addFact + setters + retStmt + getFact + getParams + retFact +  "\n}";
    fs.writeFile("contracts/" + fact_tbl.name + ".sol", contrPayload, function(err) {
        if(err) {
            res.send({msg:"error"});
            return console.log(err);
        }
        console.log("The file was saved!");
        res.send({msg:"OK","filename":fact_tbl.name + ".sol"});
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
                delete result2["0"];
                delete result2["1"];
                delete result2["2"];
                delete result2["3"];
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
    if(contract) {
        contract.methods.dataId().call(function (err, result) {
            if(!err) {
                //async loop waiting to get all the facts separately
                getAllFacts(result).then(retval => {
                    console.log(retval);
                    let groupByResult = {};
                    if(req.params.field === 'product'){
                        groupByResult = groupBy(retval,'productId');
                    } else if(req.params.field === 'customer'){
                        groupByResult = groupBy(retval,'customer');
                    } else {
                        groupByResult = 'error';
                    }
                    groupByResult = JSON.stringify(groupByResult);
                    //call contract function to store groupBy
                        const transactionObject = {
                            from: acc,
                            gas: 1500000,
                            gasPrice: '30000000000000'
                        };

                        contract.methods.addGroupBy(groupByResult).send(transactionObject,  (err, txHash) => {
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
        let productId = parseInt(req.body.productId,10);
        let quantity = parseInt(req.body.quantity,10);
        let customerId = parseInt(req.body.customerId,10);
        const transactionObject = {
            from: acc,
            gas: 1500000,
            gasPrice: '30000000000000'
        };

        contract.methods.addFact(productId, quantity, customerId).send(transactionObject,  (err, txHash) => {
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