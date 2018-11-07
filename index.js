const express = require('express');
const bodyParser = require('body-parser');
const solc = require('solc');
const fs = require('fs');
const delay = require('delay');
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
var DataHandler = null;
let acc = null;

app.get('/', function (req,res) {
    res.render("index");
});

app.listen(3000, () => console.log(`Example app listening on http://localhost:3000`));

async function deploy(account){
            const input = fs.readFileSync('./contracts/DataHandler.sol');
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

app.get('/deployContract', function (req, res) {

    web3.eth.getAccounts(function (err,accounts) {
        if (!err) {
        acc = accounts[1];
            deploy(accounts[0])
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

app.get('/getallfacts', function (req,res) {
    if(contract) {
        contract.methods.dataId().call(function (err, result) {
            if(!err) {
              //async loop waiting to get all the facts separately
                contract.methods.facts(0).call(function (errr, result2) {
                    if(!errr){
                        res.send(result2);
                    } else {
                        console.log(errr);
                        console.log("ERRRRRR");
                        res.send(errr);
                    }
                })
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