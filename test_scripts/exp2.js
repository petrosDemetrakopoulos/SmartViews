const fs = require('fs');
let path = require('path');
let fileToSaveTestData = 'testData_';
const filename = './queriesEXP3.txt';
const generator = require('./testDataGenerator2');
const dir = '../test_data/';
const dreq = '100dir';
const Promise = require('promise');
const ResultsFile = 'resultsEXP1_DefaultCostFunction_8.txt';
const rp = require('request-promise');

const load = (file) => {
    let read = Promise.denodeify(fs.readFile);
    return read(path.resolve(__dirname, file), 'utf8');
};

const loadFiles = (directory, valid, error) => {
    return new Promise((resolve, reject) => {
        let result = '';
        fs.readdir(path.resolve(__dirname, directory), function (error, items) {
            if (error) {
                return console.log(error)
            } else {
                let filtered = items.filter(el => valid.includes(el));
                let listFiles = filtered.toString();
                items = listFiles.split(',');
                result = items;
                resolve(result);
            }
        });
    });
};

const loadData = async (fileno, queries) => {
    return new Promise((resolve, reject) => {
        let file = fileno;

        let url = 'http://localhost:3000/load_dataset/' + fileno;
        let urlGB = 'http://localhost:3000/getViewByName/' + queries + '(COUNT)/' + 'ABCD';
        console.log(queries[0]);
        console.log('urlGB: ' + urlGB);
        console.log('url: ' + url);
        let t = 12000 * 60 * 10;

        let options = {
            uri: url,
            timeout: t,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/38.0.2125.111 Safari/537.36', 'Connection': 'keep-alive'},
            json: true // Automatically parses the JSON string in the response
        };

        let options2 = {
            timeout:t,
            uri: urlGB,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/38.0.2125.111 Safari/537.36','Connection': 'keep-alive' },
            json: true // Automatically parses the JSON string in the response
        };

        rp(options)
            .then(function () {
                rp(options2)
                    .then((res) => handleResponse(res).then(
                        () => {
                            resolve();
                        }
                    ))
                    .catch(function (err) {
                        console.log(err);
                        reject(err);
                    });
            })
            .catch(function (err) {
                console.log(err);
                reject(err);
            });
    });
};

const handleResponse = async(body) => {
    return new Promise((resolve, reject) => {
        let f = body.toString().substr(String(body).indexOf('operation').toString());
        console.log(f);
        let JSONresp = JSON.parse(('{"' + f).toString());
        writeToFile(JSON.stringify(JSONresp), ResultsFile).then(() => {
            console.log(JSONresp);
            resolve()
        });
    });
};

const writeToFile = async(data, filepath) => {
    return new Promise((resolve, reject) => {
        let write = Promise.denodeify(fs.appendFile);
        const res = String(data + ',\n');
        console.log('result: ' + res);
        let writeFile = write(filepath, res);
        resolve(writeFile);
    });
};

const saveFile = (dataToWrite, outComeFilePath) => {
    writeToFile(dataToWrite, outComeFilePath)
        .then(() => console.log ('file' + outComeFilePath + 'saved successfully'))
        .catch((err) => console.log(err));
};

const jparse = function(filename, error) {
    fs.readFile(filename, function read(err, data) {
        if (err) {
            throw err;
        }
        let res = data;
        res = '[' + String(res) + ']';

        res = res.replace('},]', '}]');
        let jFile = JSON.parse(res);

        let blockchainArray = [];
        let cacheRetrieveArray = [];
        let cacheSaveArray = [];
        let totalArray = [];
        let allTotalArray = [];
        let sqlArray = [];

        for (let i = 0; i < jFile.length; i++) {
            let jObject = jFile[i];
            blockchainArray.push(jObject.bcTime);
            cacheRetrieveArray.push(jObject.cacheRetrieveTime);
            cacheSaveArray.push(jObject.cacheSaveTime);
            sqlArray.push(jObject.sqlTime);
            totalArray.push(jObject.totalTime);
        }

        for (let i = 0;i < totalArray.length; i++) {
            console.log('i: ' + i + ' ' + totalArray[i]);
        }
    });
};

const main = async() => {
    //jparse(ResultsFile);
    load(filename)
        .then(async(res) => {
            let fns = [];
            const queries = res.split(',');
            for (let i = 1; i <= 100; i++) {
                let crnFN =  await generator.generate(100 * (i-1),100 * i);
                fns.push(crnFN);
                // return array with filenames, then filter the ones read from the directory
            }
            loadFiles(dir, fns)
                .then(async(files) => {
                    for (let i = 0; i < queries.length; i++) {
                        await loadData(files[i], queries[i]).then(() => {
                            console.log('file ' + i +' loaded');
                        });
                    }
                });
        })
        .catch((err) => {
            console.log(err);
        })
};
main().then(() => {
    console.log("DONE")
});