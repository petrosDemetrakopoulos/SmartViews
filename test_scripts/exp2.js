const fs = require('fs');
let path = require('path');
const filename = './EXPViewSequence.txt';
const generator = require('./testDataGenerator');
const dir = '../test_data/benchmarks/';
const Promise = require('promise');
const ResultsFile = 'result_final_DefaultCostFunction_450.json';
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
        let url = 'http://localhost:3000/load_dataset/' + fileno;
        let urlGB = 'http://localhost:3000/getViewByName/' + queries + '(COUNT)/' + 'ABCDE';
        console.log(queries[0]);
        console.log('urlGB: ' + urlGB);
        console.log('url: ' + url);
        let t = 12000 * 60 * 10;

        let options = {
            uri: url,
            timeout: t,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/38.0.2125.111 Safari/537.36',
                'Connection': 'keep-alive' },
            json: true // Automatically parses the JSON string in the response
        };

        let options2 = {
            timeout: t,
            uri: urlGB,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/38.0.2125.111 Safari/537.36',
                'Connection': 'keep-alive' },
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

const handleResponse = async (body) => {
    return new Promise((resolve, reject) => {
        let f = body.toString().substr(String(body).indexOf('operation').toString());
        console.log(body);
        let JSONresp = JSON.parse(('{"' + f).toString());
        writeToFile(JSON.stringify(JSONresp), ResultsFile).then(() => {
            console.log(JSONresp);
            resolve()
        });
    });
};

const writeToFile = async (data, filepath) => {
    return new Promise((resolve) => {
        let write = Promise.denodeify(fs.appendFile);
        const res = String(data + ',\n');
        console.log('result: ' + res);
        let writeFile = write(filepath, res);
        resolve(writeFile);
    });
};

const main = async () => {
    return new Promise((resolve, reject) => {
        load(filename)
            .then(async (res) => {
                let fns = [];
                const queries = res.split(',');
                for (let i = 1; i <= 100; i++) {
                    let crnFN = await generator.generate(100 * (i - 1), 100 * i);
                    fns.push(crnFN);
                    // return array with filenames, then filter the ones read from the directory
                }
                loadFiles(dir, fns)
                    .then(async (files) => {
                        for (let i = 0; i < queries.length; i++) {
                            if (files[i]) { // guard for possible undefined value in the files array
                                await loadData(files[i], queries[i]).then(() => {
                                    console.log('file ' + i + ' loaded');
                                });
                            }
                        }
                        resolve();
                    });
            })
            .catch((err) => {
                console.log(err);
                reject(err);
            })
    });
};
main().then(() => {
    console.log('DONE');
    process.exit();
}).catch((err) => {
    console.log(err);
    process.exit();
});

module.exports = { main: main };
