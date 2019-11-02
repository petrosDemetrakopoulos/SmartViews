const fs = require('fs');
const request = require('request');
let numberOfFacts = 10000;
let fileToSaveTestData = 'testData_';
const helper = require('../helpers/helper');

let all = [];
let allSecondRun = [];
let allThirdRun = [];
for (let j = 0; j < numberOfFacts; j++) {
    let A = helper.getRandomInt(0, 1000);
    let Asec = helper.getRandomInt(0, 1000);
    let Athird = helper.getRandomInt(0, 1000);
    let B = helper.getRandomInt(0, 1000);
    let Bsec = helper.getRandomInt(0, 1000);
    let Bthird = helper.getRandomInt(0, 1000);
    let C = helper.getRandomInt(0, 1000);
    let Csec = helper.getRandomInt(0, 1000);
    let Cthird = helper.getRandomInt(0, 1000);
    let D = helper.getRandomFloat(0, 1000);
    let Dsec = helper.getRandomInt(0, 1000);
    let Dthird = helper.getRandomInt(0, 1000);
    let newObj = { pk: j, A: A, B: B, C: C, D: D };
    let newObjSec = { pk: j + numberOfFacts, A: Asec, B: Bsec, C: Csec, D: Dsec };
    let newObjThird = { pk: j + numberOfFacts + numberOfFacts, A: Athird, B: Bthird, C: Cthird, D: Dthird };
    all.push(newObj);
    allSecondRun.push(newObjSec);
    allThirdRun.push(newObjThird);
}

fs.writeFile('test_data/' + fileToSaveTestData + '1' + '.json', JSON.stringify(all), function (err) {
    if (err) {
        return console.log(err);
    }
    fs.writeFile('test_data/' + fileToSaveTestData + '2' + '.json', JSON.stringify(allSecondRun), function (err) {
        if (err) {
            return console.log(err);
        }
        fs.writeFile('test_data/' + fileToSaveTestData + '3' + '.json', JSON.stringify(allThirdRun), function (err) {
            if (err) {
                return console.log(err);
            }
            let url = 'http://localhost:3000/load_dataset/' + fileToSaveTestData + '1' + '.json';
            let urlSec = 'http://localhost:3000/load_dataset/' + fileToSaveTestData + '2' + '.json';
            let urlThird = 'http://localhost:3000/load_dataset/' + fileToSaveTestData + '3' + '.json';
            let urlgbCountAB = 'http://localhost:3000/getViewByName/A|B(COUNT)';
            console.log('The file was saved!');
            // experiment scenario begins
            // 1) We add n records

            request({url: url, method: 'GET', headers: {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:70.0) Gecko/20100101 Firefox/70.0','Connection': 'keep-alive'
                }, timeout: 150000000}, async function (error, httpResponse, body) {
                if (error) {
                    return console.log(error);
                }
                console.log('added first records');
                // 2) We request group by A|B(COUNT)
                await request({ url: urlgbCountAB, method: 'GET',  headers: {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:70.0) Gecko/20100101 Firefox/70.0','Connection': 'keep-alive'
                    }, timeout: 150000000}, async function (error, httpResponse, body) {
                    if (error) {
                        return console.log(error);
                    }
                    console.log('Run  1 completed');
                    console.log('Number of facts = ' + numberOfFacts);
                    console.log('Time results (s)');
                    console.log('------------');
                    let JSONresp = JSON.parse(body);
                    if ('sqlTime' in JSONresp) {
                        console.log('SQL Time: ' + JSONresp.sqlTime.toFixed(5));
                    }
                    console.log('Blockchain Time: ' + JSONresp.bcTime.toFixed(5));
                    if ('cacheSaveTime' in JSONresp) {
                        console.log('Cache save Time: ' + JSONresp.cacheSaveTime.toFixed(5));
                    }
                    if ('cacheRetrieveTime' in JSONresp) {
                        console.log('Cache retrieve Time: ' + JSONresp.cacheRetrieveTime.toFixed(5));
                    }
                    console.log('Total Time: ' + JSONresp.totalTime.toFixed(5));
                    console.log('All total time: ' + JSONresp.allTotal.toFixed(5));
                    console.log('**');
                    // 3) We add n records again
                    await request({ url: urlSec, method: 'GET', headers: {
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:70.0) Gecko/20100101 Firefox/70.0',
                            'Connection': 'keep-alive'
                        }, timeout: 150000000 }, async function (error, httpResponse, body) {
                        console.log('%%%');
                        if (error) {
                            return console.log(error);
                        }
                        console.log('added second records');
                        // 4) We request group by A|B(COUNT) again
                        await request({ url: urlgbCountAB, method: 'GET', headers: {
                                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:70.0) Gecko/20100101 Firefox/70.0', 'Connection': 'keep-alive'
                            }, timeout: 150000000}, async function (error, httpResponse, body2) {
                            if (error) {
                                return console.log(error);
                            }
                            console.log('Run  2 completed');
                            console.log('Number of facts = ' + numberOfFacts);
                            console.log('Time results (s)');
                            console.log('------------');
                            let JSONresp = JSON.parse(body2);
                            if ('sqlTime' in JSONresp) {
                                console.log('SQL Time: ' + JSONresp.sqlTime.toFixed(5));
                            }
                            console.log('Blockchain Time: ' + JSONresp.bcTime.toFixed(5));
                            if ('cacheSaveTime' in JSONresp) {
                                console.log('Cache save Time: ' + JSONresp.cacheSaveTime.toFixed(5));
                            }
                            if ('cacheRetrieveTime' in JSONresp) {
                                console.log('Cache retrieve Time: ' + JSONresp.cacheRetrieveTime.toFixed(5));
                            }
                            console.log('Total Time: ' + JSONresp.totalTime.toFixed(5));
                            console.log('All total time: ' + JSONresp.allTotal.toFixed(5));
                        });
                    });
                });
            });
        });
    });
});
