const fs = require('fs');
const request = require('request');
const io = require('socket.io');
let numberOfFacts = 100;
let fileToSaveTestData = "testData_";
let crnRun  = 1;
let crnExpIteration = 1;
let numOfIterations = 5;
function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
function getRandomFloat(min, max) {
    return (Math.random() * (max - min + 1) + min).toFixed(2);
}
let all = [];
let allSecondRun = [];
    for(let j = 0; j < numberOfFacts; j++){
        let A = getRandomInt(0,100);
        let Asec = getRandomInt(0,100);
        let B = getRandomInt(0,100);
        let Bsec = getRandomInt(0,100);
        let C = getRandomInt(0,100);
        let Csec = getRandomInt(0,100);
        let D = getRandomFloat(0,100);
        let Dsec = getRandomInt(0,100);
        let newObj = {pk: j, A: A, B: B, C: C, D: D};
        let newObjSec = {pk: j + numberOfFacts, A: Asec, B: Bsec, C: Csec, D: Dsec};
        all.push(newObj);
        allSecondRun.push(newObjSec);
    }

fs.writeFile("testData/" + fileToSaveTestData + "1" + ".json", JSON.stringify(all), function(err) {
    if(err) {
        return console.log(err);
    }
    fs.writeFile("testData/" + fileToSaveTestData + "2" + ".json", JSON.stringify(allSecondRun), function(err) {
        if(err) {
            return console.log(err);
        }
    let url = "http://localhost:3000/load_dataset/" + fileToSaveTestData + "1" + ".json";
    let urlSec = "http://localhost:3000/load_dataset/" + fileToSaveTestData + "2" + ".json";
    let urlgbCountAB = "http://localhost:3000/getViewByName/A|B|C|D(COUNT)";
    console.log("The file was saved!");
    //experiment scenario begins
    //1) We add n records
    request({url: url, method: "GET"}, async function(error, httpResponse, body){
        console.log("***");
        if(error) {
            return console.log(error);
        }
        console.log("added first records");
        //2) We request group by A|B(COUNT)
        await request({url: urlgbCountAB, method: "GET"}, async function(error, httpResponse, body){
            console.log("###");
            if(error) {
                return console.log(error);
            }
            console.log("**");
            console.log("Run  1 completed");
            console.log("Number of facts = " + numberOfFacts);
            console.log("Time results (s)");
            console.log("------------");
           let JSONresp = JSON.parse(body);
            if("sqlTime" in JSONresp) {
                console.log("SQL Time: " + JSONresp.sqlTime.toFixed(5));
            }
            console.log("Blockchain Time: " + JSONresp.bcTime.toFixed(5));
            if("cacheSaveTime" in JSONresp) {
                console.log("Cache save Time: " + JSONresp.cacheSaveTime.toFixed(5));
            }
            if("cacheRetrieveTime" in JSONresp){
                console.log("Cache retrieve Time: " + JSONresp.cacheRetrieveTime.toFixed(5));
            }
            console.log("Total Time: " + JSONresp.totalTime.toFixed(5));
            console.log("All total time: " + JSONresp.allTotal.toFixed(5));
            console.log("**");
            //3) We add n records again
            await request({url: urlSec, method: "GET"}, async function(error, httpResponse, body) {
                console.log("%%%");
                if(error) {
                    return console.log(error);
                }
                console.log("added second records");
                //4) We request group by A|B(COUNT) again
                await request({url: urlgbCountAB, method: "GET"}, async function(error, httpResponse, body2){
                    console.log("@@@");
                    if(error) {
                        return console.log(error);
                    }
                    console.log("**");
                    console.log("Run  2 completed");
                    console.log("Number of facts = " + numberOfFacts);
                    console.log("Time results (s)");
                    console.log("------------");
                    let JSONresp = JSON.parse(body2);
                    if("sqlTime" in JSONresp) {
                        console.log("SQL Time: " + JSONresp.sqlTime.toFixed(5));
                    }
                    console.log("Blockchain Time: " + JSONresp.bcTime.toFixed(5));
                    if("cacheSaveTime" in JSONresp) {
                        console.log("Cache save Time: " + JSONresp.cacheSaveTime.toFixed(5));
                    }
                    if("cacheRetrieveTime" in JSONresp){
                        console.log("Cache retrieve Time: " + JSONresp.cacheRetrieveTime.toFixed(5));
                    }
                    console.log("Total Time: " + JSONresp.totalTime.toFixed(5));
                    console.log("All total time: " + JSONresp.allTotal.toFixed(5));
                    console.log("**");
                });
            });
        });
        });
    });
});

