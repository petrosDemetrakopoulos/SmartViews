const fs = require('fs');

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
function getRandomFloat(min, max) {
    return (Math.random() * (max - min + 1) + min).toFixed(2);
}
let k = 0;
let all = [];
for(let i =0; i < 20; i++){
    for(let j = 0; j < 1000; j++){
        let A = getRandomInt(0,100);
        let B = getRandomInt(0,100);
        let C = getRandomInt(0,100);
        let D = getRandomFloat(0,100);
        let newObj = {pk: k, A: A, B: B, C: C, D: D};
        all.push(newObj);
        console.log(k);
        k++;
    }
}
fs.writeFile("testData/20kfourcol.json", JSON.stringify(all), function(err) {
    if(err) {
        return console.log(err);
    }
    console.log("The file was saved!");
});