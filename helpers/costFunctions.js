const cacheController = require('../controllers/cacheController');
const helper = require('../helpers/helper');
const exec = require('child_process').execSync;
// this function  assigns a cost to each group by
function calculationCost (groupBys) {
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        crnGroupBy.cost = (0.5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function cacheEvictionCost (groupBys) {
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        crnGroupBy.cacheEvictionCost = (5 * crnGroupBy.columnSize) + (100000 / crnGroupBy.gbTimestamp);
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

function cacheEvictionCostOfficial (groupBys, latestFact, viewName, factTbl) { // the one written on paper , write one with only time or size
    let allHashes = [];
    let allGroupBys = [];
    let allGroupBys2 = [];
    let allMinus = [];
    let allFields = [];
    for (let i = 0; i < groupBys.length; i++) {
        allGroupBys.push(groupBys[i]);
        allGroupBys2.push(groupBys[i]);
        allFields.push(groupBys.columns);
    }
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        allHashes.push(crnGroupBy.hash);
    }
    cacheController.getManyCachedResults(allHashes, function (error, allCached) {
        if (error) {
            helper.log(error);
            return;
        }
        let freq = 0;

        allCached = allCached.filter(function (el) { // remove null objects in case they have been deleted
            return el != null;
        });
        if (allHashes.length > 1) {
            for (let j = 0; j < allCached.length; j++) {
                let crnGb = JSON.parse(allCached[j]);
                let viewsDefined = factTbl.views;
                for (let crnView in viewsDefined) {
                    if (factTbl.views[crnView].name === crnGb.viewName) {
                        freq = factTbl.views[crnView].frequency;
                        break;
                    }
                }
            }

            for (let i = 0; i < allGroupBys.length; i++) {
                allGroupBys2 = [];
                for (let j = 0; j < groupBys.length; j++) {
                    allGroupBys2.push(groupBys[j]);
                }
                let groupBysCachedExceptCrnOne = allGroupBys2.splice(1, i);
                let calcCostVfromVCache = calculationCostOfficial(allGroupBys, latestFact);
                let calcCostVfromVCacheMinusCrnView = calculationCostOfficial(groupBysCachedExceptCrnOne, latestFact);
                helper.log('ALL: ');
                helper.log(calcCostVfromVCache);
                helper.log('WITHOUT: ');
                helper.log(calcCostVfromVCacheMinusCrnView);
                allMinus.push(calcCostVfromVCacheMinusCrnView);
                let cost = 0;
                if (i > 0) {
                    let crnGB = calcCostVfromVCache[i];
                    for (let k = 0; k < calcCostVfromVCacheMinusCrnView.length; k++) {
                        let crnMinus = calcCostVfromVCacheMinusCrnView[k];
                        if (crnGB.id === crnMinus.id) {
                            cost = freq * (crnGB.calculationCost - crnMinus.calculationCost);
                            allGroupBys[i].cacheEvictionCost = cost;
                            helper.log('cost = ' + cost);
                        }
                    }
                } else {
                    allGroupBys[i].cacheEvictionCost = 1500;
                }
            }
            // για καθε ένα array που λείπει ένα cached view πρεπει να βρω ένα που έχει όλα τα άλλα views και να αφαιρέσω
        } else {
            allGroupBys[0].cacheEvictionCost = 1000;
        }
    });
    if (allGroupBys.length === 1) {
        allGroupBys[0].cacheEvictionCost = 1000;
    }
    return allGroupBys;
}

function calculationCostOfficial (groupBys, latestFact) { // the function we write on paper
    // where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    // which is the cost to materialize view V from view Vi (where V < Vi)
    let a = 10; // factor of deltas
    let sizeDeltas = 0;
    let sizeCached = 0;
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        sizeDeltas = latestFact - Number.parseInt(crnGroupBy.latestFact); // latestFact is the latest fact written in bc
        sizeCached = Number.parseInt(crnGroupBy.size);
        crnGroupBy.calculationCost = a * sizeDeltas + sizeCached;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

async function word2vec(groupBys, view) {
    let victims = [];
    let viewForW2V = view.gbFields.toString().replace(/,/g,"");
    for(let i = 0; i < groupBys.length; i++) {
        let currentFields= JSON.parse(groupBys[i].columns);
        let new_victim = currentFields.fields.toString().replace(/,/g,'').replace('""','');
        victims.push(new_victim);
    }
    let process = exec('python word2vec.py ' + victims.toString() + " " + viewForW2V);
    let sims = process.toString('utf8');
    sims = sims.replace('[','').replace(']','')
        .replace(/\n/g,'').trim().split(',');
    sims = sims.map(sim => {
        return sim.trim();
    });
    for(let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        crnGroupBy.word2vecScore = sims[i];
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

module.exports = {
    calculationCost: calculationCost,
    cacheEvictionCost: cacheEvictionCost,
    cacheEvictionCostOfficial: cacheEvictionCostOfficial,
    calculationCostOfficial: calculationCostOfficial,
    word2vec: word2vec
};
