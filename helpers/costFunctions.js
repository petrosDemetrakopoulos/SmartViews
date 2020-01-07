const cacheController = require('../controllers/cacheController');
const helper = require('../helpers/helper');
const exec = require('child_process').execSync;

function cost (Vi, V, latestFact) {
    // The cost of materializing view V using the cached view Vi
    let sizeDeltas = latestFact - Number.parseInt(Vi.latestFact); // latestFact is the latest fact written in bc
    let sizeCached = Number.parseInt(Vi.size);
    V.calculationCost = 500 * sizeDeltas + sizeCached;
    return V;
}

async function costMat(V, Vc, latestFact) {
    // The cost of materializing view V using the set of cached views Vc
    let costs = [];
    for (let i = 0; i < Vc.length; i++) {
        let Vi = Vc[i];
        let crnCost = await cost(Vi, V, latestFact);
        costs.push(crnCost);
    }
    await costs.sort(function (a, b) {
        return parseFloat(a.calculationCost) - parseFloat(b.calculationCost);
    });
    return costs[0].calculationCost;
}

function remove (array, element) {
    return array.filter(el => el !== element);
}

async function dispCost (Vc, latestFact, factTbl) {
    return new Promise((resolve, reject) =>  {
        let allHashes = [];
        let toBeEvicted = [];
        for (let i = 0; i < Vc.length; i++) {
            let crnGroupBy = Vc[i];
            allHashes.push(crnGroupBy.hash);
        }
        cacheController.getManyCachedResults(allHashes).then(async allCached => {
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

                for (let i = 0; i < Vc.length; i++) {
                    let Vi = Vc[i];
                    let VcMinusVi = remove(Vc, Vi);
                    let viewsMaterialisableFromVi = getViewsMaterialisableFromVi(Vc, Vi, i);
                    viewsMaterialisableFromVi = remove(viewsMaterialisableFromVi, Vi);
                    let dispCostVi = 0;
                    for (let j = 0; j < viewsMaterialisableFromVi.length; j++) {
                        let V = viewsMaterialisableFromVi[j];
                        let costMatVVC = await costMat(V, Vc, latestFact);
                        let costMatVVcMinusVi = await costMat(V, VcMinusVi, latestFact);
                        dispCostVi += (costMatVVC - costMatVVcMinusVi);
                    }
                    dispCostVi = dispCostVi * freq;
                    Vi.cacheEvictionCost = dispCostVi / Number.parseInt(Vi.size);
                    toBeEvicted.push(Vi);
                }
            }
            resolve(toBeEvicted);
        }).catch(err => {
            reject(err)
        });
    });
}

function getViewsMaterialisableFromVi (Vc, Vi) {
    let viewsMaterialisableFromVi = [];
    for (let j = 0; j < Vc.length; j++) { // finding all the Vs < Vi
            let crnView = Vc[j];
            let crnViewFields = JSON.parse(crnView.columns);
            let ViFields = JSON.parse(Vi.columns);
            for (let index in crnViewFields.fields) {
                crnViewFields.fields[index] = crnViewFields.fields[index].trim();
            }
            let containsAllFields = true;
            for (let k = 0; k < crnViewFields.fields.length; k++) {
                if (!ViFields.fields.includes(crnViewFields.fields[k])) {
                    containsAllFields = false
                }
            }
            if (containsAllFields) {
                viewsMaterialisableFromVi.push(crnView);
            }
    }
    return viewsMaterialisableFromVi;
}

function calculationCostOfficial (groupBys, latestFact) { // the function we write on paper
    // where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    // which is the cost to materialize view V from view Vi (where V < Vi)
    let a = 500; // factor of deltas
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

async function word2vec (groupBys, view) {
    let victims = [];
    let viewForW2V = view.gbFields.toString().replace(/,/g, '');
    for (let i = 0; i < groupBys.length; i++) {
        let currentFields = JSON.parse(groupBys[i].columns);
        let newVictim = currentFields.fields.toString()
            .replace(/,/g, '')
            .replace('""', '');
        victims.push(newVictim);
    }
    let process = exec('python word2vec.py ' + victims.toString() + ' ' + viewForW2V);
    let sims = process.toString('utf8');
    sims = sims.replace('[', '').replace(']', '')
        .replace(/\n/g, '').trim().split(',');
    sims = sims.map(sim => {
        return sim.trim();
    });
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        crnGroupBy.word2vecScore = sims[i];
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

module.exports = {
    dispCost: dispCost,
    calculationCostOfficial: calculationCostOfficial,
    word2vec: word2vec
};
