const cacheController = require('../controllers/cacheController');
const exec = require('child_process').execSync;
const _ = require('underscore');

function cost (Vi, V, latestFact) {
    // The cost of materializing view V using the cached view Vi
    const sizeDeltas = latestFact - Number.parseInt(Vi.latestFact); // latestFact is the latest fact written in bc
    const sizeCached = Number.parseInt(Vi.size);
    V.calculationCost = 500 * sizeDeltas + sizeCached;
    return V;
}

function costMat (V, Vc, latestFact) {
    // The cost of materializing view V using the set of cached views Vc
    //console.log('costMat Function')
    //console.log('cost of materializing view V'+V.columns+' using VC, size: '+Vc.length)
    let costs = [];
    for (let i = 0; i < Vc.length; i++) {
        let Vi = Vc[i];
        if (isMaterializableFrom(V,Vi)) {
            const sizeDeltas = latestFact - Number.parseInt(Vi.latestFact); // latestFact is the latest fact written in bc
            const sizeCached = Number.parseInt(Vi.size);
            V.calculationCost = 500 * sizeDeltas + sizeCached;
            costs.push(V);
        }
    }
    costs.sort((a, b) => parseFloat(a.calculationCost) - parseFloat(b.calculationCost));
    console.log('costMat result: ' + costs[0].calculationCost);
    return costs[0].calculationCost;
}

function remove (array, element) {
    return array.filter(el => el !== element);
}

async function dispCost (Vc, latestFact, factTbl) {
    return new Promise((resolve, reject) => {
        let allHashes = [];
        let toBeEvicted = [];
        for (let i = 0; i < Vc.length; i++) {
            const crnGroupBy = Vc[i];
            allHashes.push(crnGroupBy.hash);
        }
        //console.log('inside DispCost')
        cacheController.getManyCachedResults(allHashes).then(async allCached => {
            let freq = 0;
            allCached = allCached.filter(function (el) { // remove null objects in case they have been deleted
                return el != null;
            });

            if (allHashes.length > 1) {
                for (let j = 0; j < allCached.length; j++) {
                    const crnGb = JSON.parse(allCached[j]);
                    const viewsDefined = factTbl.views;
                    for (let crnView in viewsDefined) {
                        if (factTbl.views[crnView].name === crnGb.viewName) {
                            freq = factTbl.views[crnView].frequency;
                            break;
                        }
                    }
                }

                for (let i = 0; i < Vc.length; i++) {
                    let Vi = Vc[i]; //Vi in paper
                    let VcMinusVi = remove(Vc, Vi); //set of cached views without Vi
                    //console.log('current Vi: '+Vi.fields)
                    let viewsMaterialisableFromVi = getViewsMaterialisableFromVi(Vc, Vi, i);
                    viewsMaterialisableFromVi = remove(viewsMaterialisableFromVi, Vi);
                    let dispCostVi = 0;
                    for (let j = 0; j < viewsMaterialisableFromVi.length; j++) {
                        let V = viewsMaterialisableFromVi[j];
                        let costMatVVC = costMat(V, Vc, latestFact);
                        let costMatVVcMinusVi = costMat(V, VcMinusVi, latestFact);
                        dispCostVi += (costMatVVC - costMatVVcMinusVi);
                        console.log('current Vi: '+Vi.columns+'costMatWC: '+costMatVVC+' CostMatWcMinusVi: '+costMatVVcMinusVi+' result: '+dispCostVi+' frequency: '+freq)
//prepei na tsekarw an ta views sta 2 costs mporoun na kanoun materialize to Vi
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
        //console.log('Vi fields: '+ViFields)
        for (let index in crnViewFields.fields) {
            crnViewFields.fields[index] = crnViewFields.fields[index].trim();
            //console.log('Diff fields: '+crnViewFields.fields[index])
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
    //ViF = JSON.parse(Vi.columns);
    //console.log('Views materializable from Vi :'+ViF.fields)
    //for(i=0;i<viewsMaterialisableFromVi.length;i++){
    //  console.log(viewsMaterialisableFromVi[i].columns)
    //}
    return viewsMaterialisableFromVi;
}

function calculationCostOfficial (groupBys, latestFact) { // the function we write on paper
    // where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    // which is the cost to materialize view V from view Vi (where V < Vi)
    const a = 500; // factor of deltas
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
    const viewForW2V = view.fields.toString().replace(/,/g, '');
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

//definition of cube distance
//For two views V1,V2 we can define their distance in data cube lattice
//as the number of roll-ups, drill-downs needed to go from V1 to V2.

//DataCubeDistance('ABC','AB'))=1 { ΑBC --rollup--> AB }
//DataCubeDistance('ABC','ACD')=2   {ABC ->AC->ACD }
//DataCubeDistance('ABC','DE) = 5 {ABC->AB->A->()->D->DE}

//dim(V)= set of dimensions in view V
//DataCubeDistance(x,y) = |dim(x) UNION dim(y) - dim(x) INTERSECTION dim(y)|

function dataCubeDistance (view1, view2) {
    let view1fields = JSON.parse(view1.columns);
    let view2fields = view2.fields;
    view1fields = view1fields.fields;
    const union = _.union(view1fields, view2fields).sort();
    const intersection = _.intersection(view1fields, view2fields).sort();
    return union.length - intersection.length;
}

function dataCubeDistanceBatch (cachedViews, view) {
    for (let i = 0; i < cachedViews.length; i++) {
        cachedViews[i].dataCubeDistance = dataCubeDistance(cachedViews[i], view);
    }
    return cachedViews;
}

function isMaterializableFrom (view1, view2) {
    //check if view2 can materialize view1 ex. isMaterializableFrom('AB','ABC')=true
    let view1Fields = JSON.parse(view1.columns);
    let view2Fields = JSON.parse(view2.columns);
    //console.log('IsMaterializableFrom function')
    //console.log('Vi fields: '+ViFields)
    for (let index in view1Fields.fields) {
        view1Fields.fields[index] = view1Fields.fields[index].trim();
    }
    let containsAllFields = true;
    for (let k = 0; k < view1Fields.fields.length; k++) {
        if (!view2Fields.fields.includes(view1Fields.fields[k])) {
            containsAllFields = false
        }
    }
    return containsAllFields;
}

module.exports = {
    dispCost: dispCost,
    calculationCostOfficial: calculationCostOfficial,
    word2vec: word2vec,
    dataCubeDistanceBatch: dataCubeDistanceBatch
};