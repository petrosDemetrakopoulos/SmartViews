function sumObjects(ob1, ob2) {
    let sum = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                sum[key] = ob1[key] + ob2[key]
            }
        }
    });
    sum["operation"] = ob1["operation"];
    sum["field"] = ob1["field"];
    return sum;
}

function maxObjects(ob1, ob2) {
    let max = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                if(ob1[key] >= ob2[key]) {
                    max[key] = ob1[key];
                } else {
                    max[key] = ob2[key];
                }
            }
        }
    });
    max["operation"] = ob1["operation"];
    max["field"] = ob1["field"];
    return max;
}

function minObjects(ob1, ob2) {
    let min = {};

    Object.keys(ob1).forEach(key => {
        if(key !== "operation" && key !== "field") {
            if (ob2.hasOwnProperty(key)) {
                if(ob1[key] <= ob2[key]) {
                    min[key] = ob1[key];
                } else {
                    min[key] = ob2[key];
                }
            }
        }
    });
    min["operation"] = ob1["operation"];
    min["field"] = ob1["field"];
    return max;
}

module.exports = {
    sumObjects: sumObjects,
    maxObjects: maxObjects,
    minObjects: minObjects
};
