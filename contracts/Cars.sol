pragma solidity ^0.4.0;

contract Cars { 
	uint public dataId;
	uint public groupId;

	constructor() {
		dataId = 0;
		groupId = 0;
	}

	struct Car{ 
		string brand;
		string model;
		string year;
		string category;
		uint cylinders;
		uint timestamp;
	}
	struct groupBy{ 
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>Car) public facts;

	function addFact(string brand,string model,string year,string category,uint cylinders) public returns (string ,string ,string ,string ,uint , uint ID){
		facts[dataId].brand= brand;
		facts[dataId].model= model;
		facts[dataId].year= year;
		facts[dataId].category= category;
		facts[dataId].cylinders= cylinders;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].brand,facts[dataId-1].model,facts[dataId-1].year,facts[dataId-1].category,facts[dataId-1].cylinders,dataId -1);
	}

	function getFact(uint id) public constant returns (string brand,string model,string year,string category,uint cylinders){
		return (facts[id].brand,facts[id].model,facts[id].year,facts[id].category,facts[id].cylinders);
	}

	function addGroupBy(string hash) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].hash = hash;
    		groupBys[groupId].timestamp = now;
    		groupId += 1;
    		return (groupBys[groupId-1].hash, groupId-1);
    	}

	function getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){
    		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp);
    	}

	function getLatestGroupByTimestamp() public returns(uint ts){
		if(groupId > 0){
			return groupBys[groupId-1].timestamp;
		} else {
			return 0;
		}
	}

	function getLatestFactTimestamp() public returns(uint ts){
		if(dataId > 0){
			return facts[dataId-1].timestamp;
		} else {
			return 0;
		}
	}


}