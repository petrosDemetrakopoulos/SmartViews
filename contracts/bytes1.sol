pragma solidity ^0.4.0;

contract _bytes1 {
	uint public dataId;
	uint public groupId;

	constructor() {
		dataId = 0;
		groupId = 0;
	}

	struct bytes_1{
		bytes32 key_1;
		uint timestamp;
	}
	struct groupBy{
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>bytes_1) public facts;

	function addFact(bytes32 key_1) public returns (bytes32 , uint ID){
		facts[dataId].key_1= key_1;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].key_1,dataId -1);
	}

	function getFact(uint id) public constant returns (bytes32 key_1, uint timestamp){
		return (facts[id].key_1, facts[id].timestamp);
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

	function getLatestGroupBy() public constant returns(string latestGroupBy, uint ts){
		if(groupId > 0){
			return (groupBys[groupId-1].hash, groupBys[groupId-1].timestamp);
		} else {
			return ("",0);
		}
	}


}