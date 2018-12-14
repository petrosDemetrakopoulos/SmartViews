pragma solidity ^0.4.0;

contract FactTable { 
	uint public dataId;

	constructor() {
		dataId = 0;
	}

	struct Fact{ 
		uint key_1;
		string key_2;
		uint timestamp;
	}
	mapping(uint =>Fact) public facts;

	function addFact(uint key_1,string key_2) public returns (uint ,string , uint ID){
		facts[dataId].key_1= key_1;
		facts[dataId].key_2= key_2;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].key_1,facts[dataId-1].key_2,dataId -1);
	}

	function getFact(uint id) public constant returns (uint key_1,string key_2){
		return (facts[id].key_1,facts[id].key_2);
	}
}