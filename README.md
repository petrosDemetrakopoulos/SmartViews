# Smart-Views: A blockchain enabled OLAP Data Warehouse
This repository hosts the application server of Smart-Views.
 
The directory structure of the project is shown below.

The project is coded in Node.JS and uses the Ethereum Blockchain using the Web3.JS library.
```
├── README.md
├── config.json
├── config_lab.json
├── config_private.json
├── contracts
│   ├── ABCD.sol
│   ├── Cars.sol
│   ├── DataHandler.sol
│   ├── FactTable.sol
│   ├── Sales.sol
│   ├── SalesMin.sol
│   ├── sales_json.sol
│   └── sales_new.sol
├── helpers
│   ├── contractDeployer.js
│   ├── contractGenerator.js
│   ├── helper.js
│   └── transformations.js
├── index.js
├── package-lock.json
├── package.json
├── templates
│   ├── ABCD.json
│   ├── cars.json
│   ├── fact_tbl.json
│   ├── new_sales.json
│   ├── new_sales_min.json
│   └── sales.json
├── test_data
│   ├── 10
│   │   ├── 10fourcol.json
│   │   └── 10fourcol_b.json
│   ├── 100
│   │   ├── 100fourcol.json
│   │   └── 100fourcol_b.json
│   ├── 100K
│   │   └── 100kfourcol.json
│   ├── 10K
│   │   ├── 10kfourcol.json
│   │   └── 10kfourcol_b.json
│   ├── 150k
│   │   └── 150kfourcol.json
│   ├── 1K
│   │   ├── 1kfourcol.json
│   │   └── 1kfourcol_b.json
│   ├── 1M
│   │   └── 1Mfourcol.json
│   ├── 200
│   │   └── 200fourcol.json
│   ├── 200K
│   │   └── 200kfourcol.json
│   ├── 20K
│   │   └── 20kfourcol.json
│   ├── 250K
│   │   └── 250kfourcol.json
│   └── 30K
│       └── 30Kfourcol.json
├── test_scripts
│   ├── exp1.js
│   └── testDataGenerator.js
└── views
    ├── dashboard.ejs
    ├── form.ejs
    └── index.ejs
```
## Run instructions
After you clone the repo type ```npm install``` in the directory project and wait until all dependencies are installed. Then update th ```config.json``` file with the correct values for the fields ```redisPort```, ```redisIP```, ```blockchainIP``` and ```sql``` which are the most important ones in order to start the server.
The config file should look like this:
```
{
  "recordsSlice": 1000,
  "cacheEvictionPolicy": "FIFO",
  "maxCacheSize": 20,
  "cacheSlice": 400,
  "autoCacheSlice": "auto",
  "maxGbSize": 100,
  "redisPort": 6379,
  "redisIP": "127.0.0.1",
  "blockchainIP": "http://localhost:8545",
  "sql":  {
    "host": "localhost",
    "user": "sqlUser",
    "password": "yourPassword",
    "database": "yourDatabaseName"},
  "cacheEnabled": true
}
```
**The mySQL database must be created by you and it must be empty.**
It is mandatory as the server uses SQL to do all the calculations for the Group Bys and the merging.

## Before running the server
Before you run the server you must do 3 things:

1) Start mySQL server if not running
2) Start the eththereum blockchain simulator (ganache-cli)
    * In order to do that, open a terminal window and type the command: ```ganache-cli -e 8000000000000 -l 80000000000000```. -e and -l parameters are set to those values in oder to be sure tht the emulator has sufficient funds for the transactions we will perform.
    Of course, in order to perform this action ```ganache-cli``` must be already installed. If you have not yet istalled ```ganache-cli``` you can do so by typing ```npm install ganache-cli -g```.
3) Start ```redis-server```.  You can do this by simply typing ```redis-server``` in a terminal window. Again, be sure to have ```redis-server``` installed before you perform this action.

Now you can type ```node index.js``` in the project root directory and if everything id fine, the server will start immediately.
If everything is set up correct you should see the following lines in the terminal:
```
Smart-Views listening on http://localhost:3000/dashboard
Redis  connected
mySQL connected
``` 

#The server structure
The server structure is shown in the diagram below.
The server communicates with the Ethereum blockchain via the "Blockchain controller". 
The blockchain controller contains the functions that call the methods of a deployed smart contract and it then passes the responses to the API level.


![](structure.png)

# The Code
The back-end code is separated in 3 main categories.
1) **Helper functions** (in "helpers" directory)
2) **Smart contracts**, which are the Solidity contracts running over Ethereum blockchain (in "contracts" directory) and they are auto generated from the server.
3) **The API** (in "index.js" file) that we use to call smart contract methods.

The front-end code is much simpler and it is located under "views" directory. 
It contains the .ejs templates for the pages we use to call the API functions.


