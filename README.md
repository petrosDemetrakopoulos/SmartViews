# Smart-Views: A blockchain enabled OLAP Data Warehouse
This repository hosts the application server of Smart-Views.
 
The directory structure of the project is shown below.

The project is coded in Node.JS and uses the Ethereum Blockchain using the Web3.JS library.
```
├── README.md
├── config.json
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
│   ├── 100K
│   │   └── 100kfourcol.json
│   ├── 100fourcol.json
│   ├── 100fourcol_b.json
│   ├── 10fourcol.json
│   ├── 10fourcol_b.json
│   ├── 10kfourcol.json
│   ├── 10kfourcol_b.json
│   ├── 150k
│   │   └── 150kfourcol.json
│   ├── 1Mfourcol.json
│   ├── 1kfourcol.json
│   ├── 1kfourcol_b.json
│   ├── 200k
│   │   └── 200kfourcol.json
│   ├── 20k
│   │   └── 20kfourcol.json
│   ├── 20kfourcol.json
│   ├── 250k
│   │   └── 250kfourcol.json
│   ├── testData_1.json
│   └── testData_2.json
├── test_scripts
│   ├── exp1.js
│   └── testDataGenerator.js
└── views
    ├── dashboard.ejs
    ├── form.ejs
    └── index.ejs
```
