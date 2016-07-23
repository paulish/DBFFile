var DBFFile = require('/Users/emmanuel/bachelor/DBFFile/dbf-file.js');



DBFFile.open('/Users/emmanuel/bachelor/young.dbf', "base64")
    .then(dbf => {
       // console.log(`DBF file contains ${dbf.recordCount} rows.`);
        console.log(`Field names: ${dbf.fields.map(f => f.name)}`);
        return dbf.readRecords(100);
    })
    .then(rows => rows.forEach(row => row))
    .catch(err => console.log('An error occurred: ' + err));

 //automatic memo
 //encoding
 //no more raw