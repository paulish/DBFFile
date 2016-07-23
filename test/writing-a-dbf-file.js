'use strict';
var path = require('path');
var _ = require('lodash');
var Promise = require('bluebird');
var rimraf = Promise.promisify(require('rimraf'));
var asyncawait_1 = require('asyncawait');
var chai_1 = require('chai');
var DBFFile = require('dbffile');
describe('Writing a DBF file', function () {
    var tests = [
        {
            filename: 'PYACFL.DBF',
            rowCount: 15,
            firstRow: { AFCLPD: 'W', AFHRPW: 2.92308, AFLVCL: 0.00, AFCRDA: new Date(1999, 2, 25), AFPSDS: '' }
        },
    ];
    rimraf(path.join(__dirname, "./fixtures/*.out"));
    tests.forEach(function (test) {
        it(test.filename, asyncawait_1.async.cps(function () {
            var srcPath = path.join(__dirname, "./fixtures/" + test.filename);
            var dstPath = path.join(__dirname, "./fixtures/" + test.filename + ".out");
            var srcDbf = asyncawait_1.await(DBFFile.open(srcPath));
            var dstDbf = asyncawait_1.await(DBFFile.create(dstPath, srcDbf.fields));
            var rows = asyncawait_1.await(srcDbf.readRecords(100));
            asyncawait_1.await(dstDbf.append(rows));
            dstDbf = asyncawait_1.await(DBFFile.open(dstPath));
            rows = asyncawait_1.await(dstDbf.readRecords(500));
            var firstRow = _.pick(rows[0], _.keys(test.firstRow));
            chai_1.expect(dstDbf.recordCount).equal(test.rowCount);
            chai_1.expect(firstRow).deep.equal(test.firstRow);
        }));
    });
});
//# sourceMappingURL=writing-a-dbf-file.js.map