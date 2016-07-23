'use strict';
var path = require('path');
var _ = require('lodash');
var asyncawait_1 = require('asyncawait');
var chai_1 = require('chai');
var DBFFile = require('dbffile');
describe('Reading a DBF file', function () {
    var tests = [
        {
            filename: 'PYACFL.DBF',
            rowCount: 45,
            firstRow: { AFCLPD: 'W', AFHRPW: 2.92308, AFLVCL: 0.00, AFCRDA: new Date(1999, 2, 25), AFPSDS: '' },
            delCount: 30,
            error: null
        },
        {
            filename: 'dbase_03.dbf',
            rowCount: null,
            firstRow: null,
            delCount: null,
            error: "Duplicate field name: 'Point_ID'"
        }
    ];
    tests.forEach(function (test) {
        it(test.filename, asyncawait_1.async.cps(function () {
            var filepath = path.join(__dirname, "./fixtures/" + test.filename);
            var expectedRows = test.rowCount;
            var expectedData = test.firstRow;
            var expectedDels = test.delCount;
            var expectedError = test.error;
            var actualRows = null;
            var actualData = null;
            var actualDels = null;
            var actualError = null;
            try {
                var dbf = asyncawait_1.await(DBFFile.open(filepath));
                var rows = asyncawait_1.await(dbf.readRecords(500));
                actualRows = dbf.recordCount;
                actualData = _.pick(rows[0], _.keys(expectedData));
                actualDels = dbf.recordCount - rows.length;
            }
            catch (ex) {
                actualError = ex.message;
            }
            if (expectedError || actualError) {
                chai_1.expect(actualError).equals(expectedError);
            }
            else {
                chai_1.expect(actualRows).equals(expectedRows);
                chai_1.expect(actualData).deep.equal(expectedData);
                chai_1.expect(actualDels).equals(expectedDels);
            }
        }));
    });
});
//# sourceMappingURL=reading-a-dbf-file.js.map