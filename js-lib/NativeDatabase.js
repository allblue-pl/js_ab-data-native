'use strict';

const
    abData = require('ab-data'),
    abLock = require('ab-lock'),
    abNative = require('ab-native'),
    js0 = require('js0'),

    Database = require('ab-database-native').Database
;

export default class NativeDatabase {
    static async CreateDatabaseInfo_Async(db, transactionId = null) {
        js0.args(arguments, Database, [ 'int', js0.Null, 
                js0.Default ]);

        let dbVer = new abData.DatabaseVersion('sqlite', [ 0, 0, 0]);
        let databaseInfo = new abData.DatabaseInfo(dbVer);

        let tableNames = await db.getTableNames_Async(transactionId);
        for (let tableName of tableNames) {
            let tableInfo = new abData.TableInfo(tableName);

            let columnInfos = await db.getTableColumnInfos_Async(tableName, 
                    transactionId);
            for (let columnInfo of columnInfos) {
                tableInfo.addFieldInfo(new abData.FieldInfo(
                    columnInfo.name,
                    columnInfo.type.toLowerCase(),
                    columnInfo.notNull,
                    '',
                ));
            }

            let indexInfos = await db.getTableIndexInfos_Async(tableName,
                    transactionId);
            for (let indexInfo of indexInfos) {
                let indexColumnInfos = await db.getIndexColumnInfos_Async(
                        indexInfo.name, transactionId);
                if (indexInfo.isPK) {
                    let pks = [];
                    for (let indexColumnInfo of indexColumnInfos)
                        pks.push(indexColumnInfo.name);
                    tableInfo.setPKs(pks);
                } else {
                    let abIndexInfo = new abData.IndexInfo();
                    for (let indexColumnInfo of indexColumnInfos) {
                        abIndexInfo.addColumnInfo(indexColumnInfo.seq, 
                                indexColumnInfo.name, indexColumnInfo.desc);
                    }
                    tableInfo.addIndexInfo(indexInfo.name, abIndexInfo);
                }
            }

            databaseInfo.addTableInfo(tableInfo);
        }
        
        return databaseInfo;
    }
}