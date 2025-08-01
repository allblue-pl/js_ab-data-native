'use strict';

const
    js0 = require('js0')
;

export default class Wrapper {
    constructor(dataStore, db) {
        js0.args(arguments, require('../DataStore'), require('./Database'));

        this.dataStore = dataStore;
        this.db = db;

        let nad = new abNative.ActionsSetDef()
            .addWeb('Table_Delete', {
                tableName: 'string',
                where: Array,
                transactionId: [ 'int', js0.Null ],
            }, {
                error: [ js0.Null, 'string' ],
            }, async (args) => {
                if (!this.db.initialized)
                    await this.db.init_Async();
                
                try {
                    let table = this.dataStore.getTable(args.tableName);
                    await table.delete_Async(this.dataStore.db, 
                            { where: args.where }, args.transactionId);

                    return {
                        error: null,
                    };
                } catch (e) {
                    console.error('ABData Wrapper', e);

                    return {
                        error: e.message,
                    }
                }
            })
            .addWeb('Table_Select', {
                tableName: 'string',
                args: js0.RawObject,
                transactionId: [ 'int', js0.Null ],
            }, {
                rows: [ js0.Null, Array ],
                error: [ js0.Null, 'string' ],
            }, async (args) => {
                if (!this.db.initialized)
                    await this.db.init_Async();
                
                try {
                    let table = this.dataStore.getTable(args.tableName);
                    let rows = await table.select_Async(this.dataStore.db,
                            args.args, args.transactionId);

                    return {
                        rows: rows,
                        error: null,
                    };
                } catch (e) {
                    console.error('ABData Wrapper', e);

                    return {
                        rows: null,
                        error: e.message,
                    }
                }
            })
            .addWeb('Table_Update', {
                tableName: 'string',
                rows: js0.ArrayItems(js0.RawObject),
                transactionId: [ 'int', js0.Null ],
            }, {
                error: [ js0.Null, 'string' ],
            }, async (args) => {
                if (!this.db.initialized)
                    await this.db.init_Async();
                
                try {
                    let table = this.dataStore.getTable(args.tableName);
                    await table.update_Async(this.dataStore.db, args.rows, 
                            args.transactionId);

                    return {
                        error: null,
                    };
                } catch (e) {
                    console.error('ABData Wrapper', e);

                    return {
                        error: e.message,
                    }
                }
            });
        
        this.nativeActions = abNative.addActionsSet('ABData', nad);
    }
}