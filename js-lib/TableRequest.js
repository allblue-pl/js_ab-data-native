'use strict';

const
    abData = require('ab-data'),
    js0 = require('js0'),

    Database = require('ab-database-native').Database,
    
    Request = require('./Request'),
    Table = require('./Table')
;

export default class TableRequest extends Request {
    static async Delete_Async(device, db, table, args, transactionId = null) {
        js0.args(arguments, require('./NativeDevice'), Database, 
                require('../Table'), js0.RawObject, [ 'int', js0.Null, 
                js0.Default ]);
        js0.typeE(args, js0.Preset(abData.TableRequestDef.Args_Select()));

        await table.delete_Async(db, args, transactionId);
    }

    static async Update_Async(device, db, table, rows, transactionId = null) {
        js0.args(arguments, require('./NativeDevice'), Database, 
                require('../Table'), Array, [ 'int', js0.Null, js0.Default ]);

        for (let row of rows) {
            if (!('_Id' in row))
                continue;

            row['_Modified_DateTime'] = null;

            if (device.isNewId(row._Id))
                device.useId(row._Id);
        }

        await table.update_Async(db, rows, transactionId);
    }


    constructor(db, table) {
        js0.args(arguments, Database, require('./Table'));
        super();

        this._db = db;
        this._table = table;

        this.setA('delete', async (args) => {
            return await this._action_Delete_Async(args);
        });
        this.setA('row', async (args) => {
            return await this._action_Row_Async(args);
        });
        this.setA('select', async (args) => {
            return this._action_Select_Async(args);
        });
        this.setA('set', async (args) => {
            return await this._action_Set_Async(args);
        });
        this.setA('update', async (args) => {
            return await this._action_Update_Async(args);
        });
    }

    async _action_Delete_Async(args) {
        let result = {
            success: (await this._table.delete_Async(this._db, {
                where: args.where,
            })).success,
            error: null,
        };  

        if (!result.success) {
            result.error = spocky.Debug ?
                    this._db.getError() :
                    'Cannot delete rows.';
        }

        return result;
    }

    async _action_Row_Async(args) {
        js0.args(arguments, js0.Preset({
            columns: null,
            limit: [ js0.Null, js0.PresetArray([ 'int', 'int' ]), js0.Default(null) ],
            where: [ Array, js0.Default([]) ],
        }));

        args.limit = [ 0, 1 ];

        let result = await this._table.select_Async(this._db, args);

        let row = null;
        if (result.rows !== null) {
            if (result.rows.length > 0)
                row = result.rows[0];
        }

        return {
            success: result.error === null,
            row: row,
            error: result.error,
        };
    }

    async _action_Select_Async(args) {
        js0.args(arguments, js0.Preset(abData.TableRequestDef.Args_Select()));

        let result = await this._table.select_Async(this._db, args);

        return {
            success: result.error === null,
            rows: result.rows,
            error: result.error,
        };
    }

    async _action_Update_Async(args) {
        if (args.rows.length === 0) {
            return {
                success: true,
                error: null,
            };
        }

        let pk = this._table.primaryKey;

        let columnNames = [];
        let row_0 = args.rows[0];
        for (let columnName in row_0)
            columnNames.push(columnName);

        if (!(columnNames.includes(pk)))
            throw new Error(`No Primary Key '${pk}' column.`);

        let rows = [];
        for (let i = 0; i < args.rows.length; i++) {
            let row = args.rows[i];
            if (Object.keys(row).length !== columnNames.length)
                throw new Error(`Columns inconsistency with first row in row '${i}'.`);

            for (let i = 0; i < columnNames.length; i++) {
                if (!(columnNames[i] in row)) {
                    throw new Error(`Column inconsistency with first row.` +
                            ` No column '${columnNames[i]}' in row '${i}'.`)
                };
            }

            if (this._table._columns.has('_Modified_DateTime'))
                row._Modified_DateTime = null;

            rows.push(row);
        }

        let update_Result = await this._table.update_Async(this._db, rows);

        let result = {
            success: update_Result.success,
            error: abData.debug ?
                    update_Result.error :
                    null,
        };

        return result;
    }
}