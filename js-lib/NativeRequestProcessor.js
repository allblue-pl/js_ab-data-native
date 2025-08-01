'use strict';

const
    abData = require('ab-data'),
    abLock = require('ab-lock'),
    js0 = require('js0'),
    webABApi = require('web-ab-api'),

    Database = require('ab-database-native').Database,

    NativeDataStore = require('./NativeDataStore')
;

export default class NativeRequestProcessor extends abData.RequestProcessor {
    get db() {
        return this._db;
    }


    constructor(dataScheme, device, db) {
        js0.args(arguments, abData.DataScheme, require('./NativeDevice'), 
                Database);

        super(dataScheme, device);

        this._scheme = dataScheme;
        this._device = device;
        this._db = db;

        this._requests = {};
    }

    getRequest(requestName) {
        js0.args(arguments, 'string');

        if (!this.hasRequest(requestName))
            throw new Error(`Request '${requestName}' does not exist.`);

        return this._requests[requestName];
    }

    hasRequest(requestName) {
        js0.args(arguments, 'string');

        return requestName in this._requests;
    }

    async __processRequestBatch_Async(requests, transactionId = null) {
        js0.args(arguments, Array, [ 'int', js0.Null, js0.Default ]);

        let response = new abData.Response();

        let success = true;
        let localTransaction = false;

        try {
            if (transactionId === null) {
                transactionId = await this._db.transaction_Start_Async();
                localTransaction = true;
            }

            let requests_W = [];
            for (let request of requests) {
                let requestId = request[0];
                let requestName = request[1];
                let actionName = request[2];
                let actionArgs = request[3];

                response.results[requestId] = null;
                response.requestIds.push(requestId);
                response.actionErrors[requestId] = null;

                let result = null;
                try {
                    result = await this.getRequest(requestName)
                            .executeAction_Async(this._device, actionName, 
                            actionArgs, transactionId);

                    this._scheme.validateResult(request, result);
                } catch (e) {
                    if (abData.debug)
                        console.error(e);

                    success = false;

                    response.type = abData.Response.Types_ActionError;
                    if (abData.debug) {
                        response.errorMessage = `Action Error: ` +
                                `'${requestName}:${actionName}' -> ${e.message}`;
                    } else
                        response.errorMessage = e.message;
                    response.actionErrors[requestId] = e.message;

                    break;
                }

                if (!('_type' in result)) {
                    success = false;

                    response.type = Response.Types_ActionError;
                    response.errorMessage = `No '_type' in action result: ` +
                            `'${requestName}:${actionName}'`;
                    response.actionErrors[requestId] = 
                            `No '_type' in action result.`;

                    break;
                }

                response.results[requestId] = result;

                if (result._type >= 2) {
                    success = false;

                    response.type = Response.Types_ResultError;
                    response.errorMessage = `Result Error: ` +
                            `'${requestName}:${actionName}'.`;

                    break;
                }

                if (result._type === 1) {
                    success = false;

                    response.type = Response.Types_ResultFailure;
                    response.errorMessage = `Result Failure: ` +
                            `'${requestName}:${actionName}'.`;

                    break;
                }

                if (this._scheme.getRequestDef(requestName)
                        .getActionDef(actionName).type === 'w') {
                    requests_W.push([ requestName, actionName, actionArgs ]);
                }
            }

            if (success && requests_W.length > 0)
                success = await this._db.addDBRequests_Async(requests_W);

            if (success)
                await this._updateDeviceInfo_Async(transactionId);
        } catch (e) {
            if (abData.debug)
                console.error(e);

            response.type = Response.Types_Error;
            response.errorMessage = e.message;
        }

        try {
            if (localTransaction)
                await this._db.transaction_Finish_Async(success, transactionId);
        } catch (e) {
            if (success) {
                response.success = false;

                response.type = Response.Types_Error;
                response.errorMessage = 'Cannot commit request processor transaction: ' +
                        e.message;

                return response;
            }
        }

        response.success = success;

        return response;
    }

    setR(requestName, request) {
        return this.setRequest(requestName, request);
    }

    setRequest(requestName, request) {
        js0.args(arguments, 'string', require('./Request'));

        if (requestName in this._requests)
            throw new Error(`Request '${requestName}' already exists.`);

        this._requests[requestName] = request;

        return this;
    }

    async _updateDeviceInfo_Async(transactionId = null) {
        js0.args(arguments, [ 'int', js0.Null, js0.Default ]);

        let localTransaction = false;
        if (transactionId === null) {
            transactionId = await this._db.transaction_Start_Async();
            localTransaction = true;
        }

        let lastDeclaredItemId = this.device.lastItemId;
        // for (let itemId_Declared of this.device.declaredItemIds) {
        //     if (itemId_Declared > lastDeclaredItemId)
        //         lastDeclaredItemId = itemId_Declared;
        // }

        let deviceInfo = await NativeDataStore.GetDeviceInfo_Async(this._db, 
                transactionId);
        let declaredItemIds = deviceInfo.declaredItemIds;
        for (let itemId of this.device.declaredItemIds) {
            if (!declaredItemIds.includes(itemId))
                declaredItemIds.push(itemId);
        }

        await NativeDataStore.SetDeviceInfo_Async(this._db, 
                this.device.id, this.device.hash, lastDeclaredItemId, 
                this.device.lastUpdate, declaredItemIds, transactionId);

        if (localTransaction)
            await this._db.transaction_Finish_Async(true, transactionId);
    }
}