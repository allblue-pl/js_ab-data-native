'use strict';

const
    abData = require('ab-data'),
    js0 = require('js0')
;

export default class NativeDevice extends abData.Device {
    constructor(deviceId, deviceHash, lastUpdate, lastItemId, declaredItemIds = []) {
        super(deviceId, deviceHash, lastUpdate, lastItemId, declaredItemIds);

        this._itemIds_Used = [];
    }

    isNewId(id) {
        js0.args(arguments, 'number');

        if (super.isNewId(id)) {
            let idInfo = abData.Device.GetIdInfo(id);
            return !this._itemIds_Used.includes(idInfo['itemId']);
        }

        return false;
    }

    useId(id) {
        js0.args(arguments, js0.Long);

        let idInfo = abData.Device.GetIdInfo(id);

        if (!this._isNewId_Device(idInfo))
            throw new Error(`'_Id' '${id}' is not a new id.`);

        this._itemIds_Used.push(idInfo['itemId']);
    }
}