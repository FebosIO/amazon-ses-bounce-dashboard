// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict'
// Create service client module using ES6 syntax.
import {dynamoPutItem, sqsSendMessage} from "./utils/index.js";


export const handler = async (event, context) => {
    const timestamp = new Date().toISOString();
    const requestId = context.awsRequestId
    try {
        const {
            id = requestId,
            documentoId = null,
            messageId = null,
            pais,
            ambiente,
            dominio,
            manifiesto,
            empresa = '0',
            aplicacion = 'FEB',
            servicio = '',
            proceso = '',
            destinatarios = [],
            copias = [],
            ConfigurationSetName = "default"
        } = event;

        const saveData = {
            id,
            estado:'queqed',
            documentoId,
            messageId,
            pais,
            stage: ambiente,
            domain: dominio,
            manifiesto,
            empresa,
            destinatarios,
            copias,
            servicio,
            proceso,
            application: aplicacion,
            timestamp: timestamp,
            ConfigurationSetName,
        }

        const params = {
            MessageDeduplicationId: id,
            MessageGroupId: requestId,
            MessageBody: JSON.stringify(saveData),
            QueueUrl: process.env.SQS_URL
        };

        let response = {};
        await dynamoPutItem({TableName: process.env.TABLE_EMAIL_NAME, Item: saveData})
        const data = await sqsSendMessage(params);
        if (data) {
            response = {
                ...response,
                statusCode: 200,
                body: saveData
            };
        } else {
            response = {
                ...response,
                statusCode: 500,
                body: {
                    message: 'Some error occured !!'
                }
            };
        }
        return response
    } catch (err) {
        err.requestId = requestId;
        console.warn(event)
        console.log("Error in writing data to the DynamoDB table : ", err.message)
        throw err
    }
}





