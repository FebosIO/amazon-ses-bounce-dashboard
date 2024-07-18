// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict'

import {dynamoPutItem, milisegundosToEpoch, sendEventToBus, sunMinutesToDateFromISO} from "./utils/index.js";
import {v4 as uuidv4} from 'uuid';

const TABLE_EMAIL_SUPPRESSION_NAME = process.env.TABLE_EMAIL_SUPPRESSION_NAME || 'ses-email-suppression';
const TABLE_EVENT_NAME = process.env.TABLE_EVENT_NAME || 'ses-event'
const TTL = parseInt(process.env.TTL || '525600')


export const handler = async (event, context) => {
    try {
        for (let record of event.Records) {
            await procesarRecord(record);
        }
    } catch (err) {
        console.warn(event)
        console.log("Error in writing data to the DynamoDB table : ", err.message)
        console.error(err)
        throw err
    }
}

async function procesarRecord(record) {
    const sqsBody = JSON.parse(record.body)
    const message = sqsBody.Message ? JSON.parse(sqsBody.Message || "null") || sqsBody : sqsBody
    // All event definition. For more information, see
    // https://docs.aws.amazon.com/ses/latest/dg/event-publishing-retrieving-sns-examples.html
    const type = message.eventType
    const mail = message.mail
    const messageId = message.mail.messageId
    const event_detail = message[type.toLowerCase()] || {}
    const timestamp = event_detail.timestamp || new Date().toISOString();
    const expiration = milisegundosToEpoch(sunMinutesToDateFromISO(timestamp, TTL))
    const data = {
        id: uuidv4(),
        messageId: messageId,
        estado: 'queued',
        timestamp: timestamp,
        type: type,
        event: event_detail,
        expiration,
        mail
    };
    const response = await dynamoPutItem({
        TableName: TABLE_EVENT_NAME, Item: data
    })
    await procesarEventosSuppression({
        type,
        event_detail,
        mail,
        timestamp,
        messageId,
        companyId: obtenerCompanyFromEmailTags(mail),
        stage: obtenerStageFromEmailTags(mail)
    })
}

function obtenerStageFromEmailTags(mail) {
    try {
        if (mail.tags.stage) {
            return mail.tags.stage.join(",")
        }
    } catch (e) {
        console.error(e);
    }
    return 'produccion'
}

function obtenerCompanyFromEmailTags(mail) {
    try {
        if (mail.tags.empresa) {
            return mail.tags.empresa.join(",")
        }
    } catch (e) {
        console.error(e);
    }
    return '0'
}

async function procesarEventosSuppression({
                                              type,
                                              event_detail,
                                              mail,
                                              timestamp,
                                              messageId,
                                              companyId = '0',
                                              stage = 'produccion'
                                          }) {
    if (type === 'Bounce') {
        const recipents = event_detail.bouncedRecipients
        for (let recipent of recipents) {
            const {
                emailAddress, diagnosticCode, action
            } = recipent;
            const event = {
                id: emailAddress,
                emailAddress,
                timestamp,
                type,
                stage,
                message: diagnosticCode || action || '',
                messageId,
                companyId
            };
            await dynamoPutItem({
                TableName: TABLE_EMAIL_SUPPRESSION_NAME, Item: event
            })
            sendEventToBus({
                type: 'email-suppression',
                event
            })
        }
    } else if (type === 'Reject') {
        const {
            reason
        } = event_detail;
        for (let destinationElement of mail.destination) {
            const event = {
                id: destinationElement,
                emailAddress: destinationElement,
                timestamp,
                type,
                stage,
                message: reason || '',
                messageId,
                companyId
            }
            await dynamoPutItem({
                TableName: TABLE_EMAIL_SUPPRESSION_NAME, Item: event
            })
            sendEventToBus({
                type: 'email-suppression',
                event
            })
        }
    } else if (type === 'Complaint') {
        const recipents = event_detail.complainedRecipients
        for (let recipent of recipents) {
            const {
                emailAddress
            } = recipent;
            const diagnosticCode = event_detail.complaintFeedbackType
            const event = {
                id: emailAddress,
                emailAddress: emailAddress,
                timestamp,
                type,
                stage,
                message: diagnosticCode || '',
                messageId,
                companyId
            }
            await dynamoPutItem({
                TableName: TABLE_EMAIL_SUPPRESSION_NAME, Item: event
            })
            sendEventToBus({
                type: 'email-suppression',
                event: {
                    emailAddress,
                    companyId
                }
            })
        }
    }
}