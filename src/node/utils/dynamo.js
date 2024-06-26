import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, PutCommand, UpdateCommand} from "@aws-sdk/lib-dynamodb";


const marshallOptions = {
    // Whether to automatically convert empty strings, blobs, and sets to `null`.
    convertEmptyValues: false, // false, by default.
    // Whether to remove undefined values while marshalling.
    removeUndefinedValues: true, // false, by default.
    removeNullValues: true, // false, by default.
    // Whether to convert typeof object to map attribute.
    convertClassInstanceToMap: false, // false, by default.
};

const unmarshallOptions = {
    // Whether to return numbers as a string instead of converting them to native JavaScript numbers.
    wrapNumbers: false, // false, by default.
}

const translateConfig = {marshallOptions, unmarshallOptions};

// Create the DynamoDB Document client.
const client = new DynamoDBClient(translateConfig);

const ddbClient = DynamoDBDocumentClient.from(client, { marshallOptions });

/**
 *
 * @param TableName
 * @param Item = { id }
 * @returns {Promise<(Omit<PutItemCommandOutput, "Attributes" | "ItemCollectionMetrics"> & {Attributes?: Record<string, NativeAttributeValue>, ItemCollectionMetrics?: Omit<ItemCollectionMetrics, "ItemCollectionKey"> & {ItemCollectionKey?: Record<string, NativeAttributeValue>}}) | PutItemCommandOutput> | void}
 */
export const dynamoPutItem = (
    {
        TableName,
        Item
    }
) => ddbClient.send(new PutCommand({
    TableName,
    Item
}));

/**
 *
 * @param TableName
 * @param Key = {
 *       id: "1",
 *     }
 * @param UpdateExpression = "set VALUE = :color"
 * @param ExpressionAttributeValues =  {
 *                                          ":color": "black",
 *                                      }
 * @param ReturnValues = "ALL_NEW"
 * @returns {Promise<(Omit<UpdateItemCommandOutput, "Attributes" | "ItemCollectionMetrics"> & {Attributes?: Record<string, NativeAttributeValue>, ItemCollectionMetrics?: Omit<ItemCollectionMetrics, "ItemCollectionKey"> & {ItemCollectionKey?: Record<string, NativeAttributeValue>}}) | UpdateItemCommandOutput> | void}
 */
export const dynamoUpdateItem = ({
                                     TableName,
                                     Key,
                                     UpdateExpression,
                                     ExpressionAttributeValues,
                                     ReturnValues = "ALL_NEW",
                                 }) => {
    const comand = new UpdateCommand(
        {
            TableName,
            Key,
            UpdateExpression,
            ExpressionAttributeValues,
            ReturnValues,
        }
    )
    return ddbClient.send(comand);
};

