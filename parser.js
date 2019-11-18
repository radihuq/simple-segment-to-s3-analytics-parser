module.exports.parseAnalytics = function () {
    console.log('Running simple-segment-to-s3-analytics-parser');
    require('dotenv/config');
    const AWS = require('aws-sdk');
    const zlib = require('zlib');
    
    const BUCKET = process.env.S3_BUCKET; //Segment bucket
    const REGION = process.env.S3_REGION; //ex: 'us-east-2'
    const ACCESS = process.env.S3_ACCESS;
    const SECRET_KEY = process.env.S3_SECRET_KEY;

    const S3 = new AWS.S3({
        apiVersion: '2006-03-01',
        region: REGION,
        credentials: {
            accessKeyId: ACCESS,
            secretAccessKey: SECRET_KEY
        }
    });

    const S3Params = {
        Bucket: BUCKET
    }

    //CALL PROCESS HERE
    getLogs(S3Params);

    async function getLogs (S3Params) {
        const objects = await getS3Objects(S3Params);
        const parse = await syncS3Objects(objects);
        return(parse);
    }

    function getS3Objects (S3Params) {
        return new Promise((resolve) => {
            S3.listObjects(S3Params, (err, data) => {
                if (err) {
                    console.log(err);
                    resolve(err);
                } else {
                    console.log(`Retrieved list of logs. Total length: ${data.Contents.length}`);
                    resolve(data.Contents);
                }
            })
        });
    }

    async function syncS3Objects (logs) {
        let totalLength = logs.length;

        let logsArray = [];

        console.log(`Gathering logs...`);
        const startingIndex = (logs.length - 20); //Logs collection can be expansive. Test with 20 first, then change this to 0 when ready to run at full-scale
        for (let i=startingIndex, len = totalLength; i < len; i++) {
            if (logs[i].Key.split('.').slice(-1).pop() == 'gz') { //Making sure file is a compressed Segment log (.gz)
                const S3Obj = await getObject(logs[i].Key);
                const S3ObjUnzipped = await gunzipObject(S3Obj);
                logsArray.push(S3ObjUnzipped);
                console.log(`Completed getting data from log ${i}/${totalLength}`);
            }
        }
        let goodLogsLength = logsArray.length;
        console.log(`Completed gathering logs. Total logs: ${goodLogsLength}. [Skipped ${(totalLength-goodLogsLength)} bad logs]`);

        let parsedArray = [];
        let currentActionIndex = 0;
        for (let i=0, len = logsArray.length; i < len; i++) {
            for (let j=0, jlen = logsArray[i].length; j < jlen; j++) {
                if (logsArray[i][j] !== '') { //Making sure log is not empty
                    let parsedLog = JSON.parse(logsArray[i][j]);
                    parsedArray.push(parsedLog);
                    ++currentActionIndex;
                    console.log(`Current total actions logged: ${currentActionIndex}`);

                    // [SPECIFICY WHICH PARAMETERS YOU WANT TO INCLUDE]
                    // let obj = {
                    //     anonymousId: parsedItem.anonymousId,
                    //     channel: parsedItem.channel,
                    //     contextPage: parsedItem.context.page,
                    //     contextUserAgent: parsedItem.context.userAgent,
                    //     originalTimestamp: parsedItem.originalTimestamp,
                    //     projectId: parsedItem.projectId,
                    //     properties: parsedItem.properties,
                    //     type: parsedItem.type,
                    //     event: parsedItem.event,
                    //     userId: parsedItem.userId,
                    // }
                    // parsedArray.push(obj);
                    // ++currentActionIndex;
                    // console.log(`Current total actions logged: ${currentActionIndex}`);
                }
            }
        }

        console.log(`Total number of actions logged by Segment: ${parsedArray.length}`);
        let key = 0;
        console.log(`Example returned object (key: ${key}):`);
        console.log(parsedArray[key]);
        return parsedArray;
    }

    async function getObject (Key) {
        const params = {
            Bucket: BUCKET,
            Key: Key
        }

        try {
            const file = await S3.getObject(params).promise();
            return file.Body;
        } catch (err) {
            throw new Error(`Could not retrieve file from S3: ${err.message}`);
        }
    }

    async function gunzipObject (buffer) {
        try {
            return new Promise((resolve) => {
                let unzipbuffer = Buffer.from(buffer, 'base64');
                zlib.unzip(unzipbuffer, (err, buffer) => {
                    if (!err) {
                        let analytics = buffer.toString().replace(/^"/g, '').replace(/"$/g, '').split(/\n/g); //Cleaning up data
                        let analyticsArray = [];
                        for (let i=0, len = analytics.length; i < len; i++) { //For loop in case there are more than one actions tracked in this log
                            if (analytics[i] !== '') {
                                analyticsArray.push(analytics[i]);
                            }
                        }
                        resolve(analyticsArray);
                    } else {
                        resolve(err);
                    }
                })
            })
        } catch (err) {
            throw new Error(`Could not Gunzip file: ${err.message}`);
        }
    }


}
