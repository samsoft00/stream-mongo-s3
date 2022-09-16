import AWS from 'aws-sdk';
import dayjs from 'dayjs';
import { promisify } from 'util'
import { stringify } from 'csv/.';
import { Db, MongoClient } from 'mongodb'
import { PassThrough, Transform, pipeline } from 'stream'

// types
type AnyObject = { [key: string]: any }
type StreamUpload = { 
    writeStream: PassThrough, 
    asyncUpload: Promise<AWS.S3.ManagedUpload.SendData> 
}

let db: Db;
const asyncPipeline = promisify(pipeline)

// config
let config: AnyObject = {
    region: 'us-east-1',
    aws_access_key_id: process.env.AWS_ACCESS_KEY,
    bucket: process.env.BUCKET,
    key: `${dayjs().format('YYYY-MM-DD_HH:00')}.csv`
}

// async upload func
const s3StreamUploder = (params: AWS.S3.PutObjectRequest): StreamUpload => {
    const s3 = new AWS.S3()
    const pass = new PassThrough()

    return {
        writeStream: pass,
        asyncUpload: s3.upload({ Bucket: params.Bucket, Key: params.Key, Body: pass }).promise()
    }
}

const handler = async (): Promise<any> => {

    if (!db) {
        // setup mongo connection
        const client = new MongoClient(process.env.DB_URL, {
          useNewUrlParser: true,
          useUnifiedTopology: true
        })
     
        await client.connect()
        db = client.db(process.env.DB_NAME)
     }
     
     // set up aws config
     AWS.config.update({
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY,
        region: '',
        signatureVersion: 'v4'
     })

     // queries
     const query = {}
     const cursor = db.collection('logs').find(query)

    // Dont store blank files
    const count = await cursor.count()
    if (!count) { return }

    const csvStringify = stringify({
        header: true,
        columns: [
          { key: 'date', header: 'Date' },
          { key: 'recipient', header: 'Customer' },
          { key: 'event', header: 'Event' },
          { key: 'subject', header: 'Subject' },
          { key: 'meta', header: 'Additional data' }
        ]
    })
    
    const { writeStream, asyncUpload } = s3StreamUploder({ Bucket: config.bucket, Key: config.key })
    await asyncPipeline(
        cursor,
        // Transform stream
        new Transform({
            writableObjectMode: true,
            readableObjectMode: true,
            transform (chunk, enc, done) {

                // transform chunk
                this.push({})
                done()
            }
        }),
        csvStringify,
        writeStream
    )

    await asyncUpload
}

handler()
    .then(() => console.log('done'))
    .catch(console.error)