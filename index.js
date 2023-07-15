const { PubSub } = require('@google-cloud/pubsub');
const Web3 = require('web3');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');

const projectId = process.env.PROJECT_ID;
const subscriptionName = 'latest-blocknumber-topic-sub';
const transactionsTopicName = 'transactions-topic';
const { v1 } = require('@google-cloud/pubsub');

const client = new v1.SubscriberClient();


const pubsub = new PubSub({ projectId });

// Function to retrieve the API key from Secret Manager
async function getApiKey() {
  const secretName = `projects/${process.env.PROJECT_NUMBER}/secrets/web3-api-key/versions/latest`;
  const client = new SecretManagerServiceClient();
  const [version] = await client.accessSecretVersion({ name: secretName });
  return version.payload.data.toString();
}

async function publishTransaction(transaction) {
  const data = Buffer.from(JSON.stringify(transaction));
  await pubsub.topic(transactionsTopicName).publish(data);
}

async function handleError(blockNumber) {
  const data = Buffer.from(JSON.stringify({ blockNumber }));
  await pubsub.topic(subscriptionName).publish(data);
}

async function retrieveBlockNumbers() {
  const apiKey = await getApiKey();
  const web3 = new Web3(`https://mainnet.infura.io/v3/${apiKey}`);

  const handleMessage = async (message) => {
    let blockNumber; // Declare blockNumber variable

    const request = {
      subscription: client.subscriptionPath(process.env.PROJECT_ID, subscriptionName),
      maxMessages: 1,
    };

    const [response] = await client.pull(request);
    const messages = response.receivedMessages;
    console.log(messages);
    console.log("after msges");
    console.log([response]);
  
   /* try {
      const request = {
        subscription: client.subscriptionPath(process.env.PROJECT_ID, subscriptionName),
        maxMessages: 1,
      };
  
      const [response] = await client.pull(request);
      const messages = response.receivedMessages;
      console.log(messages);


      const ackRequest = {
        subscription: request.subscription,
        ackIds: [messages[0].ackId],
      };

      await client.acknowledge(ackRequest);
  
      blockNumber = messages;
      
        const block = await web3.eth.getBlock(blockNumber);
        const transactions = block.transactions;
  
        for (const transaction of transactions) {
          console.log(transaction);
          await publishTransaction(transaction);
        }
      
  
      message.ack();
    } catch (error) {
      console.error('Error processing message:', error);
      if (blockNumber) {
        await handleError(blockNumber); // Put the block number back in the Pub/Sub topic for reprocessing
      }
      message.ack();
    } */
  };
  
  

  const subscription = pubsub.subscription(subscriptionName);
  subscription.on('message', handleMessage);
}

retrieveBlockNumbers();