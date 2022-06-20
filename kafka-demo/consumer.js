import {Kafka} from 'kafkajs';

const kafka = new Kafka({
  clientId: 'employee-service',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'project',
  //this is to disable retry on faulure during process. so it start with last commited offset instead of first offset of the batch
  retry: {
    initialRetryTime: 100,
    retries: 0,
  },
});
await consumer.subscribe({topic: 'employee-topic', fromBeginning: true});

await consumer.run({
  autoCommit: false,
  eachMessage: async ({topic, partition, message}) => {
    console.log({
      partition,
      offset: message.offset,
      value: message.value.toString(),
    });
    await new Promise((resolve) => setTimeout(resolve, 1000));

    //  if (message.offset == '310') throw new Error('something happened');
    //+1 is used to make sure previous message (which committed last) not come back to re process
    await consumer.commitOffsets([
      {
        topic,
        partition,
        offset: (Number(message.offset) + 1).toString(),
      },
    ]);
    console.log('offset set to ', Number(message.offset) + 1);
  },
});
