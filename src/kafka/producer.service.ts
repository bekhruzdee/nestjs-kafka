import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
} from '@nestjs/common';
import { Kafka, Producer, ProducerRecord, Partitioners } from 'kafkajs';

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'nestjs-producer',
    brokers: ['localhost:9092'],
    retry: { retries: 5 },
  });

  private readonly producer: Producer = this.kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  async onModuleInit() {
    await this.producer.connect();
    console.log('Kafka Producer connected');
  }

  async produce(record: ProducerRecord) {
    try {
      await this.producer.send(record);
      console.log(`Message sent to topic: ${record.topic}`);
    } catch (error) {
      console.error('Producer error:', error);
      throw error;
    }
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
    console.log('Kafka Producer disconnected');
  }
}
