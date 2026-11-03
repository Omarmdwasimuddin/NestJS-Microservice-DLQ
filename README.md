## NestJS Microservices with Kafka (Implement Dead Letter Queue (DLQ))

#### Prerequisite
আগে এই project complete করে নাও:
[NestJS Microservices with Kafka (project)](https://github.com/Omarmdwasimuddin/NestJS-Microservice-Kafka)


#### Start Kafka Broker
> VS Code Terminal-এ প্রথমে command দাও।
```bash
wsl -d Ubuntu
```
```bash
docker start kafka
```
---


#### Implement Dead Letter Queue (DLQ): `notification-service/src/app/app.controller.ts`
```ts
import { Controller, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern('user_created')
  async handleUserCreated(@Payload() data: any) {
    try {
      console.log('Main Event Received:', data);
      throw new Error('Payment service is Failed!')
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log('Sending Event to DLQ');
      this.kafkaClient.emit('user_created_dlq', {
        failedData: data,
        error: errorMessage,
        failedAt: new Date()
      })
    }
  }
  @EventPattern('user_created_dlq')
  handleDLQ(@Payload() data: any) {
    console.log('DLQ Event Received:', data);
  }
}
```
---


#### `notification-service/src/app/app.module.ts`
```ts
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'notification-client',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'notification-consumer-group',
          }
        }
      }
    ])
  ],
  controllers: [AppController],
})
export class AppModule {}
```
---

####  Run API Gateway
```bash
npx nx serve api-gateway
```
#### Run Notification Service
```bash
npx nx serve notification-service
```
---

#### Test DLQ Implementation
> Endpoint
```bash
http://localhost:3000/create-user
```
---
> <img width="393" height="212" alt="image" src="https://github.com/user-attachments/assets/db10c1ce-ca37-4fc7-8cca-a877a3cb7e93" />
