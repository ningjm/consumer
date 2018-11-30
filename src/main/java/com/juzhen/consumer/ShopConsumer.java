package com.juzhen.consumer;

import com.juzhen.producer.pojo.Shop;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * @program: consumer
 * @description: 消息队列-消费者
 * @author: Mr.Ning
 * @create: 2018-11-29 16:30
 **/

@Component
public class ShopConsumer {

    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop",durable = "true",type = "topic"),
                    key = "shop_key"
            )
    )
    @RabbitHandler
    public void onShopListen(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){

        try {
            //业务逻辑---如操作数据库失败
            int a = 1/0;
        }catch (Exception ex){
            try {
                //否认消息--消息重新投递
                channel.basicNack((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false,true);
                System.out.println("否认消息--消息重新消费");
                return;//退出方法
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println("业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
