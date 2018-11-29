package com.juzhen.consumer;

import com.juzhen.producer.pojo.Shop;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

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
        System.out.println(shop.getCategory());
    }
}
