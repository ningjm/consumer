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


/**
 * ①同一个队列，同一个key,不同的消费者,谁到谁消费，案例：发红包
 * ②交换机有三种模式：订阅(广播)（fanout）、路由(direct)、通配符(topic)
 * 订阅：交换机接到生产者消息，广播给每个队列。不用指定key，但是要写key=""
 * 路由：指定key
 * 通配符：匹配key（正确实例key.*）(错误实例：key*)
 * ④同一个队列，多个消费者：就算抢到消息的那个消费者没有确认消费。别的消费者也不会得到该消息了。因为消息从一进来就已经分配给哪个消费者
 * ⑤在topic/direct模式下，如果一个队列绑定了两个不同的key，那么只要匹配到了其中一个key。都可以进入该队列
 * ⑥不同的队列可以创建相同的key
 * ⑦同一个交换机，同一个队列，不同的消费者能不能都接收到同一条消息。答案：不能
 */


@Component
public class ShopConsumer {


    /**
     * topic模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_topic",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop",durable = "true",type = "topic"),
                    key = "key.*"
            )
    )
    @RabbitHandler
    public void onShopListen(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){

        try {
            //业务逻辑---如操作数据库失败
            int a = 1/1;
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
            System.out.println(shop.getCategory());
            System.out.println("我是消费者1业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * topic模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_topic2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop",durable = "true",type = "topic"),
                    key = "key.del"
            )
    )
    @RabbitHandler
    public void onShopListen2(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者2业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * topic模式
     * 同一个交换机，同一个队列，不同的消费者(用来测试同一个交换机，同一个队列，不同的消费者能不能都接收到同一条消息。答案：不能)
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_topic2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop",durable = "true",type = "topic"),
                    key = "key.sel"
            )
    )
    @RabbitHandler
    public void onShopListen3(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者3业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }






    /**
     * fanout模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_fanout",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop2",durable = "true",type = "fanout"),
                    key = ""
            )
    )
    @RabbitHandler
    public void onShopListen4(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者4业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * fanout模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_fanout2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop2",durable = "true",type = "fanout"),
                    key = ""
            )
    )
    @RabbitHandler
    public void onShopListen5(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        System.out.println("我是消费者4业务成功，消息确认消费");
//        try {
//            //业务成功，消息确认消费
//            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
//            System.out.println(shop.getCategory());
//            System.out.println("我是消费者5业务成功，消息确认消费");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }


    /**
     * fanout模式
     * 同一个交换机，同一个队列，不同的消费者
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_fanout2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop2",durable = "true",type = "fanout"),
                    key = ""
            )
    )
    @RabbitHandler
    public void onShopListen6(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者6业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * direct模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_direct",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop3",durable = "true",type = "direct"),
                    key = "key"
            )
    )
    @RabbitHandler
    public void onShopListen7(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者7业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * direct模式
     * 同一个交换机，不同队列
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_direct2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop3",durable = "true",type = "direct"),
                    key = "key2"
            )
    )
    @RabbitHandler
    public void onShopListen8(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者8业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * direct模式
     * 同一个交换机，同一个队列，不同的消费者(用来测试同一个交换机，同一个队列，不同的消费者能不能都接收到同一条消息。答案：不能)
     * @param shop
     * @param headers
     * @param channel
     */
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queues_shop_direct2",durable = "true"),
                    exchange = @Exchange(name = "exchang_shop3",durable = "true",type = "direct"),
                    key = "key2"
            )
    )
    @RabbitHandler
    public void onShopListen9(@Payload Shop shop, @Headers Map<String,Object> headers, Channel channel){
        try {
            //业务成功，消息确认消费
            channel.basicAck((Long)headers.get(AmqpHeaders.DELIVERY_TAG), false);//确认消息-消费消息
            System.out.println(shop.getCategory());
            System.out.println("我是消费者9业务成功，消息确认消费");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
