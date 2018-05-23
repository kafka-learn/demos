package com.mb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * @Author mubi
 * @Date 2018/5/16 下午2:13
 */
public class TestMain {


    Logger logger = LogManager.getLogger(TestMain.class);

    @Test
    public void test_listTopics(){
        logger.error("==test log==");
        Main.list_topic();
    }

    @Test
    public void test_createTopic(){
        Main.create_topic();
    }

    @Test
    public void test_describeTopic(){
        Main.describe_topic();
    }
}
