package com.oy.oy_canal.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * canal消费注解
 *
 * @author oy
 * @createDate 2026/4/24 10:06
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface ZsCanalTableListener {

    /** 数据库 */
    String database() default "";

    /** 表 */
    String[] table();

    /** 处理多批次数据 */
    boolean handleBatch() default false;

}
