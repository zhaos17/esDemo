package com.es.esapi.modal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 测试用户信息
 */
@Component
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String name;
    private long age;
}
