package org.wcg.gmall.reltime.dwd.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wangchengguang
 * @date 2025/3/15 20:21:51
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    private Integer deptId;
    private String deptName;
    private Long ts;
}
