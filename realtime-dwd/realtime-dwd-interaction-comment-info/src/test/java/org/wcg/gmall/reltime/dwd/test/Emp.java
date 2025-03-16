package org.wcg.gmall.reltime.dwd.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wangchengguang
 * @date 2025/3/15 20:20:24
 * @description
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
    private Integer empId;
    private String empName;
    private Integer deptId;
    private Long ts;
}
