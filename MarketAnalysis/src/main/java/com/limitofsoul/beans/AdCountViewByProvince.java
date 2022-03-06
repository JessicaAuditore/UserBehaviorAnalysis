package com.limitofsoul.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
