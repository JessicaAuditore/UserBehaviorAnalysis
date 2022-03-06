package com.limitofsoul.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdClinkEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
