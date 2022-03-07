package com.limitofsoul.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReceiveEvent {
    private String txId;
    private String payChannel;
    private Long timestamp;
}
